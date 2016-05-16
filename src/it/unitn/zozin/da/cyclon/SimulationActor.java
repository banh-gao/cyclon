package it.unitn.zozin.da.cyclon;

import it.unitn.zozin.da.cyclon.DataProcessor.GraphProperty;
import it.unitn.zozin.da.cyclon.DataProcessor.RoundData;
import it.unitn.zozin.da.cyclon.GraphActor.AddNodesEndedMessage;
import it.unitn.zozin.da.cyclon.GraphActor.BootNodesEndedMessage;
import it.unitn.zozin.da.cyclon.NodeActor.EndRoundMessage;
import it.unitn.zozin.da.cyclon.NodeActor.StartRoundMessage;
import it.unitn.zozin.da.cyclon.SimulationActor.SimulationStateData;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.function.BiFunction;
import akka.actor.AbstractFSM;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;

class SimulationActor extends AbstractFSM<SimulationActor.State, SimulationStateData> {

	enum State {
		Idle, NodesAdding, NodesBoot, RoundRunning, MeasureRunning
	}

	private static final int BOOT_ROUND = 0;

	{
		startWith(State.Idle, null);

		// Configure simulation
		when(State.Idle, matchEvent(Configuration.class, (confMsg, data) -> initSimulation(confMsg)));

		// Prepare simulation
		when(State.NodesAdding, matchEvent(AddNodesEndedMessage.class, (endAddMsg, data) -> executeNodesBoot(endAddMsg.addedNodes)));
		when(State.NodesBoot, matchEvent(BootNodesEndedMessage.class, (endInitMsg, data) -> startSimulation()));

		// Run simulation
		when(State.RoundRunning, matchEvent(EndRoundMessage.class, SimulationStateData.class, (endRoundMsg, simState) -> processCyclonRoundEnded(endRoundMsg, simState)));
		when(State.MeasureRunning, matchEvent(RoundData.class, SimulationStateData.class, (measureMsg, simState) -> processMeasure(measureMsg, simState)));

	}

	static class SimulationStateData {

		private final int total;

		final List<RoundData> simData = new LinkedList<RoundData>();

		public SimulationStateData(int total) {
			this.total = total;
		}

		public boolean isLast() {
			return simData.size() == total - 1;
		}

		public boolean isCompleted() {
			return simData.size() == total;
		}

		public void increaseRound(RoundData data) {
			simData.add(data);
		}

		public int getRound() {
			return simData.size();
		}
	}

	private ActorRef GRAPH;

	private ActorRef simSender;
	private Configuration conf;

	public static ActorRef newActor(ActorSystem sys) {
		return sys.actorOf(Props.create(SimulationActor.class), "control");
	}

	@Override
	public void preStart() throws Exception {
		GRAPH = context().actorOf(Props.create(GraphActor.class), "graph");
	}

	private akka.actor.FSM.State<State, SimulationStateData> initSimulation(Configuration conf) {
		simSender = sender();
		this.conf = conf;

		GRAPH.tell(new GraphActor.AddNodesMessage(conf.NODES, conf.CYCLON_CACHE_SIZE, conf.CYCLON_SHUFFLE_LENGTH), self());
		return goTo(State.NodesAdding);
	}

	private akka.actor.FSM.State<State, SimulationStateData> executeNodesBoot(NavigableSet<Integer> addedNodes) {
		System.out.print("Executing [BOOT]... ");
		Map<Integer, Integer> introducers = new HashMap<Integer, Integer>();
		for (int n : addedNodes) {
			int introducer = conf.BOOT_TOPOLOGY.topologyFunc.apply(addedNodes, n);
			introducers.put(n, introducer);
		}

		GRAPH.tell(new GraphActor.BootNodesMessage(introducers), self());

		return goTo(State.NodesBoot);
	}

	private akka.actor.FSM.State<State, SimulationStateData> startSimulation() {
		System.out.println("[boot completed]");
		return executeMeasure(new SimulationStateData(conf.ROUNDS + 1));
	}

	private akka.actor.FSM.State<State, SimulationStateData> executeMeasure(SimulationStateData simState) {

		Set<GraphProperty> measureParams;

		if (simState.getRound() == BOOT_ROUND || simState.isLast())
			measureParams = conf.FINAL_MEASURE;
		else
			measureParams = conf.ROUND_MEASURE;

		// If there is nothing to measure skip measuring
		if (measureParams.isEmpty()) {
			return controlSimulationRoundEnd(simState, RoundData.EMPTY_DATA);
		}

		if (simState.getRound() == BOOT_ROUND)
			System.out.print("Measuring [BOOT]... ");
		else
			System.out.print("Measuring round " + simState.getRound() + "... ");

		GRAPH.tell(new GraphActor.StartMeasureMessage(measureParams), self());
		return goTo(State.MeasureRunning).using(simState);
	}

	private akka.actor.FSM.State<State, SimulationStateData> processMeasure(RoundData roundMeasureMsg, SimulationStateData simState) {
		System.out.println("[completed] -> " + roundMeasureMsg);
		return controlSimulationRoundEnd(simState, roundMeasureMsg);
	}

	private akka.actor.FSM.State<State, SimulationStateData> controlSimulationRoundEnd(SimulationStateData simState, RoundData data) {
		simState.increaseRound(data);

		if (simState.isCompleted()) {
			// Send report back to simulation starter
			simSender.tell(new SimulationDataMessage(conf, simState.simData), self());
			return goTo(State.Idle);
		} else {
			return executeProtocolRound(simState);
		}
	}

	private akka.actor.FSM.State<State, SimulationStateData> executeProtocolRound(SimulationStateData simState) {
		if (simState.getRound() == BOOT_ROUND)
			System.out.print("Executing [BOOT]... ");
		else
			System.out.print("Executing round " + (simState.getRound()) + "... ");

		GRAPH.tell(new StartRoundMessage(), self());
		return goTo(State.RoundRunning).using(simState);
	}

	private akka.actor.FSM.State<State, SimulationStateData> processCyclonRoundEnded(EndRoundMessage roundMsg, SimulationStateData simState) {
		System.out.println("[completed]");
		return executeMeasure(simState);
	}

	// Simulation param message
	public static class Configuration {

		// Used to generate a random graph
		private static final Random rand = new Random();

		enum Topology {
			CHAIN((nodes, i) -> {
				return (nodes.higher(i) != null) ? nodes.higher(i) : nodes.first();
			}),
			STAR((nodes, i) -> nodes.first()),
			RANDOM((nodes, i) -> nodes.ceiling(rand.nextInt(nodes.size())));

			private final BiFunction<NavigableSet<Integer>, Integer, Integer> topologyFunc;

			private Topology(BiFunction<NavigableSet<Integer>, Integer, Integer> topologyFunc) {
				this.topologyFunc = topologyFunc;
			}

			int getIntroducerNode(NavigableSet<Integer> addedNodes, int nodeIndex) {
				return topologyFunc.apply(addedNodes, nodeIndex);
			}
		};

		public Topology BOOT_TOPOLOGY;
		int NODES;
		int ROUNDS;
		int CYCLON_CACHE_SIZE;
		int CYCLON_SHUFFLE_LENGTH;
		Set<GraphProperty> ROUND_MEASURE;
		Set<GraphProperty> FINAL_MEASURE;

		public void load(FileInputStream inStream) throws IOException {
			Properties props = new Properties();
			props.load(inStream);
			NODES = Integer.parseInt(props.getProperty("nodes"));
			ROUNDS = Integer.parseInt(props.getProperty("rounds"));
			BOOT_TOPOLOGY = Topology.valueOf(props.getProperty("topology").toUpperCase());

			CYCLON_CACHE_SIZE = Integer.parseInt(props.getProperty("cyclonCache"));
			CYCLON_SHUFFLE_LENGTH = Integer.parseInt(props.getProperty("cyclonShuffle"));

			ROUND_MEASURE = new HashSet<DataProcessor.GraphProperty>();
			if (!props.getProperty("roundMeasure", "").isEmpty())
				for (String m : props.getProperty("roundMeasure", "").split(","))
					ROUND_MEASURE.add(GraphProperty.valueOf(m.trim().toUpperCase()));

			FINAL_MEASURE = new HashSet<DataProcessor.GraphProperty>();
			if (!props.getProperty("finalMeasure", "").isEmpty())
				for (String m : props.getProperty("finalMeasure", "").split(","))
					FINAL_MEASURE.add(GraphProperty.valueOf(m.trim().toUpperCase()));
		}
	}

	public static class SimulationDataMessage {

		final Configuration conf;
		final Map<Integer, RoundData> simData = new HashMap<Integer, RoundData>();

		public SimulationDataMessage(Configuration conf, List<RoundData> simData) {
			this.conf = conf;
			for (int round = 0; round < simData.size(); round++)
				this.simData.put(round, simData.get(round));
		}

		@Override
		public String toString() {
			return "SimulationDataMessage [conf=" + conf + ", simData=" + simData + "]";
		}
	}
}