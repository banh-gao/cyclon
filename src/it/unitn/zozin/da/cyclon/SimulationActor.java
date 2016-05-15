package it.unitn.zozin.da.cyclon;

import it.unitn.zozin.da.cyclon.DataProcessor.GraphProperty;
import it.unitn.zozin.da.cyclon.DataProcessor.RoundData;
import it.unitn.zozin.da.cyclon.GraphActor.AddNodeEndedMessage;
import it.unitn.zozin.da.cyclon.GraphActor.AddNodeMessage;
import it.unitn.zozin.da.cyclon.NodeActor.BootNodeEndedMessage;
import it.unitn.zozin.da.cyclon.NodeActor.EndJoinMessage;
import it.unitn.zozin.da.cyclon.NodeActor.EndRoundMessage;
import it.unitn.zozin.da.cyclon.NodeActor.StartJoinMessage;
import it.unitn.zozin.da.cyclon.NodeActor.StartRoundMessage;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import akka.actor.AbstractFSM;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;

class SimulationActor extends AbstractFSM<SimulationActor.State, SimulationActor.StateData> {

	enum State {
		Idle,
		NodesAdding,
		PreMeasureRunning,
		NodesBoot,
		NodesJoining,
		RoundRunning,
		MeasureRunning
	}

	{
		startWith(State.Idle, null);

		// Configure simulation

		when(State.Idle, matchEvent(Configuration.class, (confMsg, data) -> initSimulation(confMsg)));

		// Prepare simulation

		when(State.NodesAdding, matchEvent(AddNodeEndedMessage.class, AddedNodes.class, (endAddMsg, addedNodes) -> processNodeAdded(endAddMsg, addedNodes)));
		when(State.PreMeasureRunning, matchEvent(RoundData.class, (measureMsg, data) -> processPreMeasure(measureMsg)));
		when(State.NodesBoot, matchEvent(BootNodeEndedMessage.class, CompletionCount.class, (endInitMsg, nodeCount) -> processNodeBooted(endInitMsg, nodeCount)));
		when(State.NodesJoining, matchEvent(EndJoinMessage.class, (endInitMsg, roundCount) -> processJoinEnded()));

		// Run simulation

		when(State.MeasureRunning, matchEvent(RoundData.class, SimulationStateData.class, (measureMsg, simState) -> processMeasure(measureMsg, simState)));
		when(State.RoundRunning, matchEvent(EndRoundMessage.class, SimulationStateData.class, (endRoundMsg, simState) -> processRoundEnded(endRoundMsg, simState)));

	}

	interface StateData {

	}

	class CompletionCount implements StateData {

		private final int total;
		private int count;

		public CompletionCount(int totalPending) {
			this.total = totalPending;
		}

		public void increaseOne() {
			count += 1;
		}

		public boolean isCompleted() {
			return count == total;
		}
	}

	class SimulationStateData implements StateData {

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

	// Used to generate a random graph
	private final Random rand = new Random();

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

	private akka.actor.FSM.State<State, StateData> initSimulation(Configuration conf) {
		simSender = sender();
		this.conf = conf;

		return addNodes(conf.NODES);
	}

	private akka.actor.FSM.State<State, StateData> addNodes(int nodes) {
		for (int i = 0; i < nodes; i++) {
			AddNodeMessage msg = new GraphActor.AddNodeMessage();
			GRAPH.tell(msg, self());
		}

		return goTo(State.NodesAdding).using(new AddedNodes(nodes));
	}

	private akka.actor.FSM.State<State, StateData> processNodeAdded(AddNodeEndedMessage message, AddedNodes addedNodes) {
		addedNodes.increaseOne(message.nodeIndex, message.newNode);

		if (addedNodes.isCompleted())
			return executeNodesBoot(addedNodes.addedNodes);
		else
			return stay();
	}

	private akka.actor.FSM.State<State, StateData> executeNodesBoot(NavigableMap<Integer, ActorRef> addedNodes) {
		System.out.print("Executing [BOOT]... ");
		for (Entry<Integer, ActorRef> n : addedNodes.entrySet()) {
			ActorRef bootNode = getIntroducerNode(addedNodes, n.getKey());
			n.getValue().tell(new NodeActor.BootNodeMessage(conf.CYCLON_CACHE_SIZE, conf.CYCLON_SHUFFLE_LENGTH, bootNode), self());
		}

		return goTo(State.NodesBoot).using(new CompletionCount(addedNodes.size()));
	}

	/**
	 * Determine which node the given node as to use as introducer node
	 */
	private ActorRef getIntroducerNode(NavigableMap<Integer, ActorRef> addedNodes, int nodeIndex) {
		Entry<Integer, ActorRef> introducerE = null;
		switch (conf.BOOT_TOPOLOGY) {
			case CHAIN :
				introducerE = addedNodes.higherEntry(nodeIndex);

				if (introducerE == null)
					introducerE = addedNodes.firstEntry();

				break;
			case STAR :
				// Star topology is centered on first node
				introducerE = addedNodes.firstEntry();
				break;
			case RANDOM :
				introducerE = addedNodes.higherEntry(rand.nextInt(addedNodes.size()));

				if (introducerE == null)
					introducerE = addedNodes.firstEntry();

				break;
			default :
				throw new AssertionError();
		}

		return introducerE.getValue();
	}

	private akka.actor.FSM.State<State, StateData> processNodeBooted(BootNodeEndedMessage message, CompletionCount count) {
		count.increaseOne();

		if (count.isCompleted()) {
			System.out.println("[completed]");
			return executePreMeasure();
		} else
			return stay();
	}

	private akka.actor.FSM.State<State, StateData> executePreMeasure() {
		System.out.print("Measuring [BOOT]... ");

		GRAPH.tell(new GraphActor.StartMeasureMessage(conf.FINAL_MEASURE), self());
		return goTo(State.PreMeasureRunning);
	}

	private akka.actor.FSM.State<State, StateData> processPreMeasure(RoundData preMeasureMsg) {
		System.out.println("[completed] -> " + preMeasureMsg);
		return executeNodesJoin();
	}

	private akka.actor.FSM.State<State, StateData> executeNodesJoin() {
		System.out.print("Executing [JOIN]... ");
		GRAPH.tell(new StartJoinMessage(), self());
		return goTo(State.NodesJoining);
	}

	private akka.actor.FSM.State<State, StateData> processJoinEnded() {
		System.out.println("[completed]");

		// Simulation is excuted for conf.ROUNDS + the join round
		SimulationStateData simState = new SimulationStateData(conf.ROUNDS + 1);
		return executeMeasure(simState);
	}

	private akka.actor.FSM.State<State, StateData> executeMeasure(SimulationStateData simState) {
		if (simState.getRound() == 0)
			System.out.print("Measuring [JOIN]... ");
		else
			System.out.print("Measuring round " + simState.getRound() + "... ");

		Set<GraphProperty> measureParams;

		if (simState.isLast())
			measureParams = conf.FINAL_MEASURE;
		else
			measureParams = conf.ROUND_MEASURE;

		GRAPH.tell(new GraphActor.StartMeasureMessage(measureParams), self());
		return goTo(State.MeasureRunning).using(simState);
	}

	private akka.actor.FSM.State<State, StateData> processMeasure(RoundData roundMeasureMsg, SimulationStateData simulationMeasure) {
		System.out.println("[completed] -> " + roundMeasureMsg);

		simulationMeasure.increaseRound(roundMeasureMsg);

		if (simulationMeasure.isCompleted()) {
			// Send report back to simulation starter
			simSender.tell(new SimulationDataMessage(conf, simulationMeasure.simData), self());
			return goTo(State.Idle);
		} else {
			return executeProtocolRound(simulationMeasure);
		}
	}

	private akka.actor.FSM.State<State, StateData> executeProtocolRound(SimulationStateData simState) {
		System.out.print("Executing round " + (simState.getRound()) + "... ");
		GRAPH.tell(new StartRoundMessage(), self());
		return goTo(State.RoundRunning).using(simState);
	}

	private akka.actor.FSM.State<State, StateData> processRoundEnded(EndRoundMessage roundMsg, SimulationStateData simState) {
		System.out.println("[completed]");
		return executeMeasure(simState);
	}

	// Simulation param message
	public static class Configuration implements StateData {

		enum Topology {
			CHAIN, STAR, RANDOM
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
			for (String m : props.getProperty("roundMeasure", "").split(","))
				ROUND_MEASURE.add(GraphProperty.valueOf(m.trim().toUpperCase()));

			FINAL_MEASURE = new HashSet<DataProcessor.GraphProperty>();
			for (String m : props.getProperty("finalMeasure", "").split(","))
				FINAL_MEASURE.add(GraphProperty.valueOf(m.trim().toUpperCase()));
		}
	}

	class AddedNodes implements StateData {

		private final int totalNodes;
		private final NavigableMap<Integer, ActorRef> addedNodes = new TreeMap<Integer, ActorRef>();

		public AddedNodes(int totalNodes) {
			this.totalNodes = totalNodes;
		}

		public void increaseOne(int index, ActorRef node) {
			addedNodes.put(index, node);
		}

		public boolean isCompleted() {
			return addedNodes.size() == totalNodes;
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