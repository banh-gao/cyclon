package it.unitn.zozin.da.cyclon;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.stream.Collectors;
import akka.actor.AbstractFSM;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import it.unitn.zozin.da.cyclon.GraphActor.EndAddNodesMessage;
import it.unitn.zozin.da.cyclon.GraphActor.EndBootMessage;
import it.unitn.zozin.da.cyclon.GraphActor.RoundData;
import it.unitn.zozin.da.cyclon.NodeActor.EndRound;
import it.unitn.zozin.da.cyclon.NodeActor.StartRound;
import it.unitn.zozin.da.cyclon.SimulationActor.SimulationStateData;

/**
 * Actor to control simulation execution
 */
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
		when(State.NodesAdding, matchEvent(EndAddNodesMessage.class, (endAddMsg, data) -> executeNodesBoot(endAddMsg.addedNodes)));
		when(State.NodesBoot, matchEvent(EndBootMessage.class, (endInitMsg, data) -> startSimulation()));

		// Run simulation
		when(State.RoundRunning, matchEvent(EndRound.class, SimulationStateData.class, (endRoundMsg, simState) -> processCyclonRoundEnded(endRoundMsg, simState)));
		when(State.MeasureRunning, matchEvent(RoundData.class, SimulationStateData.class, (measureMsg, simState) -> processMeasure(measureMsg, simState)));

	}

	static class SimulationStateData {

		private final int total;
		private int current = 0;

		final List<RoundData> simData = new LinkedList<RoundData>();

		public SimulationStateData(int total) {
			this.total = total;
		}

		public boolean isLast() {
			return current == total - 1;
		}

		public boolean isCompleted() {
			return current == total;
		}

		public void addData(RoundData data) {
			simData.add(data);
		}

		public void increaseRound() {
			current++;
		}

		public int getRound() {
			return current;
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

		GRAPH.tell(new GraphActor.StartAddNodesMessage(conf.NODES, conf.CYCLON_CACHE_SIZE, conf.CYCLON_SHUFFLE_LENGTH), self());
		return goTo(State.NodesAdding);
	}

	private akka.actor.FSM.State<State, SimulationStateData> executeNodesBoot(Set<ActorRef> addedNodes) {
		Main.LOGGER.log(Level.INFO, "Executing [BOOT]... ");

		NavigableSet<ActorRef> actorIds = new TreeSet<ActorRef>(Comparator.comparingInt(GraphActor::actorToInt));
		actorIds.addAll(addedNodes);

		// Defines the introducer for each added node
		Map<ActorRef, ActorRef> introducers = actorIds.stream().collect(Collectors.toMap(Function.identity(), (n) -> conf.BOOT_TOPOLOGY.getIntroducerNode(actorIds, n)));

		GRAPH.tell(new GraphActor.StartBootMessage(introducers), self());

		return goTo(State.NodesBoot);
	}

	private akka.actor.FSM.State<State, SimulationStateData> startSimulation() {
		Main.LOGGER.log(Level.INFO, "[completed]\n");
		return executeMeasure(new SimulationStateData(conf.ROUNDS + 1));
	}

	private akka.actor.FSM.State<State, SimulationStateData> executeMeasure(SimulationStateData simState) {
		// If the measure has to be taken only at the end, skip if this is not
		// the last round
		if (conf.FINAL_MEASURE_MODE && !simState.isLast()) {
			return controlSimulationRoundEnd(simState);
		}

		if (simState.getRound() == BOOT_ROUND)
			Main.LOGGER.log(Level.INFO, "Measuring [BOOT]... ");
		else
			Main.LOGGER.log(Level.INFO, "Measuring round " + simState.getRound() + "... ");

		GRAPH.tell(new GraphActor.StartMeasureMessage(conf.MEASURE), self());
		return goTo(State.MeasureRunning).using(simState);
	}

	private akka.actor.FSM.State<State, SimulationStateData> processMeasure(RoundData roundMeasureMsg, SimulationStateData simState) {
		Main.LOGGER.log(Level.INFO, "[completed] -> " + roundMeasureMsg + "\n");
		simState.addData(roundMeasureMsg);
		return controlSimulationRoundEnd(simState);
	}

	private akka.actor.FSM.State<State, SimulationStateData> controlSimulationRoundEnd(SimulationStateData simState) {
		simState.increaseRound();
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
			Main.LOGGER.log(Level.INFO, "Executing [BOOT]... ");
		else
			Main.LOGGER.log(Level.INFO, "Executing round " + (simState.getRound()) + "... ");

		GRAPH.tell(new StartRound(), self());
		return goTo(State.RoundRunning).using(simState);
	}

	private akka.actor.FSM.State<State, SimulationStateData> processCyclonRoundEnded(EndRound roundMsg, SimulationStateData simState) {
		Main.LOGGER.log(Level.INFO, "[completed]\n");
		return executeMeasure(simState);
	}

	// Simulation param message
	public static class Configuration {

		// Used to generate a random graph
		private static final Random rand = new Random();

		enum Topology {
			CHAIN {

				@Override
				ActorRef getIntroducerNode(NavigableSet<ActorRef> nodes, ActorRef n) {
					return (nodes.higher(n) != null) ? nodes.higher(n) : nodes.first();
				}
			},
			STAR {

				@Override
				ActorRef getIntroducerNode(NavigableSet<ActorRef> nodes, ActorRef n) {
					return nodes.first();
				}
			},
			RANDOM {

				@Override
				ActorRef getIntroducerNode(NavigableSet<ActorRef> nodes, ActorRef n) {
					return nodes.toArray(new ActorRef[]{})[rand.nextInt(nodes.size())];
				}
			};

			abstract ActorRef getIntroducerNode(NavigableSet<ActorRef> nodes, ActorRef n);
		};

		public Topology BOOT_TOPOLOGY;
		int NODES;
		int ROUNDS;
		int CYCLON_CACHE_SIZE;
		int CYCLON_SHUFFLE_LENGTH;
		GraphProperty MEASURE;
		boolean FINAL_MEASURE_MODE;

		public void load(FileInputStream inStream) throws IOException {
			Properties props = new Properties();
			props.load(inStream);
			NODES = Integer.parseInt(props.getProperty("nodes"));
			ROUNDS = Integer.parseInt(props.getProperty("rounds"));
			BOOT_TOPOLOGY = Topology.valueOf(props.getProperty("topology").toUpperCase());

			CYCLON_CACHE_SIZE = Integer.parseInt(props.getProperty("cyclonCache"));
			CYCLON_SHUFFLE_LENGTH = Integer.parseInt(props.getProperty("cyclonShuffle"));

			MEASURE = GraphProperty.valueOf(props.getProperty("measureType").trim().toUpperCase());

			FINAL_MEASURE_MODE = props.getProperty("measureMode", "final").equalsIgnoreCase("final");
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