package it.unitn.zozin.da.cyclon;

import it.unitn.zozin.da.cyclon.ControlActor.Configuration.Topology;
import it.unitn.zozin.da.cyclon.GraphActor.AddNodeEndedMessage;
import it.unitn.zozin.da.cyclon.GraphActor.AddNodeMessage;
import it.unitn.zozin.da.cyclon.GraphActor.SimulationDataMessage;
import it.unitn.zozin.da.cyclon.NodeActor.BootNodeEndedMessage;
import it.unitn.zozin.da.cyclon.NodeActor.EndJoinMessage;
import it.unitn.zozin.da.cyclon.NodeActor.EndRoundMessage;
import it.unitn.zozin.da.cyclon.NodeActor.StartJoinMessage;
import it.unitn.zozin.da.cyclon.NodeActor.StartRoundMessage;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.NavigableSet;
import java.util.Properties;
import java.util.TreeSet;
import akka.actor.AbstractFSM;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;

class ControlActor extends AbstractFSM<ControlActor.State, ControlActor.StateData> {

	enum State {
		Idle,
		NodesAdding,
		NodesBoot,
		BootMeasureRunning,
		NodesJoining,
		RoundRunning,
		MeasureRunning
	}

	interface StateData {

	}

	private enum Uninitialized implements StateData {
		Uninitialized
	}

	// Simulation param message
	public static class Configuration implements StateData {

		enum Topology {
			CHAIN, STAR
		};

		public Topology BOOT_TOPOLOGY;
		int NODES;
		int ROUNDS;
		int CYCLON_CACHE_SIZE;
		int CYCLON_SHUFFLE_LENGTH;

		boolean PER_ROUND_MEASURE = false;

		public void load(FileInputStream inStream) throws IOException {
			Properties props = new Properties();
			props.load(inStream);
			NODES = Integer.parseInt(props.getProperty("nodes"));
			ROUNDS = Integer.parseInt(props.getProperty("rounds"));
			BOOT_TOPOLOGY = Topology.valueOf(props.getProperty("topology").toUpperCase());

			CYCLON_CACHE_SIZE = Integer.parseInt(props.getProperty("cyclonCache"));
			CYCLON_SHUFFLE_LENGTH = Integer.parseInt(props.getProperty("cyclonShuffle"));
		}
	}

	class CompletionCount implements StateData {

		private final int totalPending;
		private int count;

		public CompletionCount(int totalPending) {
			super();
			this.totalPending = totalPending;
		}

		public void increaseOne() {
			count += 1;
		}

		public boolean isCompleted() {
			return count == totalPending;
		}
	}

	class AddedNodes implements StateData {

		private final int totalNodes;
		private final NavigableSet<ActorRef> addedNodes = new TreeSet<ActorRef>();

		public AddedNodes(int totalNodes) {
			this.totalNodes = totalNodes;
		}

		public void increaseOne(ActorRef addedNode) {
			addedNodes.add(addedNode);
		}

		public boolean isCompleted() {
			return addedNodes.size() == totalNodes;
		}
	}

	{
		startWith(State.Idle, Uninitialized.Uninitialized);

		when(State.Idle, matchEvent(Configuration.class, (confMsg, data) -> initSimulation(confMsg)));

		when(State.NodesAdding, matchEvent(AddNodeEndedMessage.class, AddedNodes.class, (endAddMsg, addedNodes) -> processNodeAdded(endAddMsg, addedNodes)));

		when(State.NodesBoot, matchEvent(BootNodeEndedMessage.class, CompletionCount.class, (endInitMsg, nodeCount) -> processNodeBooted(endInitMsg, nodeCount)));

		when(State.BootMeasureRunning, matchEvent(SimulationDataMessage.class, (measureMsg, data) -> processBootMeasure(measureMsg)));

		when(State.NodesJoining, matchEvent(EndJoinMessage.class, CompletionCount.class, (endInitMsg, roundCount) -> processJoinEnded()));

		when(State.MeasureRunning, matchEvent(SimulationDataMessage.class, CompletionCount.class, (measureMsg, roundCount) -> processMeasure(measureMsg, roundCount)));

		when(State.RoundRunning, matchEvent(EndRoundMessage.class, CompletionCount.class, (endRoundMsg, roundCount) -> processRoundEnded(endRoundMsg, roundCount)));

	}

	private ActorSelection GRAPH;

	private ActorRef simSender;
	private Configuration conf;

	@Override
	public void preStart() throws Exception {
		GRAPH = context().actorSelection("../graph");
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
		addedNodes.increaseOne(message.newNode);

		if (addedNodes.isCompleted())
			return executeNodesBoot(addedNodes.addedNodes);
		else
			return stay();
	}

	private akka.actor.FSM.State<State, StateData> executeNodesBoot(NavigableSet<ActorRef> addedNodes) {
		System.out.print("Executing [BOOT]... ");
		for (ActorRef n : addedNodes) {
			ActorRef bootNode = getIntroducerNode(addedNodes, n);
			n.tell(new NodeActor.BootNodeMessage(conf.CYCLON_CACHE_SIZE, conf.CYCLON_SHUFFLE_LENGTH, bootNode), self());
		}

		return goTo(State.NodesBoot).using(new CompletionCount(addedNodes.size()));
	}

	/**
	 * Determine which node the given node as to use as introducer node
	 */
	private ActorRef getIntroducerNode(NavigableSet<ActorRef> addedNodes, ActorRef node) {
		if (conf.BOOT_TOPOLOGY == Topology.CHAIN) {
			ActorRef introducer = addedNodes.higher(node);

			if (introducer == null)
				introducer = addedNodes.first();

			return introducer;
		} else {
			// Star topology centered on first node
			return addedNodes.first();
		}
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
		GRAPH.tell(new GraphActor.StartMeasureMessage(), self());
		return goTo(State.BootMeasureRunning);
	}

	private akka.actor.FSM.State<State, StateData> processBootMeasure(SimulationDataMessage measureMsg) {
		System.out.println("[completed] -> " + measureMsg);
		return executeNodesJoin();
	}

	private akka.actor.FSM.State<State, StateData> executeNodesJoin() {
		System.out.print("Executing [JOIN]... ");
		GRAPH.tell(new StartJoinMessage(), self());
		return goTo(State.NodesJoining).using(new CompletionCount(conf.ROUNDS));
	}

	private akka.actor.FSM.State<State, StateData> processJoinEnded() {
		System.out.println("[completed]");
		return executeMeasure(0);
	}

	private akka.actor.FSM.State<State, StateData> executeMeasure(int round) {
		if (round == 0)
			System.out.print("Measuring [JOIN]... ");
		else
			System.out.print("Measuring round " + round + "... ");

		GRAPH.tell(new GraphActor.StartMeasureMessage(), self());
		return goTo(State.MeasureRunning);
	}

	private akka.actor.FSM.State<State, StateData> processMeasure(SimulationDataMessage measureMsg, CompletionCount roundCount) {
		System.out.println("[completed] -> " + measureMsg);

		if (roundCount.isCompleted()) {
			// Send report back to simulation starter
			simSender.tell(measureMsg, self());
			return goTo(State.Idle).using(Uninitialized.Uninitialized);
		} else {
			return executeProtocolRound(roundCount);
		}
	}

	private akka.actor.FSM.State<State, StateData> executeProtocolRound(CompletionCount roundCount) {
		System.out.print("Executing round " + (roundCount.count + 1) + "... ");
		GRAPH.tell(new StartRoundMessage(), self());
		return goTo(State.RoundRunning);
	}

	private akka.actor.FSM.State<State, StateData> processRoundEnded(EndRoundMessage roundMsg, CompletionCount roundCount) {
		System.out.println("[completed]");
		roundCount.increaseOne();

		if (roundCount.isCompleted() || conf.PER_ROUND_MEASURE || roundCount.count == 0)
			return executeMeasure(roundCount.count);
		else
			return executeProtocolRound(roundCount);
	}
}