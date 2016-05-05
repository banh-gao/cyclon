package it.unitn.zozin.da.cyclon;

import it.unitn.zozin.da.cyclon.ControlActor.Configuration.Topology;
import it.unitn.zozin.da.cyclon.GraphActor.AddNodeEndedMessage;
import it.unitn.zozin.da.cyclon.GraphActor.AddNodeMessage;
import it.unitn.zozin.da.cyclon.GraphActor.SimulationDataMessage;
import it.unitn.zozin.da.cyclon.NodeActor.EndJoinMessage;
import it.unitn.zozin.da.cyclon.NodeActor.EndRoundMessage;
import it.unitn.zozin.da.cyclon.NodeActor.InitNodeEndedMessage;
import it.unitn.zozin.da.cyclon.NodeActor.StartJoinMessage;
import it.unitn.zozin.da.cyclon.NodeActor.StartRoundMessage;
import java.util.NavigableSet;
import java.util.TreeSet;
import akka.actor.AbstractFSM;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;

class ControlActor extends AbstractFSM<ControlActor.State, ControlActor.StateData> {

	enum State {
		Idle,
		NodesAdding,
		NodesInit,
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

		when(State.NodesInit, matchEvent(InitNodeEndedMessage.class, CompletionCount.class, (endInitMsg, nodeCount) -> processNodeInitialized(endInitMsg, nodeCount)));

		when(State.BootMeasureRunning, matchEvent(SimulationDataMessage.class, (measureMsg, data) -> processBootMeasure(measureMsg)));

		when(State.NodesJoining, matchEvent(EndJoinMessage.class, (endInitMsg, data) -> executeMeasure()));

		when(State.MeasureRunning, matchEvent(SimulationDataMessage.class, CompletionCount.class, (measureMsg, roundCount) -> processMeasure(measureMsg, roundCount)));

		when(State.RoundRunning, matchEvent(EndRoundMessage.class, CompletionCount.class, (endRoundMsg, roundCount) -> processRoundEnded(endRoundMsg, roundCount)));

	}

	private ActorSelection GRAPH;

	private ActorRef simSender;
	private Configuration conf;
	private SimulationDataMessage aggregateMeasure;

	@Override
	public void preStart() throws Exception {
		GRAPH = context().actorSelection("../graph");
	}

	private akka.actor.FSM.State<State, StateData> initSimulation(Configuration conf) {
		simSender = sender();
		this.conf = conf;
		aggregateMeasure = null;

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
			return executeNodesInit(addedNodes.addedNodes);
		else
			return stay();
	}

	private akka.actor.FSM.State<State, StateData> executeNodesInit(NavigableSet<ActorRef> addedNodes) {
		for (ActorRef n : addedNodes) {
			ActorRef bootNode = getIntroducerNode(addedNodes, n);
			n.tell(new NodeActor.InitNodeMessage(conf.CYCLON_CACHE_SIZE, conf.CYCLON_SHUFFLE_LENGTH, bootNode), self());
		}

		return goTo(State.NodesInit).using(new CompletionCount(addedNodes.size()));
	}

	/**
	 * Determine which node the given node as to use as introducer node
	 */
	private ActorRef getIntroducerNode(NavigableSet<ActorRef> addedNodes, ActorRef node) {
		// FIXME: newNodes only contains nodes added in the last round
		if (conf.BOOT_TOPOLOGY == Topology.CHAIN) {
			ActorRef introducer = addedNodes.higher(node);

			if (introducer == null)
				introducer = addedNodes.first();

			return introducer;
		} else {
			// Star topology on first node
			return addedNodes.first();
		}
	}

	private akka.actor.FSM.State<State, StateData> processNodeInitialized(InitNodeEndedMessage message, CompletionCount count) {
		count.increaseOne();

		if (count.isCompleted()) {
			return executePreMeasure();
		} else
			return stay();
	}

	private akka.actor.FSM.State<State, StateData> executePreMeasure() {
		GRAPH.tell(new GraphActor.StartMeasureMessage(), self());
		return goTo(State.BootMeasureRunning);
	}

	private akka.actor.FSM.State<State, StateData> processBootMeasure(SimulationDataMessage measureMsg) {
		// simSender.tell(measureMsg, self());
		System.out.println("Bootstrap measure: " + measureMsg);
		return executeNodesJoin();
	}

	private akka.actor.FSM.State<State, StateData> executeNodesJoin() {
		GRAPH.tell(new StartJoinMessage(), self());
		return goTo(State.NodesJoining).using(new CompletionCount(conf.ROUNDS));
	}

	private akka.actor.FSM.State<State, StateData> executeMeasure() {
		GRAPH.tell(new GraphActor.StartMeasureMessage(), self());
		return goTo(State.MeasureRunning);
	}

	private akka.actor.FSM.State<State, StateData> processMeasure(SimulationDataMessage measureMsg, CompletionCount roundCount) {
		aggregateMeasure(measureMsg);

		if (roundCount.count == 0)
			System.out.println("Join measure: " + measureMsg);
		else
			System.out.println("Measure after round " + roundCount.count + ": " + measureMsg);

		if (roundCount.isCompleted()) {
			// Send report back to simulation starter
			simSender.tell(aggregateMeasure, self());
			return goTo(State.Idle).using(Uninitialized.Uninitialized);
		} else {
			return executeProtocolRound(roundCount);
		}
	}

	private void aggregateMeasure(SimulationDataMessage roundMeasure) {
		aggregateMeasure = roundMeasure;
	}

	private akka.actor.FSM.State<State, StateData> executeProtocolRound(CompletionCount roundCount) {
		System.out.println("Starting round " + (roundCount.count + 1) + "... ");
		GRAPH.tell(new StartRoundMessage(), self());
		return goTo(State.RoundRunning);
	}

	private akka.actor.FSM.State<State, StateData> processRoundEnded(EndRoundMessage roundMsg, CompletionCount roundCount) {
		System.out.println("Completed round " + (roundCount.count + 1));
		roundCount.increaseOne();
		return executeMeasure();
	}

}