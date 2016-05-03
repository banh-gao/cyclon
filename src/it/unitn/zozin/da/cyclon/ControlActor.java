package it.unitn.zozin.da.cyclon;

import it.unitn.zozin.da.cyclon.ControlActor.Configuration.Topology;
import it.unitn.zozin.da.cyclon.GraphActor.AddNodeEndedMessage;
import it.unitn.zozin.da.cyclon.GraphActor.AddNodeMessage;
import it.unitn.zozin.da.cyclon.GraphActor.EndRoundMessage;
import it.unitn.zozin.da.cyclon.GraphActor.InitNodeEndedMessage;
import it.unitn.zozin.da.cyclon.GraphActor.RemoveNodeEndedMessage;
import it.unitn.zozin.da.cyclon.GraphActor.SimulationDataMessage;
import it.unitn.zozin.da.cyclon.GraphActor.StartRoundMessage;
import java.util.NavigableSet;
import java.util.TreeSet;
import akka.actor.AbstractFSM;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;

class ControlActor extends AbstractFSM<ControlActor.State, ControlActor.StateData> {

	enum State {
		Uninitialized,
		NodesRemoving,
		NodesAdding,
		NodesInit,
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
		int NODE_ADD;
		int NODE_REM;
		int CYCLON_CACHE_SIZE;
		int CYCLON_SHUFFLE_LENGTH;
	}

	class NodesCount implements StateData {

		private final int totalNodes;
		private int count;

		public NodesCount(int totalNodes) {
			super();
			this.totalNodes = totalNodes;
		}

		public void increaseOne() {
			count += 1;
		}

		public boolean isCompleted() {
			return count == totalNodes;
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
		startWith(State.Uninitialized, Uninitialized.Uninitialized);

		when(State.Uninitialized, matchEvent(Configuration.class, (confMsg, data) -> initSimulation(confMsg)));

		when(State.NodesRemoving, matchEvent(RemoveNodeEndedMessage.class, NodesCount.class, (endRemMsg, nodeCount) -> processNodeRemoved(endRemMsg, nodeCount)));

		when(State.NodesAdding, matchEvent(AddNodeEndedMessage.class, AddedNodes.class, (endAddMsg, addedNodes) -> processNodeAdded(endAddMsg, addedNodes)));

		when(State.NodesInit, matchEvent(InitNodeEndedMessage.class, NodesCount.class, (endInitMsg, nodeCount) -> processNodeInitialized(endInitMsg, nodeCount)));

		when(State.RoundRunning, matchEvent(EndRoundMessage.class, (endRoundMsg, data) -> processRoundEnded(endRoundMsg)));

		when(State.MeasureRunning, matchEvent(SimulationDataMessage.class, (measureMsg, data) -> processMeasureData(measureMsg)));
	}

	private ActorSelection graph;

	private Configuration conf;
	private ActorRef simSender;

	private int currentRound;

	@Override
	public void preStart() throws Exception {
		graph = context().actorSelection("../graph");
	}

	private akka.actor.FSM.State<State, StateData> initSimulation(Configuration conf) {
		this.conf = conf;
		simSender = sender();
		currentRound = 0;

		return prepareSimulationRound();
	}

	private akka.actor.FSM.State<State, StateData> prepareSimulationRound() {
		if (currentRound >= conf.ROUNDS) {
			executeMeasure();
			return goTo(State.MeasureRunning);
		}

		// Init only for first round
		if (currentRound == 0) {
			return addNodes(conf.NODES + conf.NODE_ADD);
		} else {
			return removeNodes(conf.NODE_REM);
		}
	}

	private akka.actor.FSM.State<State, StateData> removeNodes(int nodes) {
		if (nodes == 0)
			return addNodes(conf.NODE_ADD);

		for (int i = 0; i < nodes; i++)
			graph.tell(new GraphActor.RemoveNodeMessage(), self());

		return goTo(State.NodesRemoving).using(new NodesCount(nodes));
	}

	private akka.actor.FSM.State<State, StateData> processNodeRemoved(RemoveNodeEndedMessage message, NodesCount count) {
		count.increaseOne();

		if (count.isCompleted())
			return addNodes(conf.NODE_ADD);
		else
			return stay();
	}

	private akka.actor.FSM.State<State, StateData> addNodes(int nodes) {
		if (nodes == 0)
			return executeProtocolRound();

		for (int i = 0; i < nodes; i++) {
			AddNodeMessage msg = new GraphActor.AddNodeMessage();
			graph.tell(msg, self());
		}

		return goTo(State.NodesAdding).using(new AddedNodes(nodes));
	}

	private akka.actor.FSM.State<State, StateData> processNodeAdded(AddNodeEndedMessage message, AddedNodes addedNodes) {
		addedNodes.increaseOne(message.newNode);

		if (addedNodes.isCompleted())
			return initNodes(addedNodes.addedNodes);
		else
			return stay();
	}

	private akka.actor.FSM.State<State, StateData> initNodes(NavigableSet<ActorRef> addedNodes) {
		if (addedNodes.size() == 0)
			return executeProtocolRound();

		for (ActorRef n : addedNodes) {
			ActorRef bootNode = getBootNeighbor(addedNodes, n);
			n.tell(new NodeActor.InitNodeMessage(conf.CYCLON_CACHE_SIZE, conf.CYCLON_SHUFFLE_LENGTH, bootNode), self());
		}

		return goTo(State.NodesInit).using(new NodesCount(addedNodes.size()));
	}

	/**
	 * Determine which node the given node as to use as boot neighbor
	 * 
	 * @param n
	 * @param addedNodes
	 * 
	 * @param node
	 * @return
	 */
	private ActorRef getBootNeighbor(NavigableSet<ActorRef> addedNodes, ActorRef node) {
		// FIXME: newNodes only contains nodes added in the last round
		if (conf.BOOT_TOPOLOGY == Topology.CHAIN) {
			ActorRef bootIdx = addedNodes.higher(node);

			if (bootIdx == null)
				bootIdx = addedNodes.first();

			return bootIdx;
		} else {
			// Star topology on first node
			return addedNodes.first();
		}
	}

	private akka.actor.FSM.State<State, StateData> processNodeInitialized(InitNodeEndedMessage message, NodesCount count) {
		count.increaseOne();
		if (count.isCompleted()) {
			return executeProtocolRound();
		} else
			return stay();
	}

	private akka.actor.FSM.State<State, StateData> executeProtocolRound() {
		System.out.println("Starting round " + (currentRound + 1) + "... ");
		graph.tell(new StartRoundMessage(), self());
		return goTo(State.RoundRunning);
	}

	private akka.actor.FSM.State<State, StateData> processRoundEnded(EndRoundMessage roundMsg) {
		System.out.println("Completed round " + (currentRound + 1));
		currentRound++;
		return prepareSimulationRound();
	}

	private void executeMeasure() {
		graph.tell(new GraphActor.StartMeasureMessage(), self());
	}

	private akka.actor.FSM.State<State, StateData> processMeasureData(SimulationDataMessage measureMsg) {
		simSender.tell(measureMsg, self());
		return goTo(State.Uninitialized).using(Uninitialized.Uninitialized);
	}
}