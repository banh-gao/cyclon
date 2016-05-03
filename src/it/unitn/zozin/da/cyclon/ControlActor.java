package it.unitn.zozin.da.cyclon;

import it.unitn.zozin.da.cyclon.ControlActor.Configuration.Topology;
import it.unitn.zozin.da.cyclon.GraphActor.AddNodeEndedMessage;
import it.unitn.zozin.da.cyclon.GraphActor.AddNodeMessage;
import it.unitn.zozin.da.cyclon.GraphActor.EndRoundMessage;
import it.unitn.zozin.da.cyclon.GraphActor.InitNodeEndedMessage;
import it.unitn.zozin.da.cyclon.GraphActor.InitNodeMessage;
import it.unitn.zozin.da.cyclon.GraphActor.RemoveNodeEndedMessage;
import it.unitn.zozin.da.cyclon.GraphActor.StartRoundMessage;
import it.unitn.zozin.da.cyclon.StartMeasureMessage.SimulationDataMessage;
import java.util.NavigableSet;
import java.util.TreeSet;
import akka.actor.AbstractFSM;
import akka.actor.ActorPath;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;

class ControlActor extends AbstractFSM<ControlActor.State, ControlActor.Data> {

	enum State {
		Idle,
		NodesRemoval,
		NodesAdding,
		NodesInit,
		RoundRunning,
		MeasureRunning
	}

	interface Data {

	}

	enum Uninitialized implements Data {
		Uninitialized
	}

	// Simulation params
	public static class Configuration implements Data {

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

	class NodesCount implements Data {

		int nodes;

		public NodesCount(int nodes) {
			super();
			this.nodes = nodes;
		}

	}

	{
		startWith(State.Idle, Uninitialized.Uninitialized);

		when(State.Idle, matchEvent(Configuration.class, (confMsg, data) -> initSimulation(confMsg)));

		when(State.NodesRemoval, matchEvent(RemoveNodeEndedMessage.class, NodesCount.class, (endRemMsg, nodeCount) -> processNodeRemoved(endRemMsg, nodeCount)));

		when(State.NodesAdding, matchEvent(AddNodeEndedMessage.class, NodesCount.class, (endAddMsg, nodeCount) -> processNodeAdded(endAddMsg, nodeCount)));

		when(State.NodesInit, matchEvent(InitNodeEndedMessage.class, NodesCount.class, (endInitMsg, nodeCount) -> processNodeInitialized(endInitMsg, nodeCount)));

		when(State.RoundRunning, matchEvent(EndRoundMessage.class, (endRoundMsg, data) -> processRoundEnded(endRoundMsg)));

		when(State.MeasureRunning, matchEvent(SimulationDataMessage.class, (measureMsg, data) -> processMeasureData(measureMsg)));
	}

	private ActorPath GRAPH;
	private Configuration conf;
	private ActorRef sender;

	private NavigableSet<ActorRef> nodes = new TreeSet<ActorRef>();

	private int currentRound;

	@Override
	public void preStart() throws Exception {
		ActorPath userRoot = self().path().parent();
		GRAPH = userRoot.child("graph");
	}

	private akka.actor.FSM.State<State, Data> initSimulation(Configuration conf) {
		this.conf = conf;
		sender = sender();
		currentRound = 0;

		return prepareSimulationRound();
	}

	private akka.actor.FSM.State<State, Data> prepareSimulationRound() {
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

	private akka.actor.FSM.State<State, Data> removeNodes(int nodes) {
		ActorSelection g = context().actorSelection(GRAPH);
		for (int i = 0; i < nodes; i++)
			g.tell(new GraphActor.RemoveNodeMessage(), self());

		if (nodes > 0)
			return goTo(State.NodesRemoval).using(new NodesCount(nodes));
		else
			return addNodes(conf.NODE_ADD);
	}

	private akka.actor.FSM.State<State, Data> processNodeRemoved(RemoveNodeEndedMessage message, NodesCount count) {
		count.nodes--;
		nodes.remove(message.removedNode);

		if (count.nodes == 0) {
			count.nodes = conf.NODE_ADD;
			addNodes(count.nodes);
			return goTo(State.NodesAdding).using(count);
		} else
			return stay();
	}

	private akka.actor.FSM.State<State, Data> addNodes(int nodes) {
		ActorSelection g = context().actorSelection(GRAPH);
		for (int i = 0; i < nodes; i++) {
			AddNodeMessage msg = new GraphActor.AddNodeMessage();
			g.tell(msg, self());
		}

		if (nodes > 0)
			return goTo(State.NodesAdding).using(new NodesCount(nodes));
		else
			return initNodes();
	}

	private akka.actor.FSM.State<State, Data> processNodeAdded(AddNodeEndedMessage message, NodesCount count) {
		count.nodes--;
		nodes.add(message.newNode);

		if (count.nodes == 0) {
			initNodes();
			count.nodes = nodes.size();
			return goTo(State.NodesInit).using(count);
		}
		return stay();
	}

	private akka.actor.FSM.State<State, Data> initNodes() {
		for (ActorRef node : nodes) {
			ActorRef bootNode = getBootNeighbor(node);
			InitNodeMessage initMsg = new GraphActor.InitNodeMessage(conf.CYCLON_CACHE_SIZE, conf.CYCLON_SHUFFLE_LENGTH, bootNode);
			node.tell(initMsg, self());
		}

		if (nodes.size() > 0)
			return goTo(State.NodesInit).using(new NodesCount(nodes.size()));
		else
			return executeProtocolRound();
	}

	/**
	 * Determine which node the given node as to use as boot neighbor
	 * 
	 * @param node
	 * @return
	 */
	private ActorRef getBootNeighbor(ActorRef node) {
		if (conf.BOOT_TOPOLOGY == Topology.CHAIN) {
			ActorRef bootIdx = nodes.higher(node);

			if (bootIdx == null)
				bootIdx = nodes.first();

			return bootIdx;
		} else {
			// Star topology on first node
			return nodes.first();
		}
	}

	private akka.actor.FSM.State<State, Data> processNodeInitialized(InitNodeEndedMessage message, NodesCount count) {
		count.nodes--;
		if (count.nodes == 0) {
			return executeProtocolRound();
		} else
			return stay();
	}

	private akka.actor.FSM.State<State, Data> executeProtocolRound() {
		System.out.println("Starting round " + (currentRound + 1) + "... ");
		ActorSelection g = context().actorSelection(GRAPH);
		g.tell(new StartRoundMessage(), self());
		return goTo(State.RoundRunning);
	}

	private akka.actor.FSM.State<State, Data> processRoundEnded(EndRoundMessage roundMsg) {
		System.out.println("Completed round " + (currentRound + 1));
		currentRound++;
		return prepareSimulationRound();
	}

	private void executeMeasure() {
		ActorSelection g = context().actorSelection(GRAPH);
		g.tell(new StartMeasureMessage(), self());
	}

	private akka.actor.FSM.State<State, Data> processMeasureData(SimulationDataMessage measureMsg) {
		sender.tell(measureMsg, self());
		return goTo(State.Idle).using(Uninitialized.Uninitialized);
	}
}