package it.unitn.zozin.da.cyclon;

import it.unitn.zozin.da.cyclon.ControlActor.Configuration.Topology;
import it.unitn.zozin.da.cyclon.GraphActor.AddNodeEndedMessage;
import it.unitn.zozin.da.cyclon.GraphActor.AddNodeMessage;
import it.unitn.zozin.da.cyclon.GraphActor.EndRoundMessage;
import it.unitn.zozin.da.cyclon.GraphActor.InitNodeEndedMessage;
import it.unitn.zozin.da.cyclon.GraphActor.InitNodeMessage;
import it.unitn.zozin.da.cyclon.GraphActor.MeasureMessage.SimulationDataMessage;
import it.unitn.zozin.da.cyclon.GraphActor.StartMeasureMessage;
import it.unitn.zozin.da.cyclon.GraphActor.StartRoundMessage;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import akka.actor.ActorPath;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.UntypedActor;

class ControlActor extends UntypedActor {

	private static ActorPath GRAPH;

	private static final MessageMatcher<ControlActor> MATCHER = MessageMatcher.getInstance();

	private static final BiConsumer<Configuration, ControlActor> PROCESS_CONF = (Configuration conf, ControlActor c) -> {
		c.conf = conf;
		c.sender = c.getSender();
		c.startSimulation();
	};

	private static final BiConsumer<EndRoundMessage, ControlActor> PROCESS_ROUND_END = (EndRoundMessage status, ControlActor c) -> {
		c.runSimulationRound();

	};

	private static final BiConsumer<SimulationDataMessage, ControlActor> PROCESS_MEASURE_DATA = (SimulationDataMessage data, ControlActor c) -> {
		// TODO: aggregate per round
		c.sender.tell(data, c.getSelf());
		// Start next round

	};
	private static final BiConsumer<AddNodeEndedMessage, ControlActor> PROCESS_NODE_ADDED = (AddNodeEndedMessage message, ControlActor c) -> {
		c.newNodes.add(message.newNode);
		if (c.newNodes.size() == c.conf.NODES)
			c.initNodes();
	};

	private static final BiConsumer<InitNodeEndedMessage, ControlActor> PROCESS_NODE_INITIALIZED = (InitNodeEndedMessage message, ControlActor c) -> {
		c.pendingNodes--;
		if (c.pendingNodes == 0)
			c.runSimulationRound();
	};

	static {
		MATCHER.set(Configuration.class, PROCESS_CONF);
		MATCHER.set(AddNodeEndedMessage.class, PROCESS_NODE_ADDED);
		MATCHER.set(InitNodeEndedMessage.class, PROCESS_NODE_INITIALIZED);
		MATCHER.set(EndRoundMessage.class, PROCESS_ROUND_END);
		MATCHER.set(SimulationDataMessage.class, PROCESS_MEASURE_DATA);
	}

	private Configuration conf;
	private ActorRef sender;

	private List<ActorRef> newNodes = new ArrayList<ActorRef>();
	private int pendingNodes;
	private int remainingRounds;

	@Override
	public void preStart() throws Exception {
		ActorPath userRoot = getSelf().path().parent();
		GRAPH = userRoot.child("graph");
	}

	@Override
	public void onReceive(Object message) throws Exception {
		MATCHER.process(message, this);
	}

	private void startSimulation() {
		addNodes(conf.NODES);
		remainingRounds = conf.ROUNDS;
	}

	private void addNodes(int nodes) {
		ActorSelection g = getContext().actorSelection(GRAPH);

		for (int i = 0; i < nodes; i++) {
			AddNodeMessage msg = new GraphActor.AddNodeMessage();
			g.tell(msg, getSelf());
		}
	}

	private void initNodes() {
		pendingNodes = newNodes.size();
		for (ActorRef node : newNodes) {
			ActorRef bootNode = getBootNeighbor(node);
			InitNodeMessage initMsg = new GraphActor.InitNodeMessage(conf.CYCLON_CACHE_SIZE, conf.CYCLON_SHUFFLE_LENGTH, bootNode);
			node.tell(initMsg, getSelf());
		}
	}

	/**
	 * Determine which node the given node as to use as boot neighbor
	 * 
	 * @param node
	 * @return
	 */
	private ActorRef getBootNeighbor(ActorRef node) {
		if (conf.BOOT_TOPOLOGY == Topology.CHAIN) {
			int bootIdx = newNodes.indexOf(node) - 1;

			if (bootIdx == -1)
				bootIdx = newNodes.size() - 1;

			return newNodes.get(bootIdx);
		} else {
			// Star topology on first node
			return newNodes.get(0);
		}
	}

	private void runSimulationRound() {
		if (remainingRounds == 0) {
			executeMeasure();
			return;
		}
		remainingRounds--;

		removeNodes(conf.NODE_REM);
		addNodes(conf.NODE_ADD);

		executeProtocolRound();
	}

	private void removeNodes(int nodes) {
		ActorSelection g = getContext().actorSelection(GRAPH);
		for (int i = 0; i < nodes; i++)
			g.tell(new GraphActor.RemoveNodeMessage(), getSelf());
	}

	private void executeProtocolRound() {
		ActorSelection g = getContext().actorSelection(GRAPH);
		g.tell(new StartRoundMessage(), getSelf());
	}

	private void executeMeasure() {
		ActorSelection g = getContext().actorSelection(GRAPH);
		g.tell(new StartMeasureMessage(), getSelf());
	}

	// Simulation params
	public static class Configuration {

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

}