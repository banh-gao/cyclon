package it.unitn.zozin.da.cyclon;

import it.unitn.zozin.da.cyclon.ControlActor.Configuration.Topology;
import it.unitn.zozin.da.cyclon.GraphActor.AddNodeEndedMessage;
import it.unitn.zozin.da.cyclon.GraphActor.AddNodeMessage;
import it.unitn.zozin.da.cyclon.GraphActor.EndRoundMessage;
import it.unitn.zozin.da.cyclon.GraphActor.InitNodeEndedMessage;
import it.unitn.zozin.da.cyclon.GraphActor.InitNodeMessage;
import it.unitn.zozin.da.cyclon.GraphActor.StartRoundMessage;
import it.unitn.zozin.da.cyclon.StartMeasureMessage.SimulationDataMessage;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.function.BiConsumer;
import akka.actor.ActorPath;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.UntypedActor;

class ControlActor extends UntypedActor {

	private static ActorPath GRAPH;

	private static final MessageMatcher<ControlActor> MATCHER = MessageMatcher.getInstance();

	private static final BiConsumer<SimulationDataMessage, ControlActor> PROCESS_MEASURE_DATA = (SimulationDataMessage data, ControlActor c) -> {
		System.out.println(c.nodes);
		c.sender.tell(data, c.getSelf());
	};

	static {
		MATCHER.set(Configuration.class, ControlActor::processConf);
		MATCHER.set(AddNodeEndedMessage.class, ControlActor::processNodeAdded);
		// MATCHER.set(RemoveNodeEndedMessage.class,
		// ControlActor::processNodeRemoved);
		MATCHER.set(InitNodeEndedMessage.class, ControlActor::processNodeInitialized);
		MATCHER.set(EndRoundMessage.class, ControlActor::processRoundEnded);
		MATCHER.set(SimulationDataMessage.class, PROCESS_MEASURE_DATA);
	}

	private Configuration conf;
	private ActorRef sender;

	private NavigableSet<ActorRef> nodes = new TreeSet<ActorRef>();
	private int pendingNodes;
	private int round;

	@Override
	public void preStart() throws Exception {
		ActorPath userRoot = getSelf().path().parent();
		GRAPH = userRoot.child("graph");
	}

	@Override
	public void onReceive(Object message) throws Exception {
		MATCHER.process(message, this);
	}

	private static void processConf(Configuration conf, ControlActor c) {
		c.conf = conf;
		c.sender = c.getSender();
		c.round = 0;

		c.prepareSimulationRound();
	}

	private void prepareSimulationRound() {
		if (round >= conf.ROUNDS) {
			executeMeasure();
			return;
		}

		// Init only for first round
		if (round == 0) {
			addNodes(conf.NODES);
			return;
		}

		removeNodes(conf.NODE_REM);
		addNodes(conf.NODE_ADD);
	}

	private void addNodes(int nodes) {
		ActorSelection g = getContext().actorSelection(GRAPH);
		pendingNodes = nodes;
		for (int i = 0; i < nodes; i++) {
			AddNodeMessage msg = new GraphActor.AddNodeMessage();
			g.tell(msg, getSelf());
		}
	}

	private static void processNodeAdded(AddNodeEndedMessage message, ControlActor c) {
		c.pendingNodes--;
		c.nodes.add(message.newNode);
		if (c.pendingNodes == 0)
			c.initNodes();
	}

	private void initNodes() {
		pendingNodes = nodes.size();
		for (ActorRef node : nodes) {
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
			ActorRef bootIdx = nodes.higher(node);

			if (bootIdx == null)
				bootIdx = nodes.first();

			return bootIdx;
		} else {
			// Star topology on first node
			return nodes.first();
		}
	}

	private static void processNodeInitialized(InitNodeEndedMessage message, ControlActor c) {
		c.pendingNodes--;
		if (c.pendingNodes == 0)
			c.executeProtocolRound();
	}

	private static void processRoundEnded(EndRoundMessage status, ControlActor c) {
		System.out.println("Completed round " + c.round);
		c.round++;
		c.prepareSimulationRound();
	}

	private void removeNodes(int nodes) {
		ActorSelection g = getContext().actorSelection(GRAPH);
		for (int i = 0; i < nodes; i++)
			g.tell(new GraphActor.RemoveNodeMessage(), getSelf());
	}

	private static void processNodeRemoved(AddNodeEndedMessage message, ControlActor c) {
		c.pendingNodes--;
		c.nodes.remove(message.newNode);
	}

	private void executeProtocolRound() {
		System.out.println("Starting round " + round + "... ");
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