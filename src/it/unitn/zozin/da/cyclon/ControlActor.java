package it.unitn.zozin.da.cyclon;

import it.unitn.zozin.da.cyclon.GraphActor.MeasureMessage.SimulationDataMessage;
import it.unitn.zozin.da.cyclon.GraphActor.StartMeasureMessage;
import it.unitn.zozin.da.cyclon.GraphActor.StartRoundMessage;
import it.unitn.zozin.da.cyclon.Message.StatusMessage;
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

	private static final BiConsumer<StatusMessage, ControlActor> PROCESS_ROUND_END = (StatusMessage status, ControlActor c) -> {
		c.runSimulationRound();

	};

	private static final BiConsumer<SimulationDataMessage, ControlActor> PROCESS_MEASURE_DATA = (SimulationDataMessage data, ControlActor c) -> {
		// TODO: aggregate per round
		c.sender.tell(data, c.getSelf());
		// Start next round

	};

	static {
		MATCHER.set(Configuration.class, PROCESS_CONF);
		MATCHER.set(StatusMessage.class, PROCESS_ROUND_END);
		MATCHER.set(SimulationDataMessage.class, PROCESS_MEASURE_DATA);
	}

	private Configuration conf;
	private ActorRef sender;

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
		runSimulationRound();
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

	private void addNodes(int nodes) {
		ActorSelection g = getContext().actorSelection(GRAPH);
		for (int i = 0; i < nodes; i++)
			g.tell(new GraphActor.AddNodeMessage(conf.CYCLON_CACHE_SIZE, conf.CYCLON_SHUFFLE_LENGTH), getSelf());
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

		int NODES;
		int ROUNDS;
		int NODE_ADD;
		int NODE_REM;
		int CYCLON_CACHE_SIZE;
		int CYCLON_SHUFFLE_LENGTH;
	}

}