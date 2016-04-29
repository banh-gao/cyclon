package it.unitn.zozin.da.cyclon;

import it.unitn.zozin.da.cyclon.GraphActor.MeasureMessage.MeasureDataMessage;
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
		c.executeMeasure();
	};

	private static final BiConsumer<MeasureDataMessage, ControlActor> PROCESS_MEASURE_DATA = (MeasureDataMessage data, ControlActor c) -> {
		// TODO: aggregate per round
		System.out.println(data.degreeDistr);
		// Start next round
		c.runSimulationRound();
	};

	static {
		MATCHER.set(Configuration.class, PROCESS_CONF);
		MATCHER.set(StatusMessage.class, PROCESS_ROUND_END);
		MATCHER.set(MeasureDataMessage.class, PROCESS_MEASURE_DATA);
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
			sender.tell(new StatusMessage(), getSelf());
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
			g.tell(new GraphActor.AddNodeMessage(), getSelf());
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

	public static class Configuration {

		// Simulation params
		int NODES = 10;
		int ROUNDS = 5;
		int NODE_ADD = 0;
		int NODE_REM = 0;
	}

}