package it.unitn.zozin.da.cyclon;

import it.unitn.zozin.da.cyclon.GraphActor.ControlMessage.StatusMessage;
import it.unitn.zozin.da.cyclon.GraphActor.StartRoundMessage;
import akka.actor.ActorPath;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.UntypedActor;

class ControlActor extends UntypedActor {

	private static ActorPath GRAPH;

	private Configuration conf;
	private ActorRef sender;

	private int remainingRounds;

	@Override
	public void preStart() throws Exception {
		ActorPath userRoot = getSelf().path().parent();
		GRAPH = userRoot.child("graph");
	}

	private void startSimulation() {
		addNodes(conf.NODES);
		remainingRounds = conf.ROUNDS;

		runSimulationRound();
	}

	private void runSimulationRound() {
		if (remainingRounds == 0) {
			// TODO: terminate simulation
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

		// TODO:init new nodes
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

	private void measure() {
		// TODO
		if (true) {
			runSimulationRound();
			return;
		}
		ActorSelection g = getContext().actorSelection(GRAPH);
		// PatternsCS.ask(g, new MeasuringTask(),
		// 1000).thenAccept(this::aggregateMeasures);
	}

	private void aggregateMeasures(Object msg) {
		// Result res = (Result) ((ReportMessage) msg).value;
		// System.out.println("Aggregating rounds " + res);
		runSimulationRound();
	}

	@Override
	public void onReceive(Object message) throws Exception {
		if (message instanceof Configuration) {
			conf = (Configuration) message;
			sender = getSender();
			startSimulation();
		} else if (message instanceof StatusMessage) {
			measure();
		}
	}

	public static class Configuration {

		// Simulation params
		int NODES = 10;
		int ROUNDS = 4;
		int NODE_ADD = 0;
		int NODE_REM = 0;
	}
}