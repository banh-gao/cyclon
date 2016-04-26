package it.unitn.zozin.da.cyclon;

import it.unitn.zozin.da.cyclon.task.CyclonRoundTask;
import it.unitn.zozin.da.cyclon.task.MeasuringTask;
import it.unitn.zozin.da.cyclon.task.MeasuringTask.Result;
import it.unitn.zozin.da.cyclon.task.TaskMessage.ReportMessage;
import akka.actor.ActorPath;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.PoisonPill;
import akka.actor.UntypedActor;
import akka.pattern.PatternsCS;

class ControlActor extends UntypedActor {

	private static ActorPath GRAPH;

	private Configuration conf;
	private ActorRef sender;

	private int remainingRounds;
	private Result result;

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
			System.out.println("FINISH");
			getContext().parent().tell(PoisonPill.getInstance(), getSelf());
			sender.tell(result, getSelf());
			return;
		}
		remainingRounds--;
		removeNodes(conf.NODE_REM);
		addNodes(conf.NODE_ADD);
		executeCyclonRound();
		measure();
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

	private void executeCyclonRound() {
		ActorSelection g = getContext().actorSelection(GRAPH);
		PatternsCS.ask(g, new CyclonRoundTask(), 1000).thenRun(this::measure);
	}

	private void measure() {
		ActorSelection g = getContext().actorSelection(GRAPH);
		PatternsCS.ask(g, new MeasuringTask(), 1000).thenAccept(this::aggregateMeasures);
	}

	private void aggregateMeasures(Object msg) {
		Result res = (Result) ((ReportMessage) msg).value;
		System.out.println("Aggregating rounds " + res);
		runSimulationRound();
	}

	@Override
	public void onReceive(Object message) throws Exception {
		if (message instanceof Configuration) {
			conf = (Configuration) message;
			sender = getSender();
			startSimulation();
		} else if (message instanceof ReportMessage) {
			System.out.println("REPORT FROM " + getSender().path().name() + ": " + ((ReportMessage) message).value);
		}
	}

	public static class Configuration {

		// Simulation params
		int NODES = 4;
		int ROUNDS = 1;
		int NODE_ADD = 0;
		int NODE_REM = 0;

		// Cyclon params
		int NEIGHBORS = 2;
	}
}