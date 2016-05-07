package it.unitn.zozin.da.cyclon;

import it.unitn.zozin.da.cyclon.ControlActor.Configuration;
import it.unitn.zozin.da.cyclon.ControlActor.Configuration.Topology;
import it.unitn.zozin.da.cyclon.GraphActor.SimulationDataMessage;
import java.io.PrintStream;
import java.util.Map.Entry;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import scala.concurrent.duration.FiniteDuration;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.pattern.PatternsCS;
import akka.util.Timeout;

public class Main {

	static Timeout SIM_MAX_TIME = Timeout.apply(FiniteDuration.create(5, TimeUnit.MINUTES));

	static Configuration config = new Configuration();

	public static void main(String args[]) throws Exception {
		ActorSystem s = ActorSystem.create();
		s.actorOf(Props.create(GraphActor.class), "graph");
		ActorRef control = s.actorOf(Props.create(ControlActor.class), "control");

		config.NODES = 1000;
		config.ROUNDS = 100;

		config.CYCLON_CACHE_SIZE = 20;
		config.CYCLON_SHUFFLE_LENGTH = 8;

		config.BOOT_TOPOLOGY = Topology.CHAIN;
		config.PER_ROUND_MEASURE = false;

		CompletionStage<Object> res = PatternsCS.ask(control, config, SIM_MAX_TIME);
		res.thenAccept((r) -> saveResults((SimulationDataMessage) r));
		res.thenRun(() -> s.guardian().tell(PoisonPill.getInstance(), null));
	}

	private static void saveResults(SimulationDataMessage data) {
		System.out.println("FINAL REPORT: " + data);

		try {

			PrintStream f = new PrintStream("degreeDistr" + config.CYCLON_CACHE_SIZE + ".csv");
			f.println("\"in-degree\",\"nodes\"");
			for (Entry<Integer, Integer> d : data.inDegreeDistr.entrySet()) {
				int inDegree = d.getKey();
				int nodes = d.getValue();
				f.println(inDegree + "," + nodes);
			}
			f.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}