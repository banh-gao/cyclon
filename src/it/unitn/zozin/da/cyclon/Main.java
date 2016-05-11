package it.unitn.zozin.da.cyclon;

import it.unitn.zozin.da.cyclon.ControlActor.Configuration;
import it.unitn.zozin.da.cyclon.DataProcessor.SimulationDataMessage;
import java.io.FileInputStream;
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

	public static void main(String args[]) throws Exception {

		Configuration config = new Configuration();
		config.load(new FileInputStream("simulation.cfg"));

		ActorSystem s = ActorSystem.create();
		s.actorOf(Props.create(GraphActor.class), "graph");
		ActorRef control = s.actorOf(Props.create(ControlActor.class), "control");

		CompletionStage<Object> res = PatternsCS.ask(control, config, SIM_MAX_TIME);

		// TODO: save simulation results
		// res.thenAccept((r) -> saveResults((SimulationDataMessage) r,
		// config));

		res.thenRun(() -> s.guardian().tell(PoisonPill.getInstance(), null));
	}

	private static void saveResults(SimulationDataMessage data, Configuration config) {
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