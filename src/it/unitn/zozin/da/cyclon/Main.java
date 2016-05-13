package it.unitn.zozin.da.cyclon;

import it.unitn.zozin.da.cyclon.SimulationActor.Configuration;
import it.unitn.zozin.da.cyclon.SimulationActor.SimulationDataMessage;
import java.io.FileInputStream;
import java.io.PrintStream;
import java.util.Map.Entry;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import scala.concurrent.duration.FiniteDuration;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.PoisonPill;
import akka.pattern.PatternsCS;
import akka.util.Timeout;

public class Main {

	static final Timeout SIM_MAX_TIME = Timeout.apply(FiniteDuration.create(5, TimeUnit.MINUTES));
	static final Configuration config = new Configuration();

	public static void main(String args[]) throws Exception {
		config.load(new FileInputStream("simulation.cfg"));

		ActorSystem sys = ActorSystem.create();
		ActorRef simulation = SimulationActor.newActor(sys);

		CompletionStage<Object> res = PatternsCS.ask(simulation, config, SIM_MAX_TIME);
		res.thenAccept((r) -> saveResults((SimulationDataMessage) r, config));

		res.thenRun(() -> sys.guardian().tell(PoisonPill.getInstance(), null));
	}

	private static void saveResults(SimulationDataMessage data, Configuration config) {

		System.out.println("FINAL REPORT: " + data);

		DataLogger.writeData(config, data);

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