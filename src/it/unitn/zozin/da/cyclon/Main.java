package it.unitn.zozin.da.cyclon;

import java.io.FileInputStream;
import java.io.PrintWriter;
import java.util.Date;
import java.util.Map.Entry;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.PoisonPill;
import akka.pattern.PatternsCS;
import akka.util.Timeout;
import it.unitn.zozin.da.cyclon.GraphActor.RoundData;
import it.unitn.zozin.da.cyclon.SimulationActor.Configuration;
import it.unitn.zozin.da.cyclon.SimulationActor.SimulationDataMessage;
import scala.concurrent.duration.FiniteDuration;

public class Main {

	static final Timeout SIM_MAX_TIME = Timeout.apply(FiniteDuration.create(60, TimeUnit.MINUTES));
	static final Configuration config = new Configuration();
	static PrintWriter out;

	public static final Logger LOGGER = Logger.getGlobal();

	public static void main(String args[]) throws Exception {
		if (args.length < 1) {
			System.err.println("Expected config file as argument");
			System.exit(1);
		}

		config.load(new FileInputStream(args[0]));

		System.setProperty("java.util.logging.SimpleFormatter.format", "%5$s%6$s");

		out = new PrintWriter("OUTPUT.dat");

		if (!System.getProperty("it.unitn.zozin.da.cyclon.debug", "").isEmpty())
			Main.LOGGER.setLevel(Level.ALL);
		else
			Main.LOGGER.setLevel(Level.OFF);

		ActorSystem sys = ActorSystem.create();
		ActorRef simulation = SimulationActor.newActor(sys);

		CompletionStage<Object> res = PatternsCS.ask(simulation, config, SIM_MAX_TIME);
		res.thenRun(() -> sys.guardian().tell(PoisonPill.getInstance(), null));

		res.thenAcceptAsync((r) -> writeData((SimulationDataMessage) r));
	}

	public static void writeData(SimulationDataMessage data) {
		out.write(String.format("#Simulation completed on %s (nodes=%d rounds=%d cache=%d shuffle=%d topology=%s)\n", new Date().toString(), config.NODES, config.ROUNDS, config.CYCLON_CACHE_SIZE, config.CYCLON_SHUFFLE_LENGTH, config.BOOT_TOPOLOGY));
		for (Entry<Integer, RoundData> e : data.simData.entrySet()) {
			int round = e.getKey();
			RoundData roundData = e.getValue();
			// Write property for current round
			out.write(data.conf.MEASURE.serializeData(roundData.roundValue, round));
		}
		out.close();
	}
}