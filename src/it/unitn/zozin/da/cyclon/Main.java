package it.unitn.zozin.da.cyclon;

import java.io.FileInputStream;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.PoisonPill;
import akka.pattern.PatternsCS;
import akka.util.Timeout;
import it.unitn.zozin.da.cyclon.SimulationActor.Configuration;
import it.unitn.zozin.da.cyclon.SimulationActor.SimulationDataMessage;
import scala.concurrent.duration.FiniteDuration;

public class Main {

	static final Timeout SIM_MAX_TIME = Timeout.apply(FiniteDuration.create(60, TimeUnit.MINUTES));
	static final Configuration config = new Configuration();

	public static final Logger LOGGER = Logger.getGlobal();

	public static void main(String args[]) throws Exception {
		if (args.length < 1) {
			System.err.println("Expected config file as argument");
			System.exit(1);
		}

		config.load(new FileInputStream(args[0]));

		System.setProperty("java.util.logging.SimpleFormatter.format", "%5$s%6$s");

		if (args.length > 1 && "debug".equalsIgnoreCase(args[1]))
			Main.LOGGER.setLevel(Level.ALL);
		else
			Main.LOGGER.setLevel(Level.OFF);

		ActorSystem sys = ActorSystem.create();
		ActorRef simulation = SimulationActor.newActor(sys);

		CompletionStage<Object> res = PatternsCS.ask(simulation, config, SIM_MAX_TIME);
		res.thenRun(() -> sys.guardian().tell(PoisonPill.getInstance(), null));

		DataLogger l = new DataLogger(config);

		res.thenAcceptAsync((r) -> l.writeData((SimulationDataMessage) r));
	}
}