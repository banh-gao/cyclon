package it.unitn.zozin.da.cyclon;

import it.unitn.zozin.da.cyclon.ControlActor.Configuration;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.pattern.PatternsCS;

public class Main {

	public static void main(String args[]) {
		ActorSystem s = ActorSystem.create();
		s.actorOf(Props.create(GraphActor.class), "graph");
		ActorRef control = s.actorOf(Props.create(ControlActor.class), "control");

		Configuration config = new Configuration();
		config.NODES = 1000;
		config.ROUNDS = 10;
		config.NODE_ADD = 0;
		config.NODE_REM = 0;
		config.CYCLON_CACHE_SIZE = 8;
		config.CYCLON_SHUFFLE_LENGTH = 1;

		PatternsCS.ask(control, config, 10000).thenAccept((report) -> {
			System.out.println("FINAL REPORT: " + report);
			s.guardian().tell(PoisonPill.getInstance(), null);
		});
	}
}