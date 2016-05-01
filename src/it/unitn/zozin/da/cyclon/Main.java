package it.unitn.zozin.da.cyclon;

import it.unitn.zozin.da.cyclon.ControlActor.Configuration;
import it.unitn.zozin.da.cyclon.ControlActor.Configuration.Topology;
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
		config.BOOT_TOPOLOGY = Topology.CHAIN;
		config.NODES = 1000;
		config.ROUNDS = 1000;
		config.NODE_ADD = 0;
		config.NODE_REM = 0;
		config.CYCLON_CACHE_SIZE = 20;
		config.CYCLON_SHUFFLE_LENGTH = 15;

		PatternsCS.ask(control, config, 1000000).thenAccept((report) -> {
			System.out.println("FINAL REPORT: " + report);
			s.guardian().tell(PoisonPill.getInstance(), null);
		});
	}
}