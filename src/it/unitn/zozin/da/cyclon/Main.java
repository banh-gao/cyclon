package it.unitn.zozin.da.cyclon;

import it.unitn.zozin.da.cyclon.ControlActor.Configuration;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.pattern.PatternsCS;

public class Main {

	public static void main(String args[]) {
		ActorSystem s = ActorSystem.create();
		s.actorOf(Props.create(GraphActor.class), "graph");
		ActorRef control = s.actorOf(Props.create(ControlActor.class), "control");

		Configuration config = new Configuration();

		PatternsCS.ask(control, config, 10000).thenAccept((report) -> {
			System.out.println("Report " + report);
		});
	}
}