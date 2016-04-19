package it.unitn.zozin.da.cyclon;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;

public class Main {

	public static void main(String[] args) {
		ActorSystem s = ActorSystem.create("sys");
		s.actorOf(Props.create(Graph.class), "graph");
		ActorRef c = s.actorOf(Props.create(Control.class), "control");
	}
}
