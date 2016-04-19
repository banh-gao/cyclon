package it.unitn.zozin.da.cyclon;

import it.unitn.zozin.da.cyclon.ReportMessage.Type;
import java.util.TreeMap;
import akka.actor.ActorRef;
import akka.actor.UntypedActor;

public class Node extends UntypedActor {

	private final TreeMap<Integer, ActorRef> neighbors;

	public static final String START_ROUND = "startNode";
	public static final String END_ROUND = "endNode";

	public Node() {
		this.neighbors = new TreeMap<Integer, ActorRef>();
	}

	@Override
	public void onReceive(Object message) throws Exception {
		if (message instanceof TaskMessage)
			getSender().tell(new ReportMessage(Type.TASK_ENDED, ((TaskMessage) message).map.apply(this)), getSelf());
	}
}