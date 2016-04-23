package it.unitn.zozin.da.cyclon;

import it.unitn.zozin.da.cyclon.task.TaskMessage;
import it.unitn.zozin.da.cyclon.task.TaskMessage.ReportMessage;
import java.util.TreeMap;
import akka.actor.ActorRef;
import akka.actor.UntypedActor;

public class NodeActor extends UntypedActor {

	private final TreeMap<Integer, ActorRef> neighbors;

	public NodeActor() {
		this.neighbors = new TreeMap<Integer, ActorRef>();
	}

	@Override
	public void onReceive(Object message) throws Exception {
		if (message instanceof TaskMessage)
			getSender().tell(new ReportMessage(((TaskMessage) message).map(this)), getSelf());
	}
}