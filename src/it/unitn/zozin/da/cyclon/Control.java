package it.unitn.zozin.da.cyclon;

import it.unitn.zozin.da.cyclon.TaskMessage.Type;
import akka.actor.ActorSelection;
import akka.actor.UntypedActor;

public class Control extends UntypedActor {

	@Override
	public void preStart() throws Exception {
		ActorSelection g = getContext().actorSelection("../graph");
		for (int i = 0; i < 4; i++)
			g.tell(new ControlMessage(), getSelf());

		g.tell(new TaskMessage(Type.CYCLON_ROUND, (a) -> {
			return "map result";
		}), getSelf());
	}

	@Override
	public void onReceive(Object message) throws Exception {
		if (message instanceof ReportMessage)
			System.out.println("CONTROL REPORT FROM " + getSender().path().name() + ": (" + ((ReportMessage) message).type + ") " + ((ReportMessage) message).value);
	}

}
