package it.unitn.zozin.da.cyclon;

import akka.actor.UntypedActor;
import akka.dispatch.ControlMessage;

public interface Message {

	public interface TaskMessage extends ControlMessage {

		void execute(UntypedActor a);
	}

	public class StatusMessage implements ControlMessage {

	}

	public interface DataMessage {

	}
}