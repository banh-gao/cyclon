package it.unitn.zozin.da.cyclon;

import akka.actor.UntypedActor;

public interface Message {

	public interface TaskMessage {

		void execute(UntypedActor a);
	}

	public interface StatusMessage {

	}

	public interface DataMessage {

	}
}