package it.unitn.zozin.da.cyclon;

import akka.actor.UntypedActor;

public abstract class Message {

	public static abstract class ControlMessage implements akka.dispatch.ControlMessage {

		abstract void execute(UntypedActor a);
	}

	public static class StatusMessage implements akka.dispatch.ControlMessage {

	}

	public static abstract class DataMessage extends Message {

	}
}
