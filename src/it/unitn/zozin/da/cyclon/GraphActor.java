package it.unitn.zozin.da.cyclon;

import it.unitn.zozin.da.cyclon.GraphActor.ControlMessage.StatusMessage;
import it.unitn.zozin.da.cyclon.task.MeasureMessage;
import it.unitn.zozin.da.cyclon.task.MeasureMessage.ReportMessage;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.actor.UntypedActor;

public class GraphActor extends UntypedActor {

	// Task processing state
	private int pendingNodes = 0;
	private ActorRef taskSender;
	private MeasureMessage task;
	private Object partial;

	@Override
	public void onReceive(Object message) throws Exception {
		if (message instanceof ControlMessage)
			handleControlMessage((ControlMessage) message);
		else if (message instanceof StatusMessage)
			handleStatusMessage((StatusMessage) message);
		else if (message instanceof MeasureMessage)
			handleMeasureMessage((MeasureMessage) message);
		else if (message instanceof ReportMessage)
			handleReportMessage((ReportMessage) message);
		else
			throw new IllegalStateException("Unknown message type " + message.getClass());
	}

	private void handleStatusMessage(StatusMessage message) {
		pendingNodes--;
		if (pendingNodes == 0)
			taskSender.tell(new StatusMessage(), getSelf());
	}

	private void handleControlMessage(ControlMessage message) {
		taskSender = getSender();
		message.execute(this);
	}

	/**
	 * Task messages are sent to all nodes to be executed in parallel
	 * 
	 * @param message
	 */
	private void handleMeasureMessage(MeasureMessage message) {
		if (pendingNodes > 0)
			return;

		partial = null;

		taskSender = getSender();
		task = message;

		System.out.println("STARTING GRAPH TASK " + message);

		for (ActorRef c : getContext().getChildren())
			pendingNodes++;

		for (ActorRef c : getContext().getChildren())
			c.tell(message, getSelf());
	}

	/**
	 * Report messages from nodes are reduced.
	 * When all reports are arrived the final result is sent to the task sender
	 * 
	 * @param message
	 */
	@SuppressWarnings("unchecked")
	private void handleReportMessage(ReportMessage message) {
		pendingNodes--;
		partial = task.reduce(message.value, partial);

		if (pendingNodes == 0)
			taskSender.tell(new ReportMessage(partial), getSelf());
	}

	interface ControlMessage {

		void execute(UntypedActor a);

		class StatusMessage {

		}
	}

	public static class AddNodeMessage implements ControlMessage {

		@Override
		public void execute(UntypedActor a) {
			((GraphActor) a).getContext().actorOf(Props.create(NodeActor.class));
		}

	}

	public static class RemoveNodeMessage implements ControlMessage {

		@Override
		public void execute(UntypedActor a) {
			((GraphActor) a).getContext().children().head().tell(PoisonPill.getInstance(), ((GraphActor) a).getSelf());
		}
	}

	public static class StartRoundMessage implements ControlMessage {

		@Override
		public void execute(UntypedActor a) {
			if (a instanceof GraphActor) {
				GraphActor g = (GraphActor) a;
				for (ActorRef c : g.getContext().getChildren())
					g.pendingNodes++;

				for (ActorRef c : g.getContext().getChildren())
					c.tell(this, g.getSelf());
			} else
				((NodeActor) a).startProtocolRound();
		}
	}
}