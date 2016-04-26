package it.unitn.zozin.da.cyclon;

import it.unitn.zozin.da.cyclon.task.TaskMessage;
import it.unitn.zozin.da.cyclon.task.TaskMessage.ReportMessage;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.actor.UntypedActor;

public class GraphActor extends UntypedActor {

	// Cyclon parameter
	public static final int CACHE_SIZE = 3;
	public static final int SHUFFLE_LENGTH = 2;

	// Task processing state
	private int pendingNodes = 0;
	private ActorRef taskSender;
	private TaskMessage task;
	private Object partial;

	@Override
	public void onReceive(Object message) throws Exception {
		if (message instanceof ControlMessage)
			handleControlMessage((ControlMessage) message);
		else if (message instanceof TaskMessage)
			handleTaskMessage((TaskMessage) message);
		else if (message instanceof ReportMessage)
			handleReportMessage((ReportMessage) message);
		else
			throw new IllegalStateException("Unknown message type " + message.getClass());
	}

	private void handleControlMessage(ControlMessage message) {
		message.execute(this);
	}

	/**
	 * Task messages are sent to all nodes to be executed in parallel
	 * 
	 * @param message
	 */
	private void handleTaskMessage(TaskMessage message) {
		if (pendingNodes > 0)
			return;

		partial = null;

		taskSender = getSender();
		task = message;

		System.out.println("STARTING GRAPH TASK " + message);

		for (ActorRef c : getContext().getChildren()) {
			pendingNodes++;
			c.tell(message, getSelf());
		}
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

		void execute(GraphActor g);
	}

	public static class AddNodeMessage implements ControlMessage {

		@Override
		public void execute(GraphActor g) {
			g.getContext().actorOf(Props.create(NodeActor.class, CACHE_SIZE, SHUFFLE_LENGTH));
		}

	}

	public static class RemoveNodeMessage implements ControlMessage {

		@Override
		public void execute(GraphActor g) {
			g.getContext().children().head().tell(PoisonPill.getInstance(), g.getSelf());
		}
	}
}