package it.unitn.zozin.da.cyclon;

import it.unitn.zozin.da.cyclon.ReportMessage.Type;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;

public class Graph extends UntypedActor {

	public static final String ADD_NODE = "add";

	private ActorRef taskSender;

	private int pendingNodes = 0;

	@Override
	public void onReceive(Object message) throws Exception {
		if (message instanceof ControlMessage)
			handleControlMessage((ControlMessage) message);
		else if (message instanceof TaskMessage)
			handleTaskMessage((TaskMessage) message);
		else if (message instanceof ReportMessage)
			handleReportMessage((ReportMessage) message);
		else
			throw new IllegalStateException("Unknown message type");
	}

	private void handleControlMessage(ControlMessage message) {
		addNode();
	}

	private void addNode() {
		getContext().actorOf(Props.create(Node.class));
	}

	/**
	 * Task messages are sent to all nodes to be executed in parallel
	 * 
	 * @param message
	 */
	private void handleTaskMessage(TaskMessage message) {
		if (pendingNodes > 0)
			return;

		taskSender = getSender();

		System.out.println(taskSender);

		System.out.println("STARTING GRAPH TASK " + message);

		for (ActorRef c : getContext().getChildren()) {
			pendingNodes++;
			c.tell(message, getSelf());
		}

		taskSender.tell(new ReportMessage(Type.TASK_STARTED, pendingNodes), getSelf());
	}

	/**
	 * Report messages from nodes are forwarded to original task sender
	 * 
	 * @param message
	 */
	private void handleReportMessage(ReportMessage message) {
		pendingNodes--;
		taskSender.forward(message, getContext());

		if (pendingNodes == 0)
			taskSender.tell(new ReportMessage(Type.TASK_ENDED, null), getSelf());
	}
}