package it.unitn.zozin.da.cyclon;

import it.unitn.zozin.da.cyclon.Message.ControlMessage;
import it.unitn.zozin.da.cyclon.Message.StatusMessage;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.actor.UntypedActor;

public class GraphActor extends UntypedActor {

	// Task processing state
	private int pendingNodes = 0;
	private ActorRef taskSender;

	@Override
	public void onReceive(Object message) throws Exception {
		if (message instanceof ControlMessage)
			handleControlMessage((ControlMessage) message);
		else if (message instanceof StatusMessage)
			handleStatusMessage((StatusMessage) message);
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

	public static class AddNodeMessage extends ControlMessage {

		@Override
		public void execute(UntypedActor a) {
			((GraphActor) a).getContext().actorOf(Props.create(NodeActor.class));
		}
	}

	public static class RemoveNodeMessage extends ControlMessage {

		@Override
		public void execute(UntypedActor a) {
			((GraphActor) a).getContext().children().head().tell(PoisonPill.getInstance(), ((GraphActor) a).getSelf());
		}
	}

	public static class StartRoundMessage extends ControlMessage {

		@Override
		public void execute(UntypedActor a) {
			if (a instanceof GraphActor) {
				GraphActor g = (GraphActor) a;
				for (ActorRef c : g.getContext().getChildren()) {
					g.pendingNodes++;
					c.tell(this, g.getSelf());
				}
			} else
				((NodeActor) a).startProtocolRound();
		}
	}
}