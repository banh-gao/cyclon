package it.unitn.zozin.da.cyclon;

import it.unitn.zozin.da.cyclon.Message.StatusMessage;
import it.unitn.zozin.da.cyclon.Message.TaskMessage;
import java.util.function.BiConsumer;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.dispatch.ControlMessage;

public class GraphActor extends UntypedActor {

	private static final MessageMatcher<GraphActor> MATCHER = MessageMatcher.getInstance();

	private static final BiConsumer<ControlMessage, GraphActor> PROCESS_TASK = (ControlMessage message, GraphActor g) -> {
		if (message instanceof TaskMessage) {
			g.taskSender = g.getSender();
			((TaskMessage) message).execute(g);
		} else {
			g.pendingNodes--;
			if (g.pendingNodes == 0)
				g.taskSender.tell(new StatusMessage(), g.getSelf());
		}
	};

	static {
		MATCHER.set(StartRoundMessage.class, PROCESS_TASK);
		MATCHER.set(AddNodeMessage.class, PROCESS_TASK);
		MATCHER.set(RemoveNodeMessage.class, PROCESS_TASK);
		MATCHER.set(StatusMessage.class, PROCESS_TASK);
	}

	// Task processing state
	private int pendingNodes = 0;
	private ActorRef taskSender;

	@Override
	public void onReceive(Object message) throws Exception {
		MATCHER.process(message, this);
	}

	public static class AddNodeMessage implements TaskMessage {

		@Override
		public void execute(UntypedActor a) {
			((GraphActor) a).getContext().actorOf(Props.create(NodeActor.class));
		}
	}

	public static class RemoveNodeMessage implements TaskMessage {

		@Override
		public void execute(UntypedActor a) {
			((GraphActor) a).getContext().children().head().tell(PoisonPill.getInstance(), ((GraphActor) a).getSelf());
		}
	}

	public static class StartRoundMessage implements TaskMessage {

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