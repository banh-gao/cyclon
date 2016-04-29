package it.unitn.zozin.da.cyclon;

import it.unitn.zozin.da.cyclon.GraphActor.MeasureMessage.MeasureDataMessage;
import it.unitn.zozin.da.cyclon.Message.StatusMessage;
import it.unitn.zozin.da.cyclon.Message.TaskMessage;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.BiConsumer;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.actor.UntypedActor;

public class GraphActor extends UntypedActor {

	private static final MessageMatcher<GraphActor> MATCHER = MessageMatcher.getInstance();

	private static final BiConsumer<TaskMessage, GraphActor> PROCESS_CONTROL_TASK = (TaskMessage message, GraphActor g) -> {
		g.taskSender = g.getSender();
		((TaskMessage) message).execute(g);
	};

	private static final BiConsumer<StatusMessage, GraphActor> PROCESS_NODE_STATUS = (StatusMessage message, GraphActor g) -> {
		g.pendingNodes--;
		if (g.pendingNodes == 0)
			g.taskSender.tell(new StatusMessage(), g.getSelf());
	};

	private static final BiConsumer<MeasureDataMessage, GraphActor> PROCESS_NODE_MEASURE = (MeasureDataMessage measure, GraphActor g) -> {
		g.aggregateNodesMeasure(measure);
		g.pendingNodes--;
		if (g.pendingNodes == 0) {
			g.taskSender.tell(g.aggregatedMeasure, g.getSelf());
		}
	};

	static {
		MATCHER.set(StartRoundMessage.class, PROCESS_CONTROL_TASK);
		MATCHER.set(AddNodeMessage.class, PROCESS_CONTROL_TASK);
		MATCHER.set(RemoveNodeMessage.class, PROCESS_CONTROL_TASK);
		MATCHER.set(StartMeasureMessage.class, PROCESS_CONTROL_TASK);

		MATCHER.set(StatusMessage.class, PROCESS_NODE_STATUS);
		MATCHER.set(MeasureDataMessage.class, PROCESS_NODE_MEASURE);
	}

	// Task processing state
	private int pendingNodes = 0;
	private ActorRef taskSender;

	private MeasureDataMessage aggregatedMeasure;

	@Override
	public void onReceive(Object message) throws Exception {
		MATCHER.process(message, this);
	}

	private void aggregateNodesMeasure(MeasureDataMessage measure) {
		// TODO Aggregate measure from a node with other nodes
		aggregatedMeasure.aggregate(measure);
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

	public static class StartMeasureMessage implements TaskMessage {

		@Override
		public void execute(UntypedActor a) {
			if (a instanceof GraphActor) {
				GraphActor g = (GraphActor) a;
				g.aggregatedMeasure = new MeasureDataMessage();
				for (ActorRef c : g.getContext().getChildren()) {
					g.pendingNodes++;
					c.tell(this, g.getSelf());
				}
			} else
				a.getSender().tell(measureNode((NodeActor) a), a.getSelf());
		}

		private MeasureDataMessage measureNode(NodeActor n) {
			MeasureDataMessage m = new MeasureDataMessage();
			m.incrementDegree(n.cache.size());
			return m;
		}
	}

	public static class MeasureMessage {

		public static class MeasureDataMessage {

			final Map<Integer, Integer> degreeDistr = new HashMap<Integer, Integer>();

			public void incrementDegree(int degree) {
				int nodesNum = degreeDistr.getOrDefault(degreeDistr, 0);
				degreeDistr.put(degree, nodesNum + 1);
			}

			public void aggregate(MeasureDataMessage msg) {
				for (Entry<Integer, Integer> e : msg.degreeDistr.entrySet()) {
					int nodesNum = degreeDistr.getOrDefault(e.getKey(), 0);
					degreeDistr.put(e.getKey(), nodesNum + e.getValue());
				}
			}
		}
	}
}