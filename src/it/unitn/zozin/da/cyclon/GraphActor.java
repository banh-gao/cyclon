package it.unitn.zozin.da.cyclon;

import it.unitn.zozin.da.cyclon.Message.StatusMessage;
import it.unitn.zozin.da.cyclon.Message.TaskMessage;
import it.unitn.zozin.da.cyclon.StartMeasureMessage.MeasureDataMessage;
import it.unitn.zozin.da.cyclon.StartMeasureMessage.SimulationDataMessage;
import java.util.Map;
import java.util.TreeMap;
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

	private static final BiConsumer<NodeActor.EndRoundMessage, GraphActor> PROCESS_NODE_END_ROUND = (NodeActor.EndRoundMessage message, GraphActor g) -> {
		g.pendingNodes--;
		if (g.pendingNodes == 0)
			g.taskSender.tell(new EndRoundMessage(), g.getSelf());
	};

	private static final BiConsumer<MeasureDataMessage, GraphActor> PROCESS_NODE_MEASURE = (MeasureDataMessage measure, GraphActor g) -> {
		g.aggregatedMeasure.aggregate(measure);
		g.pendingNodes--;
		if (g.pendingNodes == 0) {
			System.out.println(g.aggregatedMeasure.inDegree);
			// Calculate in-degree distribution
			int unreachedNodes = g.aggregatedMeasure.totalNodes;
			Map<Integer, Integer> inDegreeDist = new TreeMap<Integer, Integer>();
			for (int inDegree : g.aggregatedMeasure.inDegree.values()) {
				int v = inDegreeDist.getOrDefault(inDegree, 0);
				inDegreeDist.put(inDegree, v + 1);
				unreachedNodes--;
			}

			inDegreeDist.put(0, unreachedNodes);

			g.taskSender.tell(new SimulationDataMessage(g.aggregatedMeasure.totalNodes, inDegreeDist), g.getSelf());
		}
	};

	static {
		MATCHER.set(StartRoundMessage.class, PROCESS_CONTROL_TASK);
		MATCHER.set(AddNodeMessage.class, PROCESS_CONTROL_TASK);
		MATCHER.set(RemoveNodeMessage.class, PROCESS_CONTROL_TASK);
		MATCHER.set(StartMeasureMessage.class, PROCESS_CONTROL_TASK);

		MATCHER.set(NodeActor.EndRoundMessage.class, PROCESS_NODE_END_ROUND);
		MATCHER.set(MeasureDataMessage.class, PROCESS_NODE_MEASURE);
	}

	// Task processing state
	int pendingNodes = 0;
	private ActorRef taskSender;

	MeasureDataMessage aggregatedMeasure;

	@Override
	public void onReceive(Object message) throws Exception {
		MATCHER.process(message, this);
	}

	public static class AddNodeMessage implements TaskMessage {

		@Override
		public void execute(UntypedActor a) {
			GraphActor g = (GraphActor) a;
			ActorRef newNode = g.getContext().actorOf(Props.create(NodeActor.class));
			g.getSender().tell(new AddNodeEndedMessage(newNode), g.getSelf());
		}
	}

	public static class AddNodeEndedMessage {

		final ActorRef newNode;

		public AddNodeEndedMessage(ActorRef newNode) {
			this.newNode = newNode;
		}

	}

	public static class InitNodeEndedMessage {

	}

	public static class InitNodeMessage {

		final int cacheSize;
		final int shuffleLength;
		final ActorRef bootNeighbor;

		public InitNodeMessage(int cacheSize, int shuffleLength, ActorRef bootNeighbor) {
			this.cacheSize = cacheSize;
			this.shuffleLength = shuffleLength;
			this.bootNeighbor = bootNeighbor;
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

	public static class EndRoundMessage implements StatusMessage {

	}
}