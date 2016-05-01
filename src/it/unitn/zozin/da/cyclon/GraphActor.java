package it.unitn.zozin.da.cyclon;

import it.unitn.zozin.da.cyclon.GraphActor.MeasureMessage.MeasureDataMessage;
import it.unitn.zozin.da.cyclon.GraphActor.MeasureMessage.SimulationDataMessage;
import it.unitn.zozin.da.cyclon.Message.StatusMessage;
import it.unitn.zozin.da.cyclon.Message.TaskMessage;
import it.unitn.zozin.da.cyclon.NeighborsCache.Neighbor;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
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
	private int pendingNodes = 0;
	private ActorRef taskSender;

	private MeasureDataMessage aggregatedMeasure;

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

			m.incrementNodeCounter();

			for (Neighbor neighbor : n.cache.getNeighbors())
				m.incrementInDegree(neighbor.address);

			return m;
		}
	}

	public static class MeasureMessage {

		public static class MeasureDataMessage {

			final Map<ActorRef, Integer> inDegree = new HashMap<ActorRef, Integer>();

			int totalNodes = 0;

			public void incrementNodeCounter() {
				totalNodes++;
			}

			public void incrementInDegree(ActorRef node) {
				int v = inDegree.getOrDefault(node, 0);
				inDegree.put(node, v + 1);
			}

			public void aggregate(MeasureDataMessage msg) {
				totalNodes += msg.totalNodes;
				for (Entry<ActorRef, Integer> e : msg.inDegree.entrySet()) {
					int v = inDegree.getOrDefault(e.getKey(), 0);
					inDegree.put(e.getKey(), v + e.getValue());
				}
			}
		}

		public static class SimulationDataMessage {

			private final Map<Integer, Integer> degreeDistr;
			private final int totalNodes;

			public SimulationDataMessage(int totalNodes, Map<Integer, Integer> degreeDistr) {
				this.totalNodes = totalNodes;
				this.degreeDistr = degreeDistr;
			}

			@Override
			public String toString() {
				return "SimulationDataMessage [degreeDistr=" + degreeDistr + ", totalNodes=" + totalNodes + "]";
			}

		}
	}
}