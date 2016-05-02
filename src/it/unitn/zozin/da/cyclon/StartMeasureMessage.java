package it.unitn.zozin.da.cyclon;

import it.unitn.zozin.da.cyclon.Message.TaskMessage;
import it.unitn.zozin.da.cyclon.NeighborsCache.Neighbor;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import akka.actor.ActorRef;
import akka.actor.UntypedActor;

public class StartMeasureMessage implements TaskMessage {

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
