package it.unitn.zozin.da.cyclon;

import it.unitn.zozin.da.cyclon.NodeActor.MeasureDataMessage;
import java.util.Map;
import java.util.TreeMap;
import scala.collection.JavaConversions;
import akka.actor.AbstractFSM;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;

public class GraphActor extends AbstractFSM<GraphActor.State, GraphActor.StateData> {

	enum State {
		Idle, RoundRunning, MeasureRunning
	}

	interface StateData {

	}

	private enum Uninitialized implements StateData {
		Uninitialized
	}

	class NodesCount implements StateData {

		int nodes;

		public NodesCount(int nodes) {
			super();
			this.nodes = nodes;
		}

	}

	{
		startWith(State.Idle, Uninitialized.Uninitialized);

		when(State.Idle, matchEvent(RemoveNodeMessage.class, (removeNodeMsg, data) -> processRemoveNode()));
		when(State.Idle, matchEvent(AddNodeMessage.class, (addNodeMsg, data) -> processAddNode()));

		when(State.Idle, matchEvent(StartRoundMessage.class, (startRoundMsg, data) -> startRound()));
		when(State.RoundRunning, matchEvent(NodeActor.EndRoundMessage.class, NodesCount.class, (endRoundMsg, nodesCount) -> processEndRound(nodesCount)));

		when(State.Idle, matchEvent(StartMeasureMessage.class, (startMeasureMsg, data) -> startMeasure()));
		when(State.MeasureRunning, matchEvent(NodeActor.MeasureDataMessage.class, NodesCount.class, (measureDataMsg, nodesCount) -> processNodeMeasure(measureDataMsg, nodesCount)));
	}

	// Task processing state
	private ActorRef taskSender;

	MeasureDataMessage aggregatedMeasure;

	private akka.actor.FSM.State<State, StateData> processRemoveNode() {
		context().children().head().tell(PoisonPill.getInstance(), self());
		return stay();
	}

	private akka.actor.FSM.State<State, StateData> processAddNode() {
		ActorRef newNode = context().actorOf(Props.create(NodeActor.class));
		sender().tell(new AddNodeEndedMessage(newNode), self());
		return stay();
	}

	private akka.actor.FSM.State<State, StateData> startRound() {
		int pendingNodes = 0;
		taskSender = sender();
		for (ActorRef c : JavaConversions.asJavaIterable(context().children())) {
			pendingNodes++;
			c.tell(new NodeActor.StartRoundMessage(), self());
		}
		return goTo(State.RoundRunning).using(new NodesCount(pendingNodes));
	}

	private akka.actor.FSM.State<State, StateData> processEndRound(NodesCount nodesCount) {
		nodesCount.nodes--;
		if (nodesCount.nodes == 0) {
			taskSender.tell(new EndRoundMessage(), self());
			return goTo(State.Idle).using(Uninitialized.Uninitialized);
		}
		return stay();
	}

	private akka.actor.FSM.State<State, StateData> startMeasure() {
		aggregatedMeasure = new MeasureDataMessage();
		int pendingNodes = 0;
		for (ActorRef c : JavaConversions.asJavaIterable(context().children())) {
			pendingNodes++;
			c.tell(new NodeActor.StartMeasureMessage(), self());
		}
		return goTo(State.MeasureRunning).using(new NodesCount(pendingNodes));
	}

	private akka.actor.FSM.State<State, StateData> processNodeMeasure(MeasureDataMessage measure, NodesCount count) {
		aggregatedMeasure.aggregate(measure);
		count.nodes--;

		if (count.nodes == 0) {
			// Calculate in-degree distribution
			int unreachedNodes = aggregatedMeasure.totalNodes;
			Map<Integer, Integer> inDegreeDist = new TreeMap<Integer, Integer>();
			for (int inDegree : aggregatedMeasure.inDegree.values()) {
				int v = inDegreeDist.getOrDefault(inDegree, 0);
				inDegreeDist.put(inDegree, v + 1);
				unreachedNodes--;
			}

			inDegreeDist.put(0, unreachedNodes);

			taskSender.tell(new SimulationDataMessage(aggregatedMeasure.totalNodes, inDegreeDist), self());
			return goTo(State.Idle).using(Uninitialized.Uninitialized);
		}
		return stay();
	}

	public static class AddNodeMessage {
	}

	public static class AddNodeEndedMessage {

		final ActorRef newNode;

		public AddNodeEndedMessage(ActorRef newNode) {
			this.newNode = newNode;
		}

	}

	public static class InitNodeEndedMessage {

	}

	public static class RemoveNodeMessage {

	}

	public static class RemoveNodeEndedMessage {

		final ActorRef removedNode;

		public RemoveNodeEndedMessage(ActorRef removedNode) {
			this.removedNode = removedNode;
		}

	}

	public static class StartRoundMessage {

	}

	public static class EndRoundMessage {

	}

	public static class StartMeasureMessage {
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