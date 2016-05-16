package it.unitn.zozin.da.cyclon;

import it.unitn.zozin.da.cyclon.DataProcessor.GraphProperty;
import it.unitn.zozin.da.cyclon.DataProcessor.RoundData;
import it.unitn.zozin.da.cyclon.NodeActor.EndRoundMessage;
import it.unitn.zozin.da.cyclon.NodeActor.MeasureDataMessage;
import it.unitn.zozin.da.cyclon.NodeActor.StartRoundMessage;
import java.util.Set;
import scala.collection.JavaConversions;
import akka.actor.AbstractFSM;
import akka.actor.ActorRef;
import akka.actor.Props;

public class GraphActor extends AbstractFSM<GraphActor.State, GraphActor.StateData> {

	enum State {
		Idle, JoinRunning, RoundRunning, MeasureRunning
	}

	interface StateData {

	}

	class NodesCount implements StateData {

		private final int totalNodes;
		private int count;

		public NodesCount(int totalNodes) {
			this.totalNodes = totalNodes;
		}

		public void increaseOne() {
			count += 1;
		}

		public boolean isCompleted() {
			return count == totalNodes;
		}
	}

	class MeasureStateData implements StateData {

		private final int totalNodes;
		private int count;
		final Set<GraphProperty> params;

		public MeasureStateData(int totalNodes, Set<GraphProperty> params) {
			this.totalNodes = totalNodes;
			this.params = params;
		}

		public void increaseOne() {
			count += 1;
		}

		public boolean isCompleted() {
			return count == totalNodes;
		}
	}

	{
		startWith(State.Idle, null);

		when(State.Idle, matchEvent(AddNodeMessage.class, (addNodeMsg, data) -> processAddNode()));

		when(State.Idle, matchEvent(StartRoundMessage.class, (startRoundMsg, data) -> startRound(startRoundMsg)));
		when(State.RoundRunning, matchEvent(NodeActor.EndRoundMessage.class, NodesCount.class, (endRoundMsg, nodesCount) -> processEndRound(nodesCount)));

		when(State.Idle, matchEvent(StartMeasureMessage.class, (startMeasureMsg, data) -> startMeasure(startMeasureMsg)));
		when(State.MeasureRunning, matchEvent(NodeActor.MeasureDataMessage.class, MeasureStateData.class, (measureDataMsg, nodesCount) -> processNodeMeasure(measureDataMsg, nodesCount)));
	}

	// Task processing state
	private ActorRef taskSender;

	// Used to assign node ids
	private int nextNodeId = 0;

	DataProcessor dataProcessor = new DataProcessor();
	boolean[][] adjacencyMatrix;

	private akka.actor.FSM.State<State, StateData> processAddNode() {
		int nodeIndex = nextNodeId++;
		ActorRef newNode = context().actorOf(Props.create(NodeActor.class), "" + nodeIndex);
		sender().tell(new AddNodeEndedMessage(newNode, nodeIndex), self());
		return stay();
	}

	private akka.actor.FSM.State<State, StateData> startRound(StartRoundMessage startRoundMsg) {
		int pendingNodes = 0;
		taskSender = sender();
		for (ActorRef c : JavaConversions.asJavaIterable(context().children())) {
			pendingNodes++;
			c.tell(startRoundMsg, self());
		}
		return goTo(State.RoundRunning).using(new NodesCount(pendingNodes));
	}

	private akka.actor.FSM.State<State, StateData> processEndRound(NodesCount nodesCount) {
		nodesCount.increaseOne();
		if (nodesCount.isCompleted()) {
			taskSender.tell(new EndRoundMessage(), self());
			return goTo(State.Idle);
		}
		return stay();
	}

	private akka.actor.FSM.State<State, StateData> startMeasure(StartMeasureMessage msg) {
		int pendingNodes = 0;
		taskSender = sender();
		for (ActorRef c : JavaConversions.asJavaIterable(context().children())) {
			pendingNodes++;
			c.tell(new NodeActor.StartMeasureMessage(), self());
		}

		adjacencyMatrix = new boolean[pendingNodes][pendingNodes];
		return goTo(State.MeasureRunning).using(new MeasureStateData(pendingNodes, msg.params));
	}

	private akka.actor.FSM.State<State, StateData> processNodeMeasure(MeasureDataMessage measure, MeasureStateData measureStateData) {
		measureStateData.increaseOne();
		int node = toIndex(sender());
		for (ActorRef a : measure.neighbors) {
			int neighbor = toIndex(a);
			adjacencyMatrix[node][neighbor] = true;
		}

		if (measureStateData.isCompleted()) {
			RoundData processedRound = dataProcessor.processRoundSample(measureStateData.params, adjacencyMatrix);
			taskSender.tell(processedRound, self());
			return goTo(State.Idle);
		}
		return stay();
	}

	private int toIndex(ActorRef sender) {
		return Integer.parseInt(sender.path().name());
	}

	public static class AddNodeMessage {
	}

	public static class AddNodeEndedMessage {

		final ActorRef newNode;
		final int nodeIndex;

		public AddNodeEndedMessage(ActorRef newNode, int nodeIndex) {
			this.newNode = newNode;
			this.nodeIndex = nodeIndex;
		}

	}

	public static class StartMeasureMessage {

		final Set<GraphProperty> params;

		public StartMeasureMessage(Set<GraphProperty> params) {
			super();
			this.params = params;
		}
	}
}