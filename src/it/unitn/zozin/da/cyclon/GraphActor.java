package it.unitn.zozin.da.cyclon;

import it.unitn.zozin.da.cyclon.NodeActor.EndJoinMessage;
import it.unitn.zozin.da.cyclon.NodeActor.EndRoundMessage;
import it.unitn.zozin.da.cyclon.NodeActor.MeasureDataMessage;
import it.unitn.zozin.da.cyclon.NodeActor.StartJoinMessage;
import it.unitn.zozin.da.cyclon.NodeActor.StartRoundMessage;
import java.util.HashMap;
import java.util.Map;
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

	private enum Uninitialized implements StateData {
		Uninitialized
	}

	class NodesCount implements StateData {

		private final int totalNodes;
		private int count;

		public NodesCount(int totalNodes) {
			super();
			this.totalNodes = totalNodes;
		}

		public void increaseOne() {
			count += 1;
		}

		public boolean isCompleted() {
			return count == totalNodes;
		}
	}

	{
		startWith(State.Idle, Uninitialized.Uninitialized);

		when(State.Idle, matchEvent(AddNodeMessage.class, (addNodeMsg, data) -> processAddNode()));

		when(State.Idle, matchEvent(StartJoinMessage.class, (startJoinMsg, data) -> startJoin(startJoinMsg)));
		when(State.JoinRunning, matchEvent(EndJoinMessage.class, NodesCount.class, (endJoinMsg, nodesCount) -> processEndJoin(endJoinMsg, nodesCount)));

		when(State.Idle, matchEvent(StartRoundMessage.class, (startRoundMsg, data) -> startRound(startRoundMsg)));
		when(State.RoundRunning, matchEvent(NodeActor.EndRoundMessage.class, NodesCount.class, (endRoundMsg, nodesCount) -> processEndRound(nodesCount)));

		when(State.Idle, matchEvent(StartMeasureMessage.class, (startMeasureMsg, data) -> startMeasure()));
		when(State.MeasureRunning, matchEvent(NodeActor.MeasureDataMessage.class, NodesCount.class, (measureDataMsg, nodesCount) -> processNodeMeasure(measureDataMsg, nodesCount)));
	}

	// Task processing state
	private ActorRef taskSender;
	private int nodeNum = 0;

	boolean[][] adjacencyMatrix;

	private akka.actor.FSM.State<State, StateData> processAddNode() {
		ActorRef newNode = context().actorOf(Props.create(NodeActor.class), "" + nodeNum++);
		sender().tell(new AddNodeEndedMessage(newNode), self());
		return stay();
	}

	private akka.actor.FSM.State<State, StateData> startJoin(StartJoinMessage startJoinMsg) {
		int pendingNodes = 0;
		taskSender = sender();
		for (ActorRef c : JavaConversions.asJavaIterable(context().children())) {
			pendingNodes++;
			c.tell(startJoinMsg, self());
		}
		return goTo(State.JoinRunning).using(new NodesCount(pendingNodes));
	}

	private akka.actor.FSM.State<State, StateData> processEndJoin(EndJoinMessage endJoinMsg, NodesCount nodesCount) {
		nodesCount.increaseOne();
		if (nodesCount.isCompleted()) {
			taskSender.tell(endJoinMsg, self());
			return goTo(State.Idle).using(new NodesCount(nodesCount.totalNodes));
		}
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
			return goTo(State.Idle).using(Uninitialized.Uninitialized);
		}
		return stay();
	}

	private akka.actor.FSM.State<State, StateData> startMeasure() {
		int pendingNodes = 0;
		taskSender = sender();
		for (ActorRef c : JavaConversions.asJavaIterable(context().children())) {
			pendingNodes++;
			c.tell(new NodeActor.StartMeasureMessage(), self());
		}

		adjacencyMatrix = new boolean[pendingNodes][pendingNodes];
		return goTo(State.MeasureRunning).using(new NodesCount(pendingNodes));
	}

	private akka.actor.FSM.State<State, StateData> processNodeMeasure(MeasureDataMessage measure, NodesCount count) {
		count.increaseOne();
		int node = toIndex(sender());
		for (ActorRef a : measure.neighbors) {
			int neighbor = toIndex(a);
			adjacencyMatrix[node][neighbor] = true;
		}

		if (count.isCompleted()) {
			Map<Integer, Integer> inDegreeDist = calcInDegree(count.totalNodes);
			int clusteringCoeff = calcClusteringCoeff(count.totalNodes);

			taskSender.tell(new SimulationDataMessage(count.totalNodes, inDegreeDist, clusteringCoeff), self());
			return goTo(State.Idle).using(Uninitialized.Uninitialized);
		}
		return stay();
	}

	private int toIndex(ActorRef sender) {
		return Integer.parseInt(sender.path().name());
	}

	Map<Integer, Integer> calcInDegree(int totalNodes) {
		Map<Integer, Integer> inDegreeDistr = new HashMap<Integer, Integer>();

		for (int node = 0; node < totalNodes; node++) {
			int inDegree = 0;
			for (int neighbor = 0; neighbor < totalNodes; neighbor++) {
				if (adjacencyMatrix[neighbor][node])
					inDegree += 1;
			}
			int count = inDegreeDistr.getOrDefault(inDegree, 0);
			inDegreeDistr.put(inDegree, count + 1);
		}

		return inDegreeDistr;
	}

	int calcClusteringCoeff(int totalNodes) {
		// TODO: calculate clustering coeff

		return 0;
	}

	public static class AddNodeMessage {
	}

	public static class AddNodeEndedMessage {

		final ActorRef newNode;

		public AddNodeEndedMessage(ActorRef newNode) {
			this.newNode = newNode;
		}

	}

	public static class StartMeasureMessage {
	}

	public static class SimulationDataMessage {

		final Map<Integer, Integer> inDegreeDistr;
		final int clusteringCoeff;
		final int totalNodes;

		public SimulationDataMessage(int totalNodes, Map<Integer, Integer> degreeDistr, int clusteringCoeff) {
			this.totalNodes = totalNodes;
			this.inDegreeDistr = degreeDistr;
			this.clusteringCoeff = clusteringCoeff;
		}

		@Override
		public String toString() {
			return "SimulationDataMessage [inDegreeDistr=" + inDegreeDistr + ", totalNodes=" + totalNodes + "]";
		}

	}
}