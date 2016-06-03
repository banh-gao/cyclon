package it.unitn.zozin.da.cyclon;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import akka.actor.AbstractFSM;
import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.zozin.da.cyclon.NodeActor.EndMeasureMessage;
import it.unitn.zozin.da.cyclon.NodeActor.EndRound;
import it.unitn.zozin.da.cyclon.NodeActor.NodeCalcResult;
import it.unitn.zozin.da.cyclon.NodeActor.NodeCalcTask;
import it.unitn.zozin.da.cyclon.NodeActor.StartRound;
import scala.collection.JavaConversions;

/**
 * Actor controlling all the node actors and aggregating measurement data
 */
public class GraphActor extends AbstractFSM<GraphActor.State, GraphActor.StateData> {

	enum State {
		Idle, BootRunning, RoundRunning, MeasureRunning, CalcRunning
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
		final GraphProperty param;

		public MeasureStateData(int totalNodes, GraphProperty param) {
			this.totalNodes = totalNodes;
			this.param = param;
		}

		public void increaseOne() {
			count += 1;
		}

		public boolean isCompleted() {
			return count == totalNodes;
		}
	}

	class CalcStateData implements StateData {

		final public int total;
		final public GraphProperty prop;

		@SuppressWarnings("rawtypes")
		public final List values;

		public int current = 0;

		public CalcStateData(int total, GraphProperty param) {
			this.total = total;
			this.prop = param;
			this.values = new ArrayList<Object>(total);
		}

		@SuppressWarnings("unchecked")
		public void aggregateData(Object val) {
			this.values.add(val);
		}

		public boolean isCompleted() {
			return current == total;
		}

		public void increaseOne() {
			current++;
		}

		@SuppressWarnings("unchecked")
		public Object computeFinalData() {
			return values.stream().collect(prop.collector());
		}

	}

	{
		startWith(State.Idle, null);

		when(State.Idle, matchEvent(StartAddNodesMessage.class, (addNodesMsg, data) -> processAddNodes(addNodesMsg)));
		when(State.Idle, matchEvent(StartBootMessage.class, (bootNodesMsg, data) -> startBoot(bootNodesMsg)));

		when(State.BootRunning, matchEvent(EndBootMessage.class, NodesCount.class, (endBootMsg, nodesCount) -> processNodeBooted(nodesCount)));

		when(State.Idle, matchEvent(StartRound.class, (startRoundMsg, data) -> startRound(startRoundMsg)));
		when(State.RoundRunning, matchEvent(NodeActor.EndRound.class, NodesCount.class, (endRoundMsg, nodesCount) -> processEndRound(nodesCount)));

		when(State.Idle, matchEvent(StartMeasureMessage.class, (startMeasureMsg, data) -> startMeasure(startMeasureMsg)));
		when(State.MeasureRunning, matchEvent(NodeActor.EndMeasureMessage.class, MeasureStateData.class, (measureDataMsg, nodesCount) -> processNodeMeasure(measureDataMsg, nodesCount)));

		when(State.CalcRunning, matchEvent(NodeActor.NodeCalcResult.class, CalcStateData.class, (calcDataMsg, calcState) -> processCalcResult(calcDataMsg, calcState)));
	}

	// Task processing state
	private ActorRef taskSender;

	boolean[][] adjacencyMatrix;

	private akka.actor.FSM.State<State, StateData> processAddNodes(StartAddNodesMessage addMsg) {
		Set<ActorRef> addedNodes = new HashSet<ActorRef>();

		for (int i = 0; i < addMsg.requiredNodes; i++) {
			ActorRef node = context().actorOf(Props.create(NodeActor.class, addMsg.cacheSize, addMsg.shuffleLength), Integer.toString(i));
			addedNodes.add(node);
		}

		sender().tell(new EndAddNodesMessage(addedNodes), self());
		return stay();
	}

	private akka.actor.FSM.State<State, StateData> startBoot(StartBootMessage bootNodesMsg) {
		taskSender = sender();

		for (ActorRef n : bootNodesMsg.introducers.keySet()) {
			n.tell(bootNodesMsg, self());
		}

		return goTo(State.BootRunning).using(new NodesCount(bootNodesMsg.introducers.size()));
	}

	private akka.actor.FSM.State<State, StateData> processNodeBooted(NodesCount count) {
		count.increaseOne();

		if (count.isCompleted()) {
			taskSender.tell(new EndBootMessage(), self());
			return goTo(State.Idle);
		} else
			return stay();
	}

	private akka.actor.FSM.State<State, StateData> startRound(StartRound startRoundMsg) {
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
			taskSender.tell(new EndRound(), self());
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
		return goTo(State.MeasureRunning).using(new MeasureStateData(pendingNodes, msg.param));
	}

	private akka.actor.FSM.State<State, StateData> processNodeMeasure(EndMeasureMessage measure, MeasureStateData measureStateData) {
		measureStateData.increaseOne();
		int node = actorToInt(sender());
		for (ActorRef a : measure.neighbors) {
			int neighbor = actorToInt(a);
			adjacencyMatrix[node][neighbor] = true;
		}

		if (measureStateData.isCompleted()) {
			return executeNodeCalculation(measureStateData.param);
		} else {
			return stay();
		}
	}

	private akka.actor.FSM.State<State, StateData> executeNodeCalculation(GraphProperty param) {

		for (ActorRef child : JavaConversions.asJavaIterable(context().children())) {
			child.tell(new NodeCalcTask(adjacencyMatrix, param, actorToInt(child)), self());
		}

		return goTo(State.CalcRunning).using(new CalcStateData(adjacencyMatrix.length, param));
	}

	private akka.actor.FSM.State<State, StateData> processCalcResult(NodeCalcResult result, CalcStateData calcState) {
		calcState.aggregateData(result.result);

		calcState.increaseOne();
		if (calcState.isCompleted()) {
			RoundData processedRound = new RoundData(calcState.computeFinalData());
			taskSender.tell(processedRound, self());
			return goTo(State.Idle);
		} else {
			return stay();
		}
	}

	public static int actorToInt(ActorRef sender) {
		return Integer.parseInt(sender.path().name());
	}

	public static class StartAddNodesMessage {

		final int requiredNodes;
		final int cacheSize;
		final int shuffleLength;

		public StartAddNodesMessage(int requiredNodes, int cacheSize, int shuffleLength) {
			this.requiredNodes = requiredNodes;
			this.cacheSize = cacheSize;
			this.shuffleLength = shuffleLength;
		}

	}

	public static class EndAddNodesMessage {

		final Set<ActorRef> addedNodes;

		public EndAddNodesMessage(Set<ActorRef> addedNodes) {
			this.addedNodes = addedNodes;
		}

	}

	public static class StartBootMessage {

		final Map<ActorRef, ActorRef> introducers;

		public StartBootMessage(Map<ActorRef, ActorRef> introducers) {
			this.introducers = introducers;
		}

		public ActorRef getIntroducerOf(ActorRef ref) {
			return introducers.get(ref);
		}
	}

	public static class EndBootMessage {

	}

	public static class StartMeasureMessage {

		final GraphProperty param;

		public StartMeasureMessage(GraphProperty param) {
			this.param = param;
		}
	}

	public static class RoundData {

		final Object roundValue;

		public RoundData(Object roundValue) {
			this.roundValue = roundValue;
		}

		@Override
		public String toString() {
			return "RoundData " + roundValue;
		}
	}
}