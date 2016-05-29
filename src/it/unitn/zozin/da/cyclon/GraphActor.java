package it.unitn.zozin.da.cyclon;

import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableSet;
import java.util.TreeSet;
import akka.actor.AbstractFSM;
import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.zozin.da.cyclon.NodeActor.EndRoundMessage;
import it.unitn.zozin.da.cyclon.NodeActor.MeasureDataMessage;
import it.unitn.zozin.da.cyclon.NodeActor.NodeCalcResult;
import it.unitn.zozin.da.cyclon.NodeActor.NodeCalcTask;
import it.unitn.zozin.da.cyclon.NodeActor.StartRoundMessage;
import scala.collection.JavaConversions;

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

		public Object tempCalcValue;

		public int current = 0;

		public CalcStateData(int total, GraphProperty param) {
			this.total = total;
			this.prop = param;
		}

		public void aggregateData(Object val) {
			this.tempCalcValue = prop.aggregate(this.tempCalcValue, val);
		}

		public boolean isCompleted() {
			return current == total;
		}

		public void increaseOne() {
			current++;
		}

		public Object computeFinalData() {
			return prop.computeFinal(this.tempCalcValue, total);
		}

	}

	{
		startWith(State.Idle, null);

		when(State.Idle, matchEvent(AddNodesMessage.class, (addNodesMsg, data) -> processAddNodes(addNodesMsg)));
		when(State.Idle, matchEvent(BootNodesMessage.class, (bootNodesMsg, data) -> startBoot(bootNodesMsg)));

		when(State.BootRunning, matchEvent(NodeActor.BootNodeEndedMessage.class, NodesCount.class, (endBootMsg, nodesCount) -> processNodeBooted(nodesCount)));

		when(State.Idle, matchEvent(StartRoundMessage.class, (startRoundMsg, data) -> startRound(startRoundMsg)));
		when(State.RoundRunning, matchEvent(NodeActor.EndRoundMessage.class, NodesCount.class, (endRoundMsg, nodesCount) -> processEndRound(nodesCount)));

		when(State.Idle, matchEvent(StartMeasureMessage.class, (startMeasureMsg, data) -> startMeasure(startMeasureMsg)));
		when(State.MeasureRunning, matchEvent(NodeActor.MeasureDataMessage.class, MeasureStateData.class, (measureDataMsg, nodesCount) -> processNodeMeasure(measureDataMsg, nodesCount)));

		when(State.CalcRunning, matchEvent(NodeActor.NodeCalcResult.class, CalcStateData.class, (calcDataMsg, calcState) -> processCalcResult(calcDataMsg, calcState)));
	}

	// Task processing state
	private ActorRef taskSender;

	boolean[][] adjacencyMatrix;

	private akka.actor.FSM.State<State, StateData> processAddNodes(AddNodesMessage addMsg) {
		NavigableSet<Integer> addedNodes = new TreeSet<Integer>();

		for (int i = 0; i < addMsg.requiredNodes; i++) {
			context().actorOf(Props.create(NodeActor.class, addMsg.cacheSize, addMsg.shuffleLength), Integer.toString(i));
			addedNodes.add(i);
		}

		sender().tell(new AddNodesEndedMessage(addedNodes), self());
		return stay();
	}

	private akka.actor.FSM.State<State, StateData> startBoot(BootNodesMessage bootNodesMsg) {
		taskSender = sender();

		for (Entry<Integer, Integer> e : bootNodesMsg.bootNeighbors.entrySet()) {
			ActorRef dest = toRef(e.getKey());
			dest.tell(new NodeActor.BootNodeMessage(toRef(e.getValue())), self());
		}
		// TODO Auto-generated method stub
		return goTo(State.BootRunning).using(new NodesCount(bootNodesMsg.bootNeighbors.size()));
	}

	private akka.actor.FSM.State<State, StateData> processNodeBooted(NodesCount count) {
		count.increaseOne();

		if (count.isCompleted()) {
			taskSender.tell(new BootNodesEndedMessage(), self());
			return goTo(State.Idle);
		} else
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
		return goTo(State.MeasureRunning).using(new MeasureStateData(pendingNodes, msg.param));
	}

	private akka.actor.FSM.State<State, StateData> processNodeMeasure(MeasureDataMessage measure, MeasureStateData measureStateData) {
		measureStateData.increaseOne();
		int node = toIndex(sender());
		for (ActorRef a : measure.neighbors) {
			int neighbor = toIndex(a);
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
			child.tell(new NodeCalcTask(adjacencyMatrix, param, toIndex(child)), self());
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

	private int toIndex(ActorRef sender) {
		return Integer.parseInt(sender.path().name());
	}

	private ActorRef toRef(int index) {
		return context().child(Integer.toString(index)).get();
	}

	public static class AddNodesMessage {

		final int requiredNodes;
		final int cacheSize;
		final int shuffleLength;

		public AddNodesMessage(int requiredNodes, int cacheSize, int shuffleLength) {
			this.requiredNodes = requiredNodes;
			this.cacheSize = cacheSize;
			this.shuffleLength = shuffleLength;

		}

	}

	public static class AddNodesEndedMessage {

		final NavigableSet<Integer> addedNodes;

		public AddNodesEndedMessage(NavigableSet<Integer> addedNodes) {
			this.addedNodes = addedNodes;
		}

	}

	public static class BootNodesMessage {

		final Map<Integer, Integer> bootNeighbors;

		public BootNodesMessage(Map<Integer, Integer> bootNeighbors) {
			this.bootNeighbors = bootNeighbors;
		}
	}

	public static class BootNodesEndedMessage {

	}

	public static class StartMeasureMessage {

		final GraphProperty param;

		public StartMeasureMessage(GraphProperty param) {
			this.param = param;
		}
	}

	public static class RoundData {

		public static final RoundData EMPTY_DATA = new RoundData(null);
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