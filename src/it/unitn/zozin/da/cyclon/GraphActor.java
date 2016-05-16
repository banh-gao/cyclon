package it.unitn.zozin.da.cyclon;

import it.unitn.zozin.da.cyclon.DataProcessor.GraphProperty;
import it.unitn.zozin.da.cyclon.DataProcessor.RoundData;
import it.unitn.zozin.da.cyclon.NodeActor.EndRoundMessage;
import it.unitn.zozin.da.cyclon.NodeActor.MeasureDataMessage;
import it.unitn.zozin.da.cyclon.NodeActor.StartRoundMessage;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import scala.collection.JavaConversions;
import akka.actor.AbstractFSM;
import akka.actor.ActorRef;
import akka.actor.Props;

public class GraphActor extends AbstractFSM<GraphActor.State, GraphActor.StateData> {

	enum State {
		Idle, BootRunning, RoundRunning, MeasureRunning
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

		when(State.Idle, matchEvent(AddNodesMessage.class, (addNodesMsg, data) -> processAddNodes(addNodesMsg)));
		when(State.Idle, matchEvent(BootNodesMessage.class, (bootNodesMsg, data) -> startBoot(bootNodesMsg)));

		when(State.BootRunning, matchEvent(NodeActor.BootNodeEndedMessage.class, NodesCount.class, (endBootMsg, nodesCount) -> processNodeBooted(nodesCount)));

		when(State.Idle, matchEvent(StartRoundMessage.class, (startRoundMsg, data) -> startRound(startRoundMsg)));
		when(State.RoundRunning, matchEvent(NodeActor.EndRoundMessage.class, NodesCount.class, (endRoundMsg, nodesCount) -> processEndRound(nodesCount)));

		when(State.Idle, matchEvent(StartMeasureMessage.class, (startMeasureMsg, data) -> startMeasure(startMeasureMsg)));
		when(State.MeasureRunning, matchEvent(NodeActor.MeasureDataMessage.class, MeasureStateData.class, (measureDataMsg, nodesCount) -> processNodeMeasure(measureDataMsg, nodesCount)));
	}

	// Task processing state
	private ActorRef taskSender;

	private final DataProcessor dataProcessor = new DataProcessor();
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

		final Set<GraphProperty> params;

		public StartMeasureMessage(Set<GraphProperty> params) {
			super();
			this.params = params;
		}
	}
}