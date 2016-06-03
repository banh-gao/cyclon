package it.unitn.zozin.da.cyclon;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import akka.actor.AbstractFSM;
import akka.actor.ActorRef;
import it.unitn.zozin.da.cyclon.GraphActor.EndBootMessage;
import it.unitn.zozin.da.cyclon.GraphActor.StartBootMessage;
import it.unitn.zozin.da.cyclon.NodeActor.ReplyStateData;

/**
 * Actor following the Cyclon protocol
 */
public class NodeActor extends AbstractFSM<NodeActor.State, ReplyStateData> {

	// Set to a value close to estimated average path length
	public static final int JOIN_TTL = 4;

	enum State {
		Uninitialized, WaitingForReply, Idle
	}

	class ReplyStateData {

		private final int total;
		private final Map<ActorRef, Integer> requestList;
		private int count = 0;

		public ReplyStateData(int total, Map<ActorRef, Integer> requestList) {
			this.total = total;
			this.requestList = requestList;
		}

		public void increaseOne() {
			count += 1;
		}

		public boolean isCompleted() {
			return count == total;
		}
	}

	{
		startWith(State.Uninitialized, null);

		// Initialize the node
		when(State.Uninitialized, matchEvent(StartBootMessage.class, (initMsg, data) -> processBootNode(initMsg)));

		// Start executing round (move to WaitingForReply state)
		when(State.Idle, matchEvent(StartRound.class, (startRoundMsg, data) -> processStartRound()));

		// Process cyclon answer for pending request and end the current round
		when(State.WaitingForReply, matchEvent(CyclonNodeAnswer.class, ReplyStateData.class, (answerMsg, ansCount) -> processCyclonAnswer(answerMsg, ansCount)));

		// Process cyclon join and node list request
		// (no state transition)
		when(State.Idle, matchEvent(CyclonJoin.class, (joinMsg, data) -> processJoinRequest(joinMsg)));
		when(State.Idle, matchEvent(CyclonNodeRequest.class, (nodeListMsg, data) -> processCyclonRequest(nodeListMsg)));

		// Process cyclon join and node list request in the special case in
		// which this node is waiting for an answer for the current round
		// (no state transition)
		when(State.WaitingForReply, matchEvent(CyclonJoin.class, ReplyStateData.class, (joinMsg, pendingReqData) -> processJoinRequestOnWaiting(joinMsg, pendingReqData.requestList)));
		when(State.WaitingForReply, matchEvent(CyclonNodeRequest.class, ReplyStateData.class, (reqMsg, pendingReqData) -> processCyclonRequestOnWaiting(reqMsg, pendingReqData.requestList)));

		// Process measure and calc requests (no state transition)
		when(State.Idle, matchEvent(StartMeasureMessage.class, (startMeasureMsg, data) -> processMeasureRequest()));
		when(State.Idle, matchEvent(NodeCalcTask.class, (startCalcMsg, data) -> processCalcRequest(startCalcMsg)));
	}
	private final int shuffleLength;

	private boolean isJoined = false;

	private final NeighborsCache cache;

	public NodeActor(int cacheSize, int shuffleLength) {
		this.cache = new NeighborsCache(cacheSize);
		this.shuffleLength = shuffleLength;
	}

	private akka.actor.FSM.State<State, ReplyStateData> processBootNode(StartBootMessage message) {
		// Initialize cache with the boot initializer
		cache.addNeighbors(Collections.singletonMap(message.getIntroducerOf(self()), 0));

		sender().tell(new EndBootMessage(), self());

		return goTo(State.Idle);
	}

	private akka.actor.FSM.State<State, ReplyStateData> processStartRound() {
		if (!isJoined)
			return performJoin();
		else
			return sendCyclonRequest();
	}

	private akka.actor.FSM.State<State, ReplyStateData> performJoin() {
		ActorRef introducer = cache.getRandomNeighbor();

		introducer.tell(new CyclonJoin(JOIN_TTL + 1), self());

		return goTo(State.WaitingForReply).using(new ReplyStateData(cache.maxSize(), Collections.emptyMap()));
	}

	private akka.actor.FSM.State<State, ReplyStateData> processJoinRequest(CyclonJoin joinReq) {
		return processJoinRequestOnWaiting(joinReq, Collections.emptyMap());
	}

	private akka.actor.FSM.State<State, ReplyStateData> processJoinRequestOnWaiting(CyclonJoin joinReq, Map<ActorRef, Integer> pendingRequestList) {
		joinReq = joinReq.getAged();

		// This is the introducer of the sender node
		if (joinReq.TTL == JOIN_TTL) {
			for (int i = 0; i < cache.maxSize(); i++) {
				forwardJoin(joinReq);
			}
		} else {
			if (joinReq.isTimedOut()) {
				// If random walk ends here
				sendCyclonJoinAnswer(pendingRequestList);
			} else {
				forwardJoin(joinReq);
			}
		}

		return stay();
	}

	private void forwardJoin(CyclonJoin joinReq) {
		cache.getRandomNeighbor().forward(joinReq, context());
	}

	private void sendCyclonJoinAnswer(Map<ActorRef, Integer> pendingRequestList) {
		Map<ActorRef, Integer> selected = cache.removeRandomNeighbors(1, sender());
		cache.addNeighbors(Collections.singletonMap(sender(), 0));

		if (selected.isEmpty())
			pendingRequestList.entrySet().stream().limit(1).forEach((e) -> selected.put(e.getKey(), e.getValue()));

		sender().tell(new CyclonNodeAnswer(selected), self());
	}

	private akka.actor.FSM.State<State, ReplyStateData> sendCyclonRequest() {
		cache.increaseNeighborsAge();

		Map<ActorRef, Integer> replaceable = new HashMap<>();

		Entry<ActorRef, Integer> dest = cache.removeOldestNeighbor();
		replaceable.put(dest.getKey(), dest.getValue());

		// Get other random neighbors
		Map<ActorRef, Integer> requestNodes = cache.removeRandomNeighbors(shuffleLength - 1);
		replaceable.putAll(requestNodes);

		// Add fresh local node address
		requestNodes.put(self(), 0);

		dest.getKey().tell(new CyclonNodeRequest(requestNodes), self());

		return goTo(State.WaitingForReply).using(new ReplyStateData(1, replaceable));
	}

	private akka.actor.FSM.State<State, ReplyStateData> processCyclonRequest(CyclonNodeRequest req) {
		return processCyclonRequestOnWaiting(req, Collections.emptyMap());
	}

	private akka.actor.FSM.State<State, ReplyStateData> processCyclonRequestOnWaiting(CyclonNodeRequest req, Map<ActorRef, Integer> pendingRequestList) {
		// Remove itself (if present)
		req.nodes.remove(self());

		// Answer contains at most the same amount of entries as the request
		Map<ActorRef, Integer> ansNodes = cache.removeRandomNeighbors(req.nodes.size(), sender());

		if (ansNodes.size() < req.nodes.size()) {
			pendingRequestList.entrySet().stream().limit(req.nodes.size() - ansNodes.size()).forEach((e) -> ansNodes.put(e.getKey(), e.getValue()));
		}

		sender().tell(new CyclonNodeAnswer(ansNodes), self());

		cache.addNeighbors(req.nodes);

		return stay();
	}

	private akka.actor.FSM.State<State, ReplyStateData> processCyclonAnswer(CyclonNodeAnswer answer, ReplyStateData ansCount) {
		ansCount.increaseOne();

		// Remove itself (if present)
		answer.nodes.remove(self());

		// Save received nodes in cache
		cache.addNeighbors(answer.nodes);

		// Fill cache by storing again the entries sent in the request
		cache.addNeighbors(ansCount.requestList);

		if (ansCount.isCompleted()) {
			isJoined = true;
			// Complete node protocol simulation round
			sendRoundCompletedStatus();
			return goTo(State.Idle);
		}

		return stay();
	}

	private void sendRoundCompletedStatus() {
		context().parent().tell(new EndRound(), self());
	}

	private akka.actor.FSM.State<State, ReplyStateData> processMeasureRequest() {
		EndMeasureMessage m = new EndMeasureMessage(cache.getNeighbors());
		sender().tell(m, self());
		return stay();
	}

	private akka.actor.FSM.State<State, ReplyStateData> processCalcRequest(NodeCalcTask message) {
		sender().tell(new NodeCalcResult(message.calculate()), self());
		return stay();
	}

	public static class CyclonJoin {

		final int TTL;

		public CyclonJoin(int TTL) {
			this.TTL = TTL;
		}

		public CyclonJoin getAged() {
			return new CyclonJoin(TTL - 1);
		}

		public boolean isTimedOut() {
			return TTL == 0;
		}
	}

	public static abstract class CyclonNodeList {

		final Map<ActorRef, Integer> nodes;

		public CyclonNodeList(Map<ActorRef, Integer> nodes) {
			this.nodes = nodes;
		}

		@Override
		public String toString() {
			return "CyclonNodeList [nodes=" + nodes + "]";
		}
	}

	public static class CyclonNodeRequest extends CyclonNodeList {

		public CyclonNodeRequest(Map<ActorRef, Integer> nodes) {
			super(nodes);
		}

	}

	public static class CyclonNodeAnswer extends CyclonNodeList {

		public CyclonNodeAnswer(Map<ActorRef, Integer> nodes) {
			super(nodes);
		}
	}

	public static class StartRound {

	}

	public static class EndRound {

	}

	public static class StartMeasureMessage {

	}

	public static class EndMeasureMessage {

		final Set<ActorRef> neighbors;

		public EndMeasureMessage(Set<ActorRef> neighbors) {
			this.neighbors = neighbors;
		}
	}

	public static class NodeCalcTask {

		private final int node;
		private final boolean[][] graph;
		private final GraphProperty param;

		public NodeCalcTask(boolean[][] graph, GraphProperty param, int node) {
			this.graph = graph;
			this.param = param;
			this.node = node;
		}

		/**
		 * Executes the time consuming calculation in the calling thread
		 * 
		 * @return the calculation result
		 */
		public Object calculate() {
			return param.calculate(node, graph);
		}
	}

	public static class NodeCalcResult {

		final Object result;

		public NodeCalcResult(Object result) {
			this.result = result;
		}
	}
}