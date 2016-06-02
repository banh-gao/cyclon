package it.unitn.zozin.da.cyclon;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import akka.actor.AbstractFSM;
import akka.actor.ActorRef;
import it.unitn.zozin.da.cyclon.GraphActor.EndBootMessage;
import it.unitn.zozin.da.cyclon.GraphActor.StartBootMessage;
import it.unitn.zozin.da.cyclon.NeighborsCache.Neighbor;
import it.unitn.zozin.da.cyclon.NodeActor.ReplyStateData;

public class NodeActor extends AbstractFSM<NodeActor.State, ReplyStateData> {

	public static final int JOIN_TTL = 5;

	enum State {
		Uninitialized, WaitingForReply, Idle
	}

	class ReplyStateData {

		private final int total;
		private final List<Neighbor> requestList;
		private int count = 0;

		public ReplyStateData(int total, List<Neighbor> requestList) {
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

	private Neighbor selfAddress;
	private final int shuffleLength;

	private boolean isJoined = false;

	private final NeighborsCache cache;

	public NodeActor(int cacheSize, int shuffleLength) {
		this.cache = new NeighborsCache(cacheSize);
		this.shuffleLength = shuffleLength;
	}

	@Override
	public void preStart() throws Exception {
		this.selfAddress = new Neighbor(0, self());
	}

	private akka.actor.FSM.State<State, ReplyStateData> processBootNode(StartBootMessage message) {
		// Initialize cache with the boot initializer
		cache.addNeighbors(Collections.singletonList(new Neighbor(0, message.getIntroducer(self()))));

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
		Neighbor introducer = cache.getRandomNeighbor();

		introducer.address.tell(new CyclonJoin(JOIN_TTL + 1), self());

		return goTo(State.WaitingForReply).using(new ReplyStateData(cache.maxSize(), Collections.emptyList()));
	}

	private akka.actor.FSM.State<State, ReplyStateData> processJoinRequest(CyclonJoin joinReq) {
		return processJoinRequestOnWaiting(joinReq, Collections.emptyList());
	}

	private akka.actor.FSM.State<State, ReplyStateData> processJoinRequestOnWaiting(CyclonJoin joinReq, List<Neighbor> pendingRequestList) {
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
		cache.getRandomNeighbor().address.forward(joinReq, context());
	}

	private void sendCyclonJoinAnswer(List<Neighbor> pendingRequestList) {
		List<Neighbor> selected = cache.removeRandomNeighbors(1, sender());
		cache.addNeighbors(Collections.singletonList(new Neighbor(0, sender())));

		if (selected.isEmpty() && !pendingRequestList.isEmpty())
			selected = pendingRequestList.subList(0, 1);

		sender().tell(new CyclonNodeAnswer(selected), self());
	}

	private akka.actor.FSM.State<State, ReplyStateData> sendCyclonRequest() {
		cache.increaseNeighborsAge();

		List<Neighbor> replaceable = new ArrayList<>();

		Neighbor dest = cache.removeOldestNeighbor();
		replaceable.add(dest);

		// Get other random neighbors
		List<Neighbor> requestNodes = cache.removeRandomNeighbors(shuffleLength - 1);
		replaceable.addAll(requestNodes);

		// Add fresh local node address
		requestNodes.add(selfAddress);

		dest.address.tell(new CyclonNodeRequest(requestNodes), self());

		return goTo(State.WaitingForReply).using(new ReplyStateData(1, replaceable));
	}

	private akka.actor.FSM.State<State, ReplyStateData> processCyclonRequest(CyclonNodeRequest req) {
		return processCyclonRequestOnWaiting(req, Collections.emptyList());
	}

	private akka.actor.FSM.State<State, ReplyStateData> processCyclonRequestOnWaiting(CyclonNodeRequest req, List<Neighbor> pendingRequestList) {
		// Remove itself (if present)
		req.nodes.remove(selfAddress);

		// Answer contains at most the same amount of entries as the request
		List<Neighbor> ansNodes = cache.removeRandomNeighbors(req.nodes.size(), sender());

		if (ansNodes.size() < req.nodes.size() && !pendingRequestList.isEmpty()) {
			ansNodes.addAll(pendingRequestList.subList(0, Math.min(req.nodes.size() - ansNodes.size(), pendingRequestList.size())));
		}

		sender().tell(new CyclonNodeAnswer(ansNodes), self());

		cache.addNeighbors(req.nodes);

		return stay();
	}

	private akka.actor.FSM.State<State, ReplyStateData> processCyclonAnswer(CyclonNodeAnswer answer, ReplyStateData ansCount) {
		ansCount.increaseOne();

		// Remove itself (if present)
		answer.nodes.remove(selfAddress);

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
		EndMeasureMessage m = new EndMeasureMessage(cache.getNeighbors().stream().map((n) -> n.address).collect(Collectors.toList()));
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

		final List<Neighbor> nodes;

		public CyclonNodeList(List<Neighbor> nodes) {
			// NOTE: Clone elements to avoid object reference passing in local
			// executions
			this.nodes = new ArrayList<NeighborsCache.Neighbor>();
			for (Neighbor n : nodes)
				this.nodes.add(n.clone());
		}

		@Override
		public String toString() {
			return "CyclonNodeList [nodes=" + nodes + "]";
		}
	}

	public static class CyclonNodeRequest extends CyclonNodeList {

		public CyclonNodeRequest(List<Neighbor> nodes) {
			super(nodes);
		}

	}

	public static class CyclonNodeAnswer extends CyclonNodeList {

		public CyclonNodeAnswer(List<Neighbor> nodes) {
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

		final List<ActorRef> neighbors;

		public EndMeasureMessage(List<ActorRef> neighbors) {
			this.neighbors = new ArrayList<ActorRef>(neighbors);
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