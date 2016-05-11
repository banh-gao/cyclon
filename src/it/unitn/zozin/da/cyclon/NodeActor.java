package it.unitn.zozin.da.cyclon;

import it.unitn.zozin.da.cyclon.NeighborsCache.Neighbor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import akka.actor.AbstractFSM;
import akka.actor.ActorRef;

public class NodeActor extends AbstractFSM<NodeActor.State, NodeActor.StateData> {

	public static final int JOIN_TTL = 5;

	enum State {
		Uninitialized, WaitingForJoin, WaitingForReply, Idle
	}

	interface StateData {

	}

	private enum Uninitialized implements StateData {
		Uninitialized
	}

	class JoinAnswerCount implements StateData {

		private final int total;
		private int count = 0;

		public JoinAnswerCount(int total) {
			this.total = total;
		}

		public void increaseOne() {
			count += 1;
		}

		public boolean isCompleted() {
			return count == total;
		}
	}

	{
		startWith(State.Uninitialized, Uninitialized.Uninitialized);

		// Possible (state,event) combinations after initialization //

		// Start executing round (move to WaitingForReply state)
		when(State.Idle, matchEvent(StartRoundMessage.class, (startRoundMsg, data) -> sendCyclonRequest()));

		// Process cyclon answer for pending request and end the current round
		when(State.WaitingForReply, matchEvent(CyclonNodeAnswer.class, (nodeListMsg, reqStateData) -> processCyclonAnswer(nodeListMsg)));

		// Answer to a measure request (no state transition)
		when(State.Idle, matchEvent(StartMeasureMessage.class, (startMeasureMsg, data) -> processMeasureRequest()));
		when(State.WaitingForJoin, matchEvent(StartMeasureMessage.class, (startMeasureMsg, data) -> processMeasureRequest()));

		// Answer to a join requests (no state transition)
		when(State.Idle, matchEvent(CyclonJoin.class, (joinMsg, data) -> processJoinReq(joinMsg)));
		when(State.WaitingForJoin, matchEvent(CyclonJoin.class, (joinMsg, data) -> processJoinReq(joinMsg)));
		when(State.WaitingForReply, matchEvent(CyclonJoin.class, (joinMsg, data) -> processJoinReq(joinMsg)));

		// Answer to a node exchange requests (no state transition)
		when(State.Idle, matchEvent(CyclonNodeRequest.class, (nodeListMsg, data) -> processCyclonRequest(nodeListMsg)));
		when(State.WaitingForJoin, matchEvent(CyclonNodeRequest.class, (reqMsg, data) -> processCyclonRequest(reqMsg)));
		when(State.WaitingForReply, matchEvent(CyclonNodeRequest.class, (reqMsg, data) -> processCyclonRequest(reqMsg)));

		// Possible (state,event) combinations only after join //

		// Initialize the node
		when(State.Uninitialized, matchEvent(BootNodeMessage.class, (initMsg, data) -> processInitNode(initMsg)));

		// Start joining the node (executed on first round)
		when(State.WaitingForJoin, matchEvent(StartJoinMessage.class, (startJoinMsg, data) -> performJoin()));

		// Receive answers for the ongoing join process
		when(State.WaitingForJoin, matchEvent(CyclonNodeAnswer.class, JoinAnswerCount.class, (joinMsg, joinAnsCount) -> processJoinAnswer(joinMsg, joinAnsCount)));

	}

	private Neighbor selfAddress;

	private NeighborsCache cache;
	private int shuffleLength;

	@Override
	public void preStart() throws Exception {
		this.selfAddress = new Neighbor(0, self());
	}

	private akka.actor.FSM.State<State, StateData> processInitNode(BootNodeMessage message) {
		cache = new NeighborsCache(message.cacheSize);
		shuffleLength = message.shuffleLength;

		// Initialize cache with the boot neighbor
		cache.updateNeighbors(Collections.singletonList(new Neighbor(0, message.bootNeighbor)), false);

		sender().tell(new BootNodeEndedMessage(), self());
		return goTo(State.WaitingForJoin);
	}

	private akka.actor.FSM.State<State, StateData> performJoin() {
		ActorRef introducer = cache.getOldestNeighbor().address;

		introducer.tell(new CyclonJoin(JOIN_TTL + 1), self());

		return goTo(State.WaitingForJoin).using(new JoinAnswerCount(cache.maxSize()));
	}

	private akka.actor.FSM.State<State, StateData> processJoinAnswer(CyclonNodeList answer, JoinAnswerCount joinAnsCount) {
		joinAnsCount.increaseOne();

		// Save received nodes in cache
		cache.updateNeighbors(answer.nodes, false);

		if (joinAnsCount.isCompleted()) {
			context().parent().tell(new EndJoinMessage(), self());
			return goTo(State.Idle);
		}

		return stay();
	}

	private akka.actor.FSM.State<State, StateData> processJoinReq(CyclonJoin joinReq) {
		joinReq = joinReq.getReduced();

		// This is the introducer of the sender node
		if (joinReq.TTL == JOIN_TTL) {
			for (int i = 0; i < cache.maxSize(); i++) {
				forwardJoin(joinReq);
			}
		} else {
			if (joinReq.isEnded()) {
				// If random walk ends here
				sendCyclonJoinAnswer();
			} else {
				forwardJoin(joinReq);
			}
		}

		return stay();
	}

	private void forwardJoin(CyclonJoin joinReq) {
		cache.getRandomNeighbor().address.forward(joinReq, context());
	}

	private void sendCyclonJoinAnswer() {
		Neighbor replaced = cache.replaceRandomNeighbor(new Neighbor(0, sender()));

		sender().tell(new CyclonNodeAnswer(Collections.singletonList(replaced)), self());
	}

	private akka.actor.FSM.State<State, StateData> sendCyclonRequest() {
		cache.increaseNeighborsAge();

		Neighbor dest = cache.selectOldestNeighbor();

		// Get random neighbors (excluding destination neighbor)
		List<Neighbor> requestNodes = cache.selectRandomNeighbors(shuffleLength - 1, dest);

		// Add fresh local node address
		requestNodes.add(selfAddress);

		dest.address.tell(new CyclonNodeRequest(requestNodes), self());

		return goTo(State.WaitingForReply);
	}

	private akka.actor.FSM.State<State, StateData> processCyclonAnswer(CyclonNodeAnswer answer) {
		// Remove itself (if present)
		answer.nodes.remove(selfAddress);

		// Save received nodes in cache
		cache.updateNeighbors(answer.nodes, false);

		// Complete node protocol simulation round
		sendRoundCompletedStatus();

		return goTo(State.Idle);
	}

	private void sendRoundCompletedStatus() {
		context().parent().tell(new EndRoundMessage(), self());
	}

	private akka.actor.FSM.State<State, StateData> processCyclonRequest(CyclonNodeRequest req) {
		// Remove itself (if present)
		req.nodes.remove(selfAddress);

		sendCyclonAnswer();

		// Handle special case in which a request arrives while the local node
		// is waiting for an answer to its own request. In this case the entries
		// that can can be replaced when the answer arrives correspond to the
		// new entries just stored in the cache with this request.
		cache.updateNeighbors(req.nodes, true);

		return stay();
	}

	private void sendCyclonAnswer() {
		List<Neighbor> ansNodes = cache.selectRandomNeighbors(shuffleLength);
		sender().tell(new CyclonNodeAnswer(ansNodes), self());
	}

	private akka.actor.FSM.State<State, StateData> processMeasureRequest() {
		MeasureDataMessage m = new MeasureDataMessage(cache.getNeighbors().stream().map((n) -> n.address).collect(Collectors.toList()));

		sender().tell(m, self());

		return stay();
	}

	public static class BootNodeMessage {

		final int cacheSize;
		final int shuffleLength;
		final ActorRef bootNeighbor;

		public BootNodeMessage(int cacheSize, int shuffleLength, ActorRef bootNeighbor) {
			this.cacheSize = cacheSize;
			this.shuffleLength = shuffleLength;
			this.bootNeighbor = bootNeighbor;
		}
	}

	public static class BootNodeEndedMessage {

	}

	public static class StartJoinMessage {

	}

	public static class EndJoinMessage {

	}

	public static class StartRoundMessage {

	}

	public static class EndRoundMessage {

	}

	public static abstract class CyclonNodeList {

		final List<Neighbor> nodes;

		public CyclonNodeList(List<Neighbor> nodes) {
			// FIXME: Clone elements to avoid object reference passing in local
			// executions
			this.nodes = new ArrayList<NeighborsCache.Neighbor>();
			for (Neighbor n : nodes)
				this.nodes.add(n.clone());
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

	public static class CyclonJoin {

		final int TTL;

		public CyclonJoin(int TTL) {
			this.TTL = TTL;
		}

		public CyclonJoin getReduced() {
			return new CyclonJoin(TTL - 1);
		}

		public boolean isEnded() {
			return TTL == 0;
		}
	}

	public static class StartMeasureMessage {

	}

	public static class MeasureDataMessage {

		final List<ActorRef> neighbors;

		public MeasureDataMessage(List<ActorRef> neighbors) {
			this.neighbors = new ArrayList<ActorRef>(neighbors);
		}
	}
}