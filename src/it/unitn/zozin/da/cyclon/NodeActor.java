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
		Uninitialized, WaitingForReply, Idle
	}

	interface StateData {

	}

	class AnswerCount implements StateData {

		private final int total;
		private int count = 0;

		public AnswerCount(int total) {
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
		startWith(State.Uninitialized, null);

		// Initialize the node
		when(State.Uninitialized, matchEvent(BootNodeMessage.class, (initMsg, data) -> processInitNode(initMsg)));

		// Start executing round (move to WaitingForReply state)
		when(State.Idle, matchEvent(StartRoundMessage.class, (startRoundMsg, data) -> processStartRound()));

		// Process cyclon answer for pending request and end the current round
		when(State.WaitingForReply, matchEvent(CyclonNodeAnswer.class, AnswerCount.class, (answerMsg, ansCount) -> processCyclonAnswer(answerMsg, ansCount)));

		// Process cyclon join and node exchange requests
		// (no state transition)
		when(State.Idle, matchEvent(CyclonJoin.class, (joinMsg, data) -> processJoinRequest(joinMsg)));
		when(State.Idle, matchEvent(CyclonNodeRequest.class, (nodeListMsg, data) -> processExchangeRequest(nodeListMsg)));
		when(State.WaitingForReply, matchEvent(CyclonJoin.class, (joinMsg, data) -> processJoinRequest(joinMsg)));
		when(State.WaitingForReply, matchEvent(CyclonNodeRequest.class, (reqMsg, data) -> processExchangeRequest(reqMsg)));

		// Process measure requests (no state transition)
		when(State.Idle, matchEvent(StartMeasureMessage.class, (startMeasureMsg, data) -> processMeasureRequest()));
	}

	private Neighbor selfAddress;

	private final NeighborsCache cache;
	private final int shuffleLength;

	private boolean isJoined = false;

	public NodeActor(int cacheSize, int shuffleLength) {
		this.cache = new NeighborsCache(cacheSize);
		this.shuffleLength = shuffleLength;
	}

	@Override
	public void preStart() throws Exception {
		this.selfAddress = new Neighbor(0, self());
	}

	private akka.actor.FSM.State<State, StateData> processStartRound() {
		if (!isJoined)
			return performJoin();
		else
			return sendCyclonRequest();
	}

	private akka.actor.FSM.State<State, StateData> processInitNode(BootNodeMessage message) {
		// Initialize cache with the boot initializer
		cache.updateNeighbors(Collections.singletonList(new Neighbor(0, message.introducer)), false);

		sender().tell(new BootNodeEndedMessage(), self());
		return goTo(State.Idle);
	}

	private akka.actor.FSM.State<State, StateData> performJoin() {
		ActorRef introducer = cache.selectOldestNeighbor().address;

		introducer.tell(new CyclonJoin(JOIN_TTL + 1), self());

		return goTo(State.WaitingForReply).using(new AnswerCount(cache.maxSize()));
	}

	private akka.actor.FSM.State<State, StateData> processJoinRequest(CyclonJoin joinReq) {
		joinReq = joinReq.getAged();

		// This is the introducer of the sender node
		if (joinReq.TTL == JOIN_TTL) {
			for (int i = 0; i < cache.maxSize(); i++) {
				forwardJoin(joinReq);
			}
		} else {
			if (joinReq.isTimedOut()) {
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
		List<Neighbor> selected = cache.selectRandomNeighbors(shuffleLength, sender());

		cache.updateNeighbors(Collections.singletonList(new Neighbor(0, sender())), false);

		sender().tell(new CyclonNodeAnswer(selected), self());
	}

	private akka.actor.FSM.State<State, StateData> sendCyclonRequest() {
		cache.increaseNeighborsAge();

		Neighbor dest = cache.selectOldestNeighbor();

		// Get random neighbors (excluding destination neighbor)
		List<Neighbor> requestNodes = cache.selectRandomNeighbors(shuffleLength - 1, dest.address);

		// Add fresh local node address
		requestNodes.add(selfAddress);

		dest.address.tell(new CyclonNodeRequest(requestNodes), self());

		return goTo(State.WaitingForReply).using(new AnswerCount(1));
	}

	private akka.actor.FSM.State<State, StateData> processCyclonAnswer(CyclonNodeAnswer answer, AnswerCount ansCount) {
		// Remove itself (if present)
		answer.nodes.remove(selfAddress);
		ansCount.increaseOne();

		// Save received nodes in cache
		cache.updateNeighbors(answer.nodes, true);

		if (ansCount.isCompleted()) {
			isJoined = true;
			// Complete node protocol simulation round
			sendRoundCompletedStatus();
			return goTo(State.Idle);
		}

		return stay();
	}

	private void sendRoundCompletedStatus() {
		context().parent().tell(new EndRoundMessage(), self());
	}

	private akka.actor.FSM.State<State, StateData> processExchangeRequest(CyclonNodeRequest req) {
		// Remove itself (if present)
		req.nodes.remove(selfAddress);

		List<Neighbor> ansNodes = cache.selectRandomNeighbors(shuffleLength, sender());
		sender().tell(new CyclonNodeAnswer(ansNodes), self());

		cache.updateNeighbors(req.nodes, false);

		return stay();
	}

	private akka.actor.FSM.State<State, StateData> processMeasureRequest() {
		MeasureDataMessage m = new MeasureDataMessage(cache.getNeighbors().stream().map((n) -> n.address).collect(Collectors.toList()));

		sender().tell(m, self());

		return stay();
	}

	public static class BootNodeMessage {

		final ActorRef introducer;

		public BootNodeMessage(ActorRef introducer) {
			this.introducer = introducer;
		}
	}

	public static class BootNodeEndedMessage {

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

		public CyclonJoin getAged() {
			return new CyclonJoin(TTL - 1);
		}

		public boolean isTimedOut() {
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