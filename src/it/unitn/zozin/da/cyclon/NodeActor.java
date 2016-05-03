package it.unitn.zozin.da.cyclon;

import it.unitn.zozin.da.cyclon.NeighborsCache.Neighbor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import akka.actor.AbstractFSM;
import akka.actor.ActorRef;

public class NodeActor extends AbstractFSM<NodeActor.State, NodeActor.StateData> {

	enum State {
		Uninitialized, Ready
	}

	interface StateData {

	}

	private enum Uninitialized implements StateData {
		Uninitialized
	}

	{
		startWith(State.Uninitialized, Uninitialized.Uninitialized);

		when(State.Uninitialized, matchEvent(InitNodeMessage.class, (initMsg, data) -> processInitNode(initMsg)));
		when(State.Ready, matchEvent(StartRoundMessage.class, (startRoundMsg, data) -> processStartRound()));
		when(State.Ready, matchEvent(StartMeasureMessage.class, (startMeasureMsg, data) -> processMeasureRequest()));

		when(State.Ready, matchEvent(CyclonNodeList.class, (nodeListMsg, data) -> processNodeList(nodeListMsg)));
		when(State.Ready, matchEvent(CyclonJoin.class, (joinMsg, data) -> processJoinReq(joinMsg)));
	}

	private Neighbor selfAddress;

	private NeighborsCache cache;
	private int shuffleLength;

	@Override
	public void preStart() throws Exception {
		this.selfAddress = new Neighbor(0, self());
	}

	private akka.actor.FSM.State<State, StateData> processInitNode(InitNodeMessage message) {
		cache = new NeighborsCache(message.cacheSize);
		shuffleLength = message.shuffleLength;

		// Initialize cache with the boot neighbor
		cache.updateNeighbors(Collections.singletonList(new Neighbor(0, message.bootNeighbor)), false);

		sender().tell(new GraphActor.InitNodeEndedMessage(), self());
		return goTo(State.Ready);
	}

	public akka.actor.FSM.State<State, StateData> processStartRound() {
		if (cache.size() == 0) {
			// FIXME: run only on the first round of the node
			performBoot();
		} else {
			sendCyclonRequest();
		}
		return goTo(State.Ready);
	}

	private void performBoot() {
		// TODO: implement cyclon join protocol
	}

	private akka.actor.FSM.State<State, StateData> processNodeList(CyclonNodeList nodeList) {
		if (nodeList.isRequest)
			processCyclonRequest(nodeList);
		else
			processCyclonAnswer(nodeList);

		return stay();
	}

	private akka.actor.FSM.State<State, StateData> processJoinReq(CyclonJoin joinReq) {
		// TODO: Implement cyclon join protocol
		return stay();
	}

	private void sendCyclonRequest() {
		cache.increaseNeighborsAge();

		Neighbor dest = cache.selectOldestNeighbor();

		// Get random neighbors (excluding destination neighbor)
		List<Neighbor> requestNodes = cache.selectRandomNeighbors(shuffleLength - 1, dest);

		// Add fresh local node address
		requestNodes.add(selfAddress);

		dest.address.tell(new CyclonNodeList(requestNodes, true), self());

		// FIXME: end round also on answer timeout
	}

	private void processCyclonAnswer(CyclonNodeList answer) {
		// Remove itself (if present)
		answer.nodes.remove(selfAddress);

		// Save received nodes in cache
		cache.updateNeighbors(answer.nodes, false);

		// Complete node protocol simulation round
		sendRoundCompletedStatus();
	}

	private void sendRoundCompletedStatus() {
		context().parent().tell(new EndRoundMessage(), self());
	}

	private void processCyclonRequest(CyclonNodeList req) {
		// Remove itself (if present)
		req.nodes.remove(selfAddress);

		// Prepare answer and save received nodes in cache
		List<Neighbor> ansNodes = cache.selectRandomNeighbors(shuffleLength);

		// Handle special case in which a request arrives while the local node
		// is waiting for an answer to its own request. In this case the entries
		// that can can be replaced when the answer arrives correspond to the
		// new entries just stored in the cache with this request.

		cache.updateNeighbors(req.nodes, true);

		// Send answer
		sender().tell(new CyclonNodeList(ansNodes, false), self());
	}

	private akka.actor.FSM.State<State, StateData> processMeasureRequest() {
		MeasureDataMessage m = new MeasureDataMessage();

		m.incrementNodeCounter();

		for (Neighbor neighbor : cache.getNeighbors())
			m.incrementInDegree(neighbor.address);

		sender().tell(m, self());

		return stay();
	}

	public static class InitNodeMessage {

		final int cacheSize;
		final int shuffleLength;
		final ActorRef bootNeighbor;

		public InitNodeMessage(int cacheSize, int shuffleLength, ActorRef bootNeighbor) {
			this.cacheSize = cacheSize;
			this.shuffleLength = shuffleLength;
			this.bootNeighbor = bootNeighbor;
		}

	}

	public static class StartRoundMessage {

	}

	public static class EndRoundMessage {

	}

	public static class CyclonNodeList {

		final boolean isRequest;
		final List<Neighbor> nodes;

		public CyclonNodeList(List<Neighbor> nodes, boolean isRequest) {
			this.nodes = new ArrayList<NeighborsCache.Neighbor>();
			for (Neighbor n : nodes)
				this.nodes.add(n.clone());
			this.isRequest = isRequest;
		}
	}

	public static class CyclonJoin {

	}

	public static class StartMeasureMessage {

	}

	public static class MeasureDataMessage {

		final Map<ActorRef, Integer> inDegree = new HashMap<ActorRef, Integer>();

		int totalNodes = 0;

		public void incrementNodeCounter() {
			totalNodes++;
		}

		public void incrementInDegree(ActorRef node) {
			int v = inDegree.getOrDefault(node, 0);
			inDegree.put(node, v + 1);
		}

		public void aggregate(MeasureDataMessage msg) {
			totalNodes += msg.totalNodes;
			for (Entry<ActorRef, Integer> e : msg.inDegree.entrySet()) {
				int v = inDegree.getOrDefault(e.getKey(), 0);
				inDegree.put(e.getKey(), v + e.getValue());
			}
		}
	}
}