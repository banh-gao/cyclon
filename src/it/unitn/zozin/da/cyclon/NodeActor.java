package it.unitn.zozin.da.cyclon;

import it.unitn.zozin.da.cyclon.GraphActor.InitNodeMessage;
import it.unitn.zozin.da.cyclon.GraphActor.StartMeasureMessage;
import it.unitn.zozin.da.cyclon.GraphActor.StartRoundMessage;
import it.unitn.zozin.da.cyclon.Message.StatusMessage;
import it.unitn.zozin.da.cyclon.Message.TaskMessage;
import it.unitn.zozin.da.cyclon.NeighborsCache.Neighbor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import akka.actor.UntypedActor;

public class NodeActor extends UntypedActor {

	private static final MessageMatcher<NodeActor> MATCHER = MessageMatcher.getInstance();

	static {
		MATCHER.set(InitNodeMessage.class, NodeActor::processInitNode);
		MATCHER.set(StartRoundMessage.class, NodeActor::processTask);
		MATCHER.set(StartMeasureMessage.class, NodeActor::processTask);
		MATCHER.set(CyclonNodeList.class, NodeActor::processNodeList);
		MATCHER.set(CyclonJoin.class, NodeActor::processJoinReq);
	}

	private Neighbor selfAddress;
	private int shuffleLength;

	NeighborsCache cache;

	@Override
	public void preStart() throws Exception {
		this.selfAddress = new Neighbor(0, getSelf());
	}

	@Override
	public void onReceive(Object message) throws Exception {
		MATCHER.process(message, this);
	}

	private static void processInitNode(InitNodeMessage message, NodeActor n) {
		n.cache = new NeighborsCache(message.cacheSize);
		n.shuffleLength = message.shuffleLength;

		// Initialize cache with the boot neighbor
		n.cache.updateNeighbors(Collections.singletonList(new Neighbor(0, message.bootNeighbor)), false);

		n.getSender().tell(new GraphActor.InitNodeEndedMessage(), n.getSelf());
	}

	private static void processNodeList(CyclonNodeList nodeList, NodeActor n) {
		if (nodeList.isRequest)
			n.processCyclonRequest(nodeList);
		else
			n.processCyclonAnswer(nodeList);
	}

	private static void processTask(TaskMessage message, NodeActor n) {
		message.execute(n);
	}

	private static void processJoinReq(CyclonJoin joinReq, NodeActor n) {
		// TODO: Implement cyclon join protocol
	}

	public void startProtocolRound() {
		if (cache.size() == 0) {
			performBoot();
		} else {
			sendCyclonRequest();
		}
	}

	private void performBoot() {
		// TODO: implement cyclon join protocol
	}

	private void sendCyclonRequest() {
		cache.increaseNeighborsAge();

		Neighbor dest = cache.selectOldestNeighbor();

		// Get random neighbors (excluding destination neighbor)
		List<Neighbor> requestNodes = cache.selectRandomNeighbors(shuffleLength - 1, dest);

		// Add fresh local node address
		requestNodes.add(selfAddress);

		dest.address.tell(new CyclonNodeList(requestNodes, true), getSelf());

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
		getContext().parent().tell(new EndRoundMessage(), getSelf());
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
		getSender().tell(new CyclonNodeList(ansNodes, false), getSelf());
	}

	public static class EndRoundMessage implements StatusMessage {

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
}