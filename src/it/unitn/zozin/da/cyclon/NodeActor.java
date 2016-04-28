package it.unitn.zozin.da.cyclon;

import it.unitn.zozin.da.cyclon.GraphActor.ControlMessage;
import it.unitn.zozin.da.cyclon.GraphActor.ControlMessage.StatusMessage;
import it.unitn.zozin.da.cyclon.NeighborsCache.Neighbor;
import it.unitn.zozin.da.cyclon.task.MeasureMessage;
import it.unitn.zozin.da.cyclon.task.MeasureMessage.ReportMessage;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import akka.actor.ActorIdentity;
import akka.actor.ActorRef;
import akka.actor.Identify;
import akka.actor.UntypedActor;

public class NodeActor extends UntypedActor {

	public static final int CACHE_SIZE = 5;
	public static final int SHUFFLE_LENGTH = 1;

	private Neighbor selfAddress;
	private int round = 0;

	private final NeighborsCache cache;
	private List<Neighbor> replaceableNodes;

	public NodeActor() {
		cache = new NeighborsCache(CACHE_SIZE);
		replaceableNodes = new ArrayList<Neighbor>(SHUFFLE_LENGTH);
	}

	@Override
	public void preStart() throws Exception {
		this.selfAddress = new Neighbor(0, getSelf());
	}

	@Override
	public void onReceive(Object message) throws Exception {
		if (message instanceof ControlMessage)
			((ControlMessage) message).execute(this);
		else if (message instanceof ProtocolMessage)
			processProtocolMessage((ProtocolMessage) message);
		else if (message instanceof MeasureMessage)
			getSender().tell(new ReportMessage(((MeasureMessage) message).map(this)), getSelf());
		else if (message instanceof ActorIdentity) {
			ActorRef remoteActor = ((ActorIdentity) message).getRef();

			// Ignore self identity
			if (remoteActor.equals(getSelf()))
				return;

			// TODO: Implement cyclon join protocol
			if (cache.freeSlots() == 0)
				return; // Boot completed: ignore other identities

			this.cache.updateNeighbors(Collections.singletonList(new Neighbor(0, remoteActor)), Collections.emptyList(), this);

			if (cache.freeSlots() == 0) {
				sendCyclonRequest();
			}
		}
	}

	private void processProtocolMessage(ProtocolMessage message) {
		CyclonNodeList nodeList = (CyclonNodeList) message;
		if (nodeList.isRequest)
			processCyclonRequest(nodeList);
		else
			processCyclonAnswer(nodeList);
	}

	public void startProtocolRound() {
		if (cache.freeSlots() == CACHE_SIZE) {
			performBoot();
		} else {
			sendCyclonRequest();
		}
	}

	private void performBoot() {
		// TODO: implement cyclon join protocol
		getContext().actorSelection("../*").tell(new Identify(null), getSelf());
	}

	public void sendCyclonRequest() {
		cache.increaseNeighborsAge();

		Neighbor dest = cache.getOldestNeighbor();

		// Get random neighbors (excluding destination neighbor)
		List<Neighbor> requestNodes = cache.getRandomNeighbors(SHUFFLE_LENGTH - 1, dest);

		// Nodes that can be replaced when receiving an answer to this request
		replaceableNodes.clear();
		replaceableNodes.addAll(requestNodes);
		replaceableNodes.add(dest);

		// Add fresh local node address
		requestNodes.add(selfAddress);

		dest.address.tell(new CyclonNodeList(requestNodes, true), getSelf());

		// FIXME: end round also on answer timeout
	}

	private void processCyclonRequest(CyclonNodeList req) {
		// Remove itself (if present)
		while (req.nodes.remove(selfAddress));

		// Prepare answer and save received nodes in cache
		List<Neighbor> ansNodes = cache.getRandomNeighbors(SHUFFLE_LENGTH);

		cache.updateNeighbors(req.nodes, ansNodes, this);

		// Special case in which a request arrives while the local node is
		// waiting for an answer to its own request. In this case the entries
		// that can can be replaced when the answer arrives correspond to the
		// new entries just stored in the cache.
		replaceableNodes.clear();
		replaceableNodes.addAll(req.nodes);

		// Send answer
		getSender().tell(new CyclonNodeList(ansNodes, false), getSelf());
	}

	private void processCyclonAnswer(CyclonNodeList answer) {
		// TODO: add invariant to check for previously sent request

		// Remove all entries of itself
		while (answer.nodes.remove(selfAddress));

		// Save received nodes in cache
		cache.updateNeighbors(answer.nodes, replaceableNodes, this);

		// Complete node protocol simulation round
		sendRoundCompletedStatus();
	}

	private void sendRoundCompletedStatus() {
		System.out.println(round + " " + getSelf().path().name() + " CACHE: " + cache);
		round++;
		getContext().parent().tell(new StatusMessage(), getSelf());
	}

	@Override
	public String toString() {
		return "NodeActor [selfAddress=" + selfAddress + ", round=" + round + "]";
	}

	interface ProtocolMessage {

	}

	public static class CyclonNodeList implements ProtocolMessage {

		final boolean isRequest;
		final List<Neighbor> nodes;

		public CyclonNodeList(List<Neighbor> nodes, boolean isRequest) {
			this.nodes = nodes;
			this.isRequest = isRequest;
		}

		@Override
		public String toString() {
			return "CyclonNodeList [isRequest=" + isRequest + ", nodes=" + nodes + "]";
		}

	}

	public static class CyclonJoin implements ProtocolMessage {

	}
}