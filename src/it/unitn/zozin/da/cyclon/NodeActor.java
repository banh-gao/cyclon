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

	public static final int REQUEST_TIMEOUT = 1000;

	private final NeighborsCache cache;
	private Neighbor selfAddress;

	private int round = 0;
	private List<Neighbor> replaceableNodes;

	public NodeActor() {
		this.cache = new NeighborsCache(CACHE_SIZE);
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
			if (cache.size() == CACHE_SIZE)
				return; // Boot completed: ignore other identities

			this.cache.updateNeighbors(Collections.singletonList(new Neighbor(0, remoteActor)), Collections.emptyList());

			if (cache.size() == CACHE_SIZE) {
				sendCyclonRequest();
			}
		}
	}

	public void startProtocolRound() {
		if (cache.size() < CACHE_SIZE) {
			performBoot();
		} else {
			round++;
			sendCyclonRequest();
		}
	}

	private void performBoot() {
		getContext().actorSelection("../*").tell(new Identify(null), getSelf());
		// TODO: implement cyclon join protocol
	}

	public void sendCyclonRequest() {
		cache.increaseNeighborsAge();

		Neighbor dest = cache.getOldestNeighbor();

		// Get random neighbors (excluding destination neighbor)
		List<Neighbor> requestNodes = cache.getRandomNeighbors(SHUFFLE_LENGTH - 1, dest);

		// Nodes that can be replaced when receiving an answer to this request
		replaceableNodes = new ArrayList<Neighbor>(requestNodes);
		replaceableNodes.add(dest);

		// Add fresh local node address
		requestNodes.add(selfAddress);

		dest.address.tell(new CyclonNodeList(requestNodes, true), getSelf());

		// FIXME: end round when on answer timeout
	}

	private void sendRoundCompletedStatus() {
		System.out.println(round + " " + getSelf().path().name() + " CACHE: " + cache);
		getContext().parent().tell(new StatusMessage(), getSelf());
	}

	private void processProtocolMessage(ProtocolMessage message) {
		CyclonNodeList nodeList = (CyclonNodeList) message;
		if (nodeList.isRequest)
			processCyclonRequest(nodeList);
		else
			processCyclonAnswer(nodeList);
	}

	private void processCyclonRequest(CyclonNodeList message) {
		while (message.nodes.remove(selfAddress));

		List<Neighbor> nodes = cache.getRandomNeighbors(SHUFFLE_LENGTH);

		cache.updateNeighbors(message.nodes, nodes);

		getSender().tell(new CyclonNodeList(nodes, false), getSelf());
	}

	private void processCyclonAnswer(CyclonNodeList answer) {
		while (answer.nodes.remove(selfAddress));

		cache.updateNeighbors(answer.nodes, replaceableNodes);

		sendRoundCompletedStatus();
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