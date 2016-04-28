package it.unitn.zozin.da.cyclon;

import it.unitn.zozin.da.cyclon.Message.ControlMessage;
import it.unitn.zozin.da.cyclon.Message.DataMessage;
import it.unitn.zozin.da.cyclon.Message.StatusMessage;
import it.unitn.zozin.da.cyclon.NeighborsCache.Neighbor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import scala.concurrent.duration.Duration;
import akka.actor.ActorIdentity;
import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Identify;
import akka.actor.UntypedActor;

public class NodeActor extends UntypedActor {

	public static final int CACHE_SIZE = 5;
	public static final int SHUFFLE_LENGTH = 1;

	private Neighbor selfAddress;
	private int round = 0;

	private final NeighborsCache cache;
	private List<Neighbor> replaceableEntries;
	private Cancellable timeout;

	public NodeActor() {
		cache = new NeighborsCache(CACHE_SIZE);
		replaceableEntries = new ArrayList<Neighbor>(SHUFFLE_LENGTH);
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
		else if (message instanceof ActorIdentity) {
			ActorRef remoteActor = ((ActorIdentity) message).getRef();

			// Ignore self identity
			if (remoteActor.equals(getSelf()))
				return;

			// TODO: Implement cyclon join protocol
			if (cache.freeSlots() == 0)
				return; // Boot completed: ignore other identities

			cache.updateNeighbors(Collections.singletonList(new Neighbor(0, remoteActor)), Collections.emptyList());

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

		// Cache entries that can be replaced when the answer to this request is
		// received
		replaceableEntries.clear();
		replaceableEntries.addAll(requestNodes);
		replaceableEntries.add(dest);

		// Add fresh local node address
		requestNodes.add(selfAddress);

		dest.address.tell(new CyclonNodeList(requestNodes, true), getSelf());

		System.out.println(round + " REQ SENT!");

		// TODO: end round also on answer timeout
		timeout = getContext().system().scheduler().scheduleOnce(Duration.create(1000, TimeUnit.SECONDS), new Runnable() {

			@Override
			public void run() {
				System.out.println("TIMEOUT!");
			}
		}, getContext().system().dispatcher());
	}

	private void processCyclonRequest(CyclonNodeList req) {
		// Remove itself (if present)
		req.nodes.remove(selfAddress);

		// Prepare answer and save received nodes in cache
		List<Neighbor> ansNodes = cache.getRandomNeighbors(SHUFFLE_LENGTH);

		cache.updateNeighbors(req.nodes, ansNodes);

		// Handle special case in which a request arrives while the local node
		// is waiting for an answer to its own request. In this case the entries
		// that can can be replaced when the answer arrives correspond to the
		// new entries just stored in the cache with this request.
		replaceableEntries.clear();
		replaceableEntries.addAll(req.nodes);

		// Send answer
		getSender().tell(new CyclonNodeList(ansNodes, false), getSelf());
	}

	private void processCyclonAnswer(CyclonNodeList answer) {
		timeout.cancel();
		// Remove itself (if present)
		answer.nodes.remove(selfAddress);

		// Save received nodes in cache
		cache.updateNeighbors(answer.nodes, replaceableEntries);

		// Complete node protocol simulation round
		sendRoundCompletedStatus();
	}

	private void sendRoundCompletedStatus() {
		System.out.println(round + " " + getSelf().path().name() + " CACHE: " + cache);
		round++;
		getContext().parent().tell(new StatusMessage(), getSelf());
	}

	static abstract class ProtocolMessage extends DataMessage {

	}

	public static class CyclonNodeList extends ProtocolMessage {

		final boolean isRequest;
		final List<Neighbor> nodes;

		public CyclonNodeList(List<Neighbor> nodes, boolean isRequest) {
			this.nodes = nodes;
			this.isRequest = isRequest;
		}
	}

	public static class CyclonJoin extends ProtocolMessage {

	}
}