package it.unitn.zozin.da.cyclon;

import it.unitn.zozin.da.cyclon.GraphActor.StartRoundMessage;
import it.unitn.zozin.da.cyclon.Message.StatusMessage;
import it.unitn.zozin.da.cyclon.Message.TaskMessage;
import it.unitn.zozin.da.cyclon.NeighborsCache.Neighbor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.BiConsumer;
import akka.actor.ActorIdentity;
import akka.actor.ActorRef;
import akka.actor.Identify;
import akka.actor.UntypedActor;

public class NodeActor extends UntypedActor {

	public static final int CACHE_SIZE = 5;
	public static final int SHUFFLE_LENGTH = 1;

	private static final MessageMatcher<NodeActor> MATCHER = MessageMatcher.getInstance();

	private static final BiConsumer<TaskMessage, NodeActor> PROCESS_TASK = (TaskMessage message, NodeActor n) -> {
		message.execute(n);
	};

	// TODO: Implement cyclon join protocol
	private static final BiConsumer<ActorIdentity, NodeActor> PROCESS_BOOT_ANS = (ActorIdentity id, NodeActor n) -> {
		ActorRef remoteActor = id.getRef();

		// Ignore self identity
		if (remoteActor.equals(n.getSelf()))
			return;

		if (n.cache.freeSlots() == 0)
			return; // Boot completed: ignore other identities

		n.cache.updateNeighbors(Collections.singletonList(new Neighbor(0, remoteActor)), Collections.emptyList());

		if (n.cache.freeSlots() == 0) {
			n.sendCyclonRequest();
		}
	};

	private static final BiConsumer<CyclonNodeList, NodeActor> PROCESS_NODELIST = (CyclonNodeList nodeList, NodeActor n) -> {
		if (nodeList.isRequest)
			n.processCyclonRequest(nodeList);
		else
			n.processCyclonAnswer(nodeList);
	};

	static {
		MATCHER.set(StartRoundMessage.class, PROCESS_TASK);
		MATCHER.set(ActorIdentity.class, PROCESS_BOOT_ANS);
		MATCHER.set(CyclonNodeList.class, PROCESS_NODELIST);
	}

	private Neighbor selfAddress;
	private int round = 0;

	private final NeighborsCache cache;
	private List<Neighbor> replaceableEntries;

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
		MATCHER.process(message, this);
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

	public static class CyclonNodeList {

		final boolean isRequest;
		final List<Neighbor> nodes;

		public CyclonNodeList(List<Neighbor> nodes, boolean isRequest) {
			this.nodes = nodes;
			this.isRequest = isRequest;
		}
	}

	public static class CyclonJoin {

	}
}