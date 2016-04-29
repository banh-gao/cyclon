package it.unitn.zozin.da.cyclon;

import it.unitn.zozin.da.cyclon.GraphActor.StartMeasureMessage;
import it.unitn.zozin.da.cyclon.GraphActor.StartRoundMessage;
import it.unitn.zozin.da.cyclon.Message.StatusMessage;
import it.unitn.zozin.da.cyclon.Message.TaskMessage;
import it.unitn.zozin.da.cyclon.NeighborsCache.Neighbor;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import akka.actor.ActorIdentity;
import akka.actor.ActorRef;
import akka.actor.Identify;
import akka.actor.UntypedActor;

public class NodeActor extends UntypedActor {

	private static final MessageMatcher<NodeActor> MATCHER = MessageMatcher.getInstance();

	private static final BiConsumer<TaskMessage, NodeActor> PROCESS_TASK = (TaskMessage message, NodeActor n) -> {
		message.execute(n);
	};

	// TODO: Implement cyclon join protocol (Here boot is completed when we have
	// a link with one neighbor)
	private static final BiConsumer<ActorIdentity, NodeActor> PROCESS_JOIN_ANS = (ActorIdentity id, NodeActor n) -> {
		ActorRef remoteActor = id.getRef();

		// Ignore self identity
		if (remoteActor.equals(n.getSelf()))
			return;

		if (n.bootCompleted)
			return; // Boot completed: ignore other identities

		if (n.cache.size() == 0) {
			n.cache.updateNeighbors(Collections.singletonList(new Neighbor(0, remoteActor)), Collections.emptySet());
		}

		n.bootCompleted = true;
		n.sendCyclonRequest();
	};

	private static final BiConsumer<CyclonNodeList, NodeActor> PROCESS_NODELIST = (CyclonNodeList nodeList, NodeActor n) -> {
		if (nodeList.isRequest)
			n.processCyclonRequest(nodeList);
		else
			n.processCyclonAnswer(nodeList);
	};

	static {
		MATCHER.set(StartRoundMessage.class, PROCESS_TASK);
		MATCHER.set(StartMeasureMessage.class, PROCESS_TASK);
		MATCHER.set(ActorIdentity.class, PROCESS_JOIN_ANS);
		MATCHER.set(CyclonNodeList.class, PROCESS_NODELIST);
	}

	private final int shuffleLength;

	private int round = 0;
	private boolean bootCompleted = false;

	final NeighborsCache cache;

	private Neighbor selfAddress;
	private Set<Neighbor> replaceableEntries;

	public NodeActor(int cacheSize, int shuffleLength) {
		cache = new NeighborsCache(cacheSize);
		this.shuffleLength = shuffleLength;
		replaceableEntries = new HashSet<Neighbor>(shuffleLength);
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
		if (cache.size() == 0) {
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
		List<Neighbor> requestNodes = cache.getRandomNeighbors(shuffleLength - 1, dest);

		// Cache entries that can be replaced when the answer to this request is
		// received
		replaceableEntries.clear();
		replaceableEntries.addAll(requestNodes);
		replaceableEntries.add(dest);

		// Add fresh local node address
		requestNodes.add(selfAddress);

		dest.address.tell(new CyclonNodeList(requestNodes, true), getSelf());

		// TODO: end round also on answer timeout
	}

	private void processCyclonRequest(CyclonNodeList req) {
		// Remove itself (if present)
		req.nodes.remove(selfAddress);

		// Prepare answer and save received nodes in cache
		List<Neighbor> ansNodes = cache.getRandomNeighbors(shuffleLength);

		cache.updateNeighbors(req.nodes, new HashSet<NeighborsCache.Neighbor>(ansNodes));

		// Handle special case in which a request arrives while the local node
		// is waiting for an answer to its own request. In this case the entries
		// that can can be replaced when the answer arrives correspond to the
		// new entries just stored in the cache with this request.
		replaceableEntries.removeAll(ansNodes);
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