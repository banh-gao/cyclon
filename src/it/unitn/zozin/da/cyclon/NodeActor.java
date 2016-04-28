package it.unitn.zozin.da.cyclon;

import it.unitn.zozin.da.cyclon.GraphActor.ControlMessage;
import it.unitn.zozin.da.cyclon.GraphActor.ControlMessage.StatusMessage;
import it.unitn.zozin.da.cyclon.NeighborsCache.Neighbor;
import it.unitn.zozin.da.cyclon.task.MeasureMessage;
import it.unitn.zozin.da.cyclon.task.MeasureMessage.ReportMessage;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionStage;
import akka.actor.ActorIdentity;
import akka.actor.ActorRef;
import akka.actor.Identify;
import akka.actor.UntypedActor;
import akka.pattern.AskTimeoutException;
import akka.pattern.PatternsCS;

public class NodeActor extends UntypedActor {

	public static final int CACHE_SIZE = 5;
	public static final int SHUFFLE_LENGTH = 5;

	public static final int REQUEST_TIMEOUT = 1000;

	// TODO: state for testing using simple boot
	private int pendingBootNeighbors;
	// ////////////////////////////////////////////

	private final NeighborsCache cache;
	private Neighbor selfAddress;
	private int round = 0;

	public NodeActor() {
		this.cache = new NeighborsCache(CACHE_SIZE);
		this.pendingBootNeighbors = CACHE_SIZE;
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
			// TODO: Implement cyclon join protocol
			if (pendingBootNeighbors == 0)
				return; // Boot completed: ignore other identities

			ActorRef remoteActor = ((ActorIdentity) message).getRef();

			// Ignore self identity
			if (remoteActor.equals(getSelf()))
				return;

			this.cache.addNeighbors(Collections.singletonList(new Neighbor(0, remoteActor)), Collections.emptyList());
			pendingBootNeighbors--;
			if (pendingBootNeighbors == 0) {
				sendCyclonRequest();
			}
		}
	}

	public void startProtocolRound() {
		if (pendingBootNeighbors > 0) {
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

		List<Neighbor> reqNodes = cache.getRandomNeighbors(SHUFFLE_LENGTH);

		Neighbor dest = cache.getOldestNeighbor();

		// Replace destination entry with fresh local node address
		reqNodes.remove(dest);

		reqNodes.add(selfAddress);

		System.out.println(round + " REQ " + getSelf().path().name() + " TO " + dest.address.path().name() + ": " + cache);
		CompletionStage<Object> req = PatternsCS.ask(dest.address, new CyclonNodeList(reqNodes, true), REQUEST_TIMEOUT);
		req.thenAccept((ans) -> {
			processCyclonAnswer((CyclonNodeList) ans, reqNodes);
			sendRoundCompletedStatus();
		});

		req.exceptionally((t) -> {
			if (t instanceof AskTimeoutException) {
				System.out.println("Neighbor " + dest + " removed for time out!");
				cache.remove(dest);
			} else {
				t.printStackTrace();
			}
			sendRoundCompletedStatus();

			return false;
		});
	}

	private void sendRoundCompletedStatus() {
		getContext().parent().tell(new StatusMessage(), getSelf());
	}

	private void processProtocolMessage(ProtocolMessage message) {
		processCyclonRequest((CyclonNodeList) message);
	}

	private void processCyclonRequest(CyclonNodeList message) {
		while (message.nodes.remove(selfAddress));
		cache.addNeighbors(message.nodes, Collections.emptyList());
		sendCyclonAnswer();
	}

	private void sendCyclonAnswer() {
		List<Neighbor> nodes = cache.getRandomNeighbors(SHUFFLE_LENGTH);
		getSender().tell(new CyclonNodeList(nodes, false), getSelf());
	}

	private void processCyclonAnswer(CyclonNodeList answer, List<Neighbor> reqNodes) {
		answer.nodes.remove(selfAddress);
		cache.addNeighbors(answer.nodes, reqNodes);
		System.out.println(round + " ANS " + getSelf().path().name() + ": " + cache);
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