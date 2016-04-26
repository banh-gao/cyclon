package it.unitn.zozin.da.cyclon;

import it.unitn.zozin.da.cyclon.NeighborsCache.Neighbor;
import it.unitn.zozin.da.cyclon.task.TaskMessage;
import it.unitn.zozin.da.cyclon.task.TaskMessage.ReportMessage;
import java.util.Collections;
import akka.actor.ActorIdentity;
import akka.actor.ActorRef;
import akka.actor.Identify;
import akka.actor.UntypedActor;

public class NodeActor extends UntypedActor {

	// TODO: state for testing using simple boot
	private int pendingBootNeighbors;
	// ////////////////////////////////////////////

	private NeighborsCache cache;

	public NodeActor(int cacheSize, int shuffleLength) {
		this.cache = new NeighborsCache(cacheSize, shuffleLength);
		this.pendingBootNeighbors = cacheSize;
	}

	@Override
	public void onReceive(Object message) throws Exception {
		if (message instanceof TaskMessage)
			getSender().tell(new ReportMessage(((TaskMessage) message).map(this)), getSelf());
		else if (message instanceof CyclonMessage)
			processCyclonMsg((CyclonMessage) message);
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

	public void sendCyclonRequest() {
		if (pendingBootNeighbors > 0) {
			performBoot();
			return;
		}

		System.out.println("SENDING CYCLON REQUEST TO " + cache.getOldestNeighbor());
		System.out.println("SHUFFLED: " + cache.getRandomNeighbors());
		// TODO: send request to neighbor
	}

	private void performBoot() {
		System.out.println("BOOTING NODE");
		getContext().actorSelection("../*").tell(new Identify(null), getSelf());

		// TODO: implement cyclon join protocol
	}

	private void processCyclonMsg(CyclonMessage message) {
		// TODO Auto-generated method stub
	}

	public static class CyclonMessage {

	}

}