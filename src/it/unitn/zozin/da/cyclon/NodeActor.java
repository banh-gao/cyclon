package it.unitn.zozin.da.cyclon;

import it.unitn.zozin.da.cyclon.NeighborsCache.Neighbor;
import it.unitn.zozin.da.cyclon.task.TaskMessage;
import it.unitn.zozin.da.cyclon.task.TaskMessage.ReportMessage;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import akka.actor.UntypedActor;

public class NodeActor extends UntypedActor {

	private boolean isBootCompleted = false;
	private NeighborsCache cache;

	public NodeActor(int cacheSize, int shuffleLength) {
		this.cache = new NeighborsCache(cacheSize, shuffleLength);
	}

	@Override
	public void onReceive(Object message) throws Exception {
		if (message instanceof TaskMessage)
			getSender().tell(new ReportMessage(((TaskMessage) message).map(this)), getSelf());
		else if (message instanceof CyclonMessage)
			processCyclonMsg((CyclonMessage) message);
	}

	public void sendCyclonRequest() {
		if (!isBootCompleted)
			performBoot();

		List<Neighbor> req = new ArrayList<Neighbor>();

		req.add(new Neighbor(0, "n1"));
		req.add(new Neighbor(0, "n2"));
		// req.add(new Neighbor(0, "n3"));

		System.out.println(cache);
		cache.addNeighbors(req, Collections.emptyList());

		System.out.println(cache);

		cache.increaseNeighborsAge();
		System.out.println("REQ SENT: " + cache);

		List<Neighbor> ans = new ArrayList<Neighbor>();

		ans.add(new Neighbor(0, "n5"));
		ans.add(new Neighbor(0, "n6"));

		System.out.println("ANS: " + ans);

		cache.addNeighbors(ans, req);
		System.out.println("MERGE: " + cache);

		System.out.println(cache.getOldestNeighbor());
		// TODO: send request to neighbor
	}

	private void performBoot() {
		getContext().actorSelection("../*");
		// TODO: implement boot process
	}

	private void processCyclonMsg(CyclonMessage message) {
		// TODO Auto-generated method stub
	}

	public static class CyclonMessage {

	}

}