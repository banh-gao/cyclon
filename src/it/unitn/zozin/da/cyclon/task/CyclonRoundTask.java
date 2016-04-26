package it.unitn.zozin.da.cyclon.task;

import it.unitn.zozin.da.cyclon.NodeActor;

public class CyclonRoundTask implements TaskMessage<String, Object> {

	@Override
	public String map(NodeActor n) {
		System.out.println("Executing cyclon round on node " + n);
		n.sendCyclonRequest();
		return n.toString();
	}

	@Override
	public Object reduce(String value, Object partial) {
		return null;
	}
}