package it.unitn.zozin.da.cyclon.task;

import it.unitn.zozin.da.cyclon.NodeActor;
import it.unitn.zozin.da.cyclon.task.CyclonRoundTask.ResultC;

public class CyclonRoundTask implements TaskMessage<String, ResultC> {

	@Override
	public String map(NodeActor n) {
		System.out.println("Executing cyclon round on node " + n);
		return n.toString();
	}

	@Override
	public ResultC reduce(String value, ResultC partial) {
		if (partial == null)
			partial = new ResultC();

		partial.nodes += 1;
		partial.result += value;
		return partial;
	}

	public static class ResultC {

		int nodes = 0;
		String result = "";

		@Override
		public String toString() {
			return "Result [nodes=" + nodes + ", result=" + result + "]";
		}

	}
}