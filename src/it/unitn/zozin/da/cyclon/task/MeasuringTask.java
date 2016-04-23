package it.unitn.zozin.da.cyclon.task;

import it.unitn.zozin.da.cyclon.NodeActor;

public class MeasuringTask implements TaskMessage<it.unitn.zozin.da.cyclon.task.MeasuringTask.Result, it.unitn.zozin.da.cyclon.task.MeasuringTask.Result> {

	public static class Result {

	}

	@Override
	public Result map(NodeActor n) {
		System.out.println("Measuring at node " + n);
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Result reduce(Result value, Result partial) {
		if (partial == null)
			partial = new Result();
		return partial;
	}

}
