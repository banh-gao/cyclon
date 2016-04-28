package it.unitn.zozin.da.cyclon.task;

import it.unitn.zozin.da.cyclon.NodeActor;

public interface MeasureMessage<V, R> {

	public V map(NodeActor n);

	public R reduce(V value, R partial);

	public static class ReportMessage {

		public final Object value;

		public ReportMessage(Object value) {
			this.value = value;
		}
	}
}
