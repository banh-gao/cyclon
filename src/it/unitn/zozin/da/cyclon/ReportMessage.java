package it.unitn.zozin.da.cyclon;

public class ReportMessage {

	enum Type {
		NODE_ADDED, NODE_REMOVED, TASK_STARTED, TASK_ENDED
	}

	final Type type;

	final Object value;

	public ReportMessage(Type type, Object value) {
		this.type = type;
		this.value = value;
	}
}
