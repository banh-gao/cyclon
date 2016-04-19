package it.unitn.zozin.da.cyclon;

import java.util.function.Function;

public class TaskMessage {

	enum Type {
		CYCLON_ROUND
	}

	final Type type;

	final Function<Node, ?> map;

	public <V> TaskMessage(Type type, Function<Node, V> map) {
		this.type = type;
		this.map = map;
	}
}
