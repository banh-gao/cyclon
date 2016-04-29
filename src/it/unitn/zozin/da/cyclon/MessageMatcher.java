package it.unitn.zozin.da.cyclon;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

public class MessageMatcher<C> {

	private final Map<Class<?>, BiConsumer<?, C>> matcher = new HashMap<Class<?>, BiConsumer<?, C>>();

	public static <C> MessageMatcher<C> getInstance() {
		return new MessageMatcher<C>();
	}

	public <M, A> MessageMatcher<C> set(Class<M> msgType, BiConsumer<? super M, C> func) {
		matcher.put(msgType, func);
		return this;
	}

	@SuppressWarnings({"rawtypes", "unchecked"})
	public void process(Object msg, C context) {
		BiConsumer f = matcher.get(msg.getClass());
		if (f == null)
			throw new IllegalStateException("Unknown message type " + msg.getClass());

		f.accept(msg, context);
	}

}
