package net.butfly.albacore.support;

public abstract class ObjectSupport<T extends ObjectSupport<T>> extends CloneSupport<T> implements Comparable<T> {
	private static final long serialVersionUID = 1L;

	@Override
	final public int compareTo(T object) {
		return this.equals(object) ? 0 : 1;
	}

	@SuppressWarnings("unchecked")
	public static <T> T shadowClone(T object) {
		return object == null ? null : (T) ((ObjectSupport<?>) object).shadowClone();
	}
}
