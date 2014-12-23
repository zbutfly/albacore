package net.butfly.albacore.support;

public abstract class BaseObjectSupport<T extends ObjectSupport<T>> implements ObjectSupport<T> {
	private static final long serialVersionUID = -2877820654223090498L;

	@Override
	public int compareTo(T object) {
		return this.equals(object) ? 0 : 1;
	}

	public Object shadowClone() {
		try {
			return super.clone();
		} catch (CloneNotSupportedException e) {
			return this;
		}
	}
}
