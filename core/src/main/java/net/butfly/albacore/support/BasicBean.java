package net.butfly.albacore.support;

public abstract class BasicBean<T extends Beanable<T>> implements Beanable<T> {
	private static final long serialVersionUID = -2877820654223090498L;

	public Object shadowClone() {
		try {
			return super.clone();
		} catch (CloneNotSupportedException e) {
			return this;
		}
	}

	@Override
	public int compareTo(T o) {
		throw new UnsupportedOperationException();
	}
}
