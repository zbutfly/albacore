package net.butfly.albacore.utils.storage.swift.meta;

public class ContainerMeta extends MetaBase {
	private static final long serialVersionUID = -8971294576637892039L;
	private int count;

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}
}