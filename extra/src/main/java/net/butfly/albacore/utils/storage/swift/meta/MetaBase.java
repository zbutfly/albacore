package net.butfly.albacore.utils.storage.swift.meta;

import java.io.Serializable;

public abstract class MetaBase implements Serializable {
	private static final long serialVersionUID = 7986115687904027461L;
	private String name;
	private long bytes;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public long getBytes() {
		return bytes;
	}

	public void setBytes(long bytes) {
		this.bytes = bytes;
	}
}
