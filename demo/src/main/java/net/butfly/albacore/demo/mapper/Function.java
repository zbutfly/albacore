package net.butfly.albacore.demo.mapper;

import net.butfly.albacore.entity.Entity;

public class Function extends Entity<String> {
	private static final long serialVersionUID = -2591542774945143828L;

	private String name;
	private String parentId;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getParentId() {
		return parentId;
	}

	public void setParentId(String parentId) {
		this.parentId = parentId;
	}
}
