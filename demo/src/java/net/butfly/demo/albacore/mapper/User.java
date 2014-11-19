package net.butfly.demo.albacore.mapper;

import net.butfly.albacore.entity.BasicEntity;

public class User extends BasicEntity<String> {
	private static final long serialVersionUID = 3442235780368825236L;
	private String name;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
}
