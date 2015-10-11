package net.butfly.albacore.demo.neo4j.mapper;

import net.butfly.albacore.entity.Entity;

public class Person extends Entity<String> {
	private static final long serialVersionUID = 5278067469687037666L;

	private String name;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
}
