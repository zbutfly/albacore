package net.butfly.albacore.orm.jdbc.mapper;

import net.butfly.albacore.entity.Entity;

public class User extends Entity<String> {
	private static final long serialVersionUID = 7935499804081783432L;
	String name;
	Boolean gender;
	String password;

	public User() {
		super();
	}

	public User(String name, Boolean gender, String password) {
		super();
		this.name = name;
		this.gender = gender;
		this.password = password;
	}

	@Override
	public String getId() {
		return super.getId();
	}

	@Override
	public void setId(String id) {
		super.setId(id);
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public Boolean getGender() {
		return gender;
	}

	public void setGender(Boolean gender) {
		this.gender = gender;
	}
}
