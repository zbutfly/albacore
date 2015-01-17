package net.butfly.albacore.demo.mapper;

import net.butfly.albacore.entity.Entity;

public class User extends Entity<String> {
	private static final long serialVersionUID = 3442235780368825236L;
	private String name;
	private String loginName;
	private String password;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getLoginName() {
		return loginName;
	}

	public void setLoginName(String loginName) {
		this.loginName = loginName;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}
}
