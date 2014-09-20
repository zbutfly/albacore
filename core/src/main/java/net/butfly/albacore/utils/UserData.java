package net.butfly.albacore.utils;

public class UserData {
	private String userId;
	private String ip;

	public UserData() {}

	public UserData(String userId, String ip) {
		this.userId = userId;
		this.ip = ip;
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}
}
