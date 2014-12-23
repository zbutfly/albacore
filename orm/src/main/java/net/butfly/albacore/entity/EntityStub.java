package net.butfly.albacore.entity;

import java.io.Serializable;
import java.util.Date;

public final class EntityStub<K extends Serializable> extends SimpleEntity<K> {
	private static final long serialVersionUID = -5066977312870476308L;
	private K userID;
	private String ip;
	private Date time;

	public EntityStub() {}

	public EntityStub(K id, Date time, String ip) {
		super();
		this.id = id;
		this.time = time;
		this.ip = ip;
	}

	public K getUserID() {
		return this.userID;
	}

	public void setUserID(K id) {
		this.userID = id;
	}

	public Date getTime() {
		return time;
	}

	public void setTime(Date time) {
		this.time = time;
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}
}
