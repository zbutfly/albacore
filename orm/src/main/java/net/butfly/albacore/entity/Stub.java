package net.butfly.albacore.entity;

import java.io.Serializable;
import java.util.Date;

public final class Stub<K extends Serializable> extends Entity<K> {
	private static final long serialVersionUID = -5066977312870476308L;
	private K userID;
	private String ip;
	private Date time;

	public Stub() {}

	public Stub(K id, Date time, String ip) {
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
