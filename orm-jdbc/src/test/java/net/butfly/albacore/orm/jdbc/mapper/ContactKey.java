package net.butfly.albacore.orm.jdbc.mapper;

import net.butfly.albacore.entity.Key;

public class ContactKey extends Key<ContactKey> {
	private static final long serialVersionUID = 4480350096757054202L;

	public enum Type {
		MOBILE, EMAIL, ADDRESS, QQ_NUM, PERSONAL_URL
	}

	String userId;
	Type type;

	public ContactKey() {
		super();
	}

	public ContactKey(String userId, Type type) {
		super();
		this.userId = userId;
		this.type = type;
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public Type getType() {
		return type;
	}

	public void setType(Type type) {
		this.type = type;
	}
}
