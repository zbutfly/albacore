package net.butfly.demo.albacore.mapper;

import net.butfly.albacore.entity.Key;

public class PermitKey extends Key<PermitKey> {
	private static final long serialVersionUID = -900070990404832052L;
	private String userId;
	private String functionId;

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public String getFunctionId() {
		return functionId;
	}

	public void setFunctionId(String functionId) {
		this.functionId = functionId;
	}
}
