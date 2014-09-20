package net.butfly.albacore.utils.security;

import java.io.Serializable;

public final class KeyStore implements Serializable {
	private static final long serialVersionUID = 6755565307129141407L;
	private String path;
	private String password;
	private String managerPassword;

	public KeyStore(String path, String password, String managerPassword) {
		super();
		this.path = path;
		this.password = password;
		this.managerPassword = managerPassword;
	}

	public String getPath() {
		return path;
	}

	public String getPassword() {
		return password;
	}

	public String getManagerPassword() {
		return managerPassword;
	}

}
