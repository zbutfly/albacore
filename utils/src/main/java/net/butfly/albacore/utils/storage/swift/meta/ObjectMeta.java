package net.butfly.albacore.utils.storage.swift.meta;

public class ObjectMeta extends MetaBase {
	private static final long serialVersionUID = 2240784860603433053L;
	private String hash;
	private String contentType;
	private String lastModified;

	public String getHash() {
		return hash;
	}

	public void setHash(String hash) {
		this.hash = hash;
	}

	public String getContentType() {
		return contentType;
	}

	public void setContentType(String contentType) {
		this.contentType = contentType;
	}

	public String getLastModified() {
		return lastModified;
	}

	public void setLastModified(String lastModified) {
		this.lastModified = lastModified;
	}

}
