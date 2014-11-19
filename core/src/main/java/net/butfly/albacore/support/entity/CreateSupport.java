package net.butfly.albacore.support.entity;

import java.io.Serializable;
import java.util.Date;

public interface CreateSupport<K extends Serializable> extends Serializable {
	K getCreator();

	void setCreator(K userID);

	Date getCreated();

	void setCreated(Date time);

	String getCreateFrom();

	void setCreateFrom(String ip);
}
