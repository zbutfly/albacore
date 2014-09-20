package net.butfly.albacore.support.entity;

import java.io.Serializable;
import java.util.Date;

public interface UpdateSupport<K extends Serializable> extends Serializable {
	K getUpdator();

	void setUpdator(K userID);

	Date getUpdated();

	void setUpdated(Date time);

	String getUpdateFrom();

	void setUpdateFrom(String ip);
}
