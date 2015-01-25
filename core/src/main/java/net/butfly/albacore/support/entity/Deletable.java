package net.butfly.albacore.support.entity;

import java.io.Serializable;
import java.util.Date;

public interface Deletable<K extends Serializable> extends Serializable {
	K getDeletor();

	void setDeletor(K userID);

	Date getDeleted();

	void setDeleted(Date time);

	String getDeleteFrom();

	void setDeleteFrom(String ip);
}
