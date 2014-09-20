package net.butfly.albacore.entity;

import java.io.Serializable;
import java.util.Date;

import net.butfly.albacore.support.entity.SteerableSupport;

public abstract class SteerableEntity<K extends Serializable> extends BasicEntity<K> implements SteerableSupport<K> {
	private static final long serialVersionUID = 1L;
	protected EntityStub<K> createStub;
	protected EntityStub<K> deleteStub;
	protected EntityStub<K> updateStub;

	public SteerableEntity() {
		this.createStub = new EntityStub<K>();
		this.createStub = new EntityStub<K>();
		this.createStub = new EntityStub<K>();
	}

	@Override
	public K getCreator() {
		return this.createStub.id;
	}

	@Override
	public void setCreator(K userID) {
		this.createStub.id = userID;
	}

	@Override
	public Date getCreated() {
		return this.createStub.getTime();
	}

	@Override
	public void setCreated(Date time) {
		this.createStub.setTime(time);
	}

	@Override
	public String getCreateFrom() {
		return this.createStub.getIp();
	}

	@Override
	public void setCreateFrom(String ip) {
		this.createStub.setIp(ip);
	}

	@Override
	public K getUpdator() {
		return this.updateStub.id;
	}

	@Override
	public void setUpdator(K userID) {
		this.updateStub.id = userID;
	}

	@Override
	public Date getUpdated() {
		return this.updateStub.getTime();
	}

	@Override
	public void setUpdated(Date time) {
		this.updateStub.setTime(time);
	}

	@Override
	public String getUpdateFrom() {
		return this.updateStub.getIp();
	}

	@Override
	public void setUpdateFrom(String ip) {
		this.updateStub.setIp(ip);
	}

	@Override
	public K getDeletor() {
		return this.updateStub.id;
	}

	@Override
	public void setDeletor(K userID) {
		this.deleteStub.id = userID;
	}

	@Override
	public Date getDeleted() {
		return this.deleteStub.getTime();
	}

	@Override
	public void setDeleted(Date time) {
		this.deleteStub.setTime(time);
	}

	@Override
	public String getDeleteFrom() {
		return this.updateStub.getIp();
	}

	@Override
	public void setDeleteFrom(String ip) {
		this.deleteStub.setIp(ip);
	}
}
