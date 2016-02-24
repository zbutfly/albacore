package net.butfly.albacore.entity;

import java.io.Serializable;

import net.butfly.albacore.support.Bean;

abstract class AbstractEntityBase<K extends Serializable> extends Bean<AbstractEntity<K>> implements AbstractEntity<K> {
	private static final long serialVersionUID = -3946059501995247667L;

	@SuppressWarnings("unchecked")
	public <KK extends Serializable> Class<KK> getKeyClass() {
		return AbstractEntity.getKeyClass((Class<? extends AbstractEntity<?>>) getClass());
	}
}
