package net.butfly.albacore.dao;

import java.io.Serializable;

import net.butfly.albacore.dbo.criteria.Criteria;
import net.butfly.albacore.entity.BasicEntity;

public interface LogicalDAO extends EntityDAO {
	<K extends Serializable, E extends BasicEntity<K>> int deleteLogical(Class<E> entityClass, Criteria criteria);
}
