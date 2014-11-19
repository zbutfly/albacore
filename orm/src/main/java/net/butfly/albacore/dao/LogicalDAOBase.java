package net.butfly.albacore.dao;

import java.io.Serializable;

import net.butfly.albacore.dbo.criteria.Criteria;
import net.butfly.albacore.entity.BasicEntity;

public abstract class LogicalDAOBase extends EntityDAOBase implements LogicalDAO {
	private static final long serialVersionUID = 7341809666513931557L;

	@Override
	public <K extends Serializable, E extends BasicEntity<K>> int deleteLogical(Class<E> entityClass, Criteria criteria) {
		BasicEntity<K> entity = this.entityInstance(entityClass);
		entity.setDeleted(true);
		criteria.set(DAOContext.ENTITY_DELETED_FIELD, false);
		return super.update(entity, criteria);
	}

	// TODO and so on...
}
