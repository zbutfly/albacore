package net.butfly.albacore.dao;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.List;

import net.butfly.albacore.base.BizUnitBase;
import net.butfly.albacore.dbo.criteria.Criteria;
import net.butfly.albacore.dbo.criteria.Page;
import net.butfly.albacore.entity.Entity;
import net.butfly.albacore.exception.SystemException;

import org.mybatis.spring.SqlSessionTemplate;

public abstract class EntityDAOBase extends BizUnitBase implements EntityDAO {
	private static final long serialVersionUID = -1599466753909389837L;
	protected SqlSessionTemplate template, batchTemplate;
	protected String namespace;

	public EntityDAOBase() {
		this.namespace = this.getClass().getName().replaceAll("(?i)dao", "").replaceAll("(?i)impl", ".") + ".";
		this.namespace = this.namespace.replaceAll("\\.+", ".");
	}

	protected <E> E entityInstance(Class<E> entityClass) {
		try {
			return entityClass.newInstance();
		} catch (InstantiationException e) {
			throw new SystemException("", e);
		} catch (IllegalAccessException e) {
			throw new SystemException("", e);
		}
	}

	/**
	 * Namespace mapping
	 * 
	 * @param sqlId
	 * @return
	 */
	protected String getSqlId(String sqlId) {
		return this.namespace + sqlId;
	}

	/*********************************************************************************************/

	@Override
	public <K extends Serializable, E extends Entity<K>> K insert(E entity) {
		if (null == entity) return null;
		if (this.batchTemplate.insert(this.getSqlId(DAOContext.INSERT_STATMENT_ID + entity.getClass().getSimpleName()), entity) == 1) return entity
				.getId();
		else return null;
	}

	@Override
	public <K extends Serializable, E extends Entity<K>> int insert(E[] entities) {
		if (null == entities || entities.length == 0) return 0;
		return this.batchTemplate.insert(
				this.getSqlId(DAOContext.INSERT_STATMENT_ID + entities.getClass().getComponentType().getSimpleName() + "List"),
				entities);
	}

	@Override
	public <K extends Serializable, E extends Entity<K>> E delete(Class<E> entityClass, K key) {
		E e = this.select(entityClass, key);
		if (null == e) return null;
		return this.batchTemplate.delete(this.getSqlId(DAOContext.DELETE_STATMENT_ID + entityClass.getSimpleName()), key) > 0 ? e
				: null;
	}

	// TODO: batch
	@Override
	public <K extends Serializable, E extends Entity<K>> int delete(Class<E> entityClass, K[] keys) {
		int c = 0;
		for (K key : keys)
			c += this.batchTemplate.delete(this.getSqlId(DAOContext.DELETE_STATMENT_ID + entityClass.getSimpleName()), key);
		return c;
		// return
		// this.batchTemplate.delete(this.getSqlId(DAOContext.DELETE_STATMENT_ID
		// + entityClass.getSimpleName()) + "List",
		// keys);
	}

	@Override
	public <K extends Serializable, E extends Entity<K>> boolean update(E entity) {
		if (null == entity) return false;
		return this.batchTemplate.update(this.getSqlId(DAOContext.UPDATE_STATMENT_ID + entity.getClass().getSimpleName()),
				entity) > 0;
	}

	@Override
	public <K extends Serializable, E extends Entity<K>> E select(Class<E> entityClass, K key) {
		return this.batchTemplate.selectOne(this.getSqlId(DAOContext.SELECT_STATMENT_ID + entityClass.getSimpleName()), key);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <K extends Serializable, E extends Entity<K>> E[] select(Class<E> entityClass, K[] keys) {
		E[] entities = (E[]) Array.newInstance(entityClass, keys.length);
		for (int i = 0; i < keys.length; i++)
			entities[i] = this.select(entityClass, keys[i]);
		return entities;
	}

	/*********************************************************************************************/

	@Override
	public <K extends Serializable, E extends Entity<K>> int delete(Class<E> entityClass, Criteria criteria) {
		return this.batchTemplate.delete(
				this.getSqlId(DAOContext.DELETE_STATMENT_ID + entityClass.getSimpleName() + "ByCriteria"),
				criteria.getParameters());
	}

	@SuppressWarnings("unchecked")
	@Override
	public <K extends Serializable, E extends Entity<K>> int update(E entity, Criteria criteria) {
		int c = 0;
		Class<E> entityClass = (Class<E>) entity.getClass();
		K[] keys = this.getKeyPage(entityClass, DAOContext.COUNT_STATMENT_ID, DAOContext.SELECT_STATMENT_ID,
				criteria.getParameters(), Page.ALL_RECORD);
		for (K k : keys) {
			entity.setId(k);
			if (this.update(entity)) c++;
		}
		return c;
	}

	@Override
	public <K extends Serializable, E extends Entity<K>> E[] select(Class<E> entityClass, Criteria criteria, Page page) {
		K[] keys = this.getKeyPage(entityClass, DAOContext.COUNT_STATMENT_ID, DAOContext.SELECT_STATMENT_ID,
				criteria.getParameters(), page);
		return this.select(entityClass, keys);
	}

	@Override
	public <K extends Serializable, E extends Entity<K>> int count(Class<E> entityClass, Criteria criteria) {
		Object r = this.batchTemplate.selectOne(
				this.getSqlId(DAOContext.COUNT_STATMENT_ID + entityClass.getSimpleName() + "ByCriteria"),
				criteria.getParameters());
		return null == r ? 0 : ((Number) r).intValue();
	}

	protected <K extends Serializable, E extends Entity<K>> E[] getEntityPage(Class<E> entityClass, String countSqlId,
			String selectSqlId, Object arg, Page page) {
		if (null == page) throw new SystemException("Query must be limited by page.");
		if (page.getTotal() < 0) {
			Object total = this.batchTemplate.selectOne(this.getSqlId(countSqlId), arg);
			page.setTotal(null != total ? ((Number) total).intValue() : 0);
		}
		List<K> list = this.batchTemplate.selectList(this.getSqlId(selectSqlId), arg, page.toRowBounds());
		K[] keys = list.toArray(Entity.getKeyBuffer(entityClass, list.size()));
		return this.select(entityClass, keys);
	}

	protected <K extends Serializable, E extends Entity<K>> K[] getKeyPage(Class<E> entityClass, String countSqlId,
			String selectSqlId, Object arg, Page page) {
		if (null == page) throw new SystemException("Query must be limited by page.");
		if (page.getTotal() < 0) {
			Object total = this.batchTemplate.selectOne(this.getSqlId(countSqlId + entityClass.getSimpleName() + "ByCriteria"),
					arg);
			page.setTotal(null != total ? ((Number) total).intValue() : 0);
		}
		List<K> list = this.batchTemplate.selectList(this.getSqlId(selectSqlId + entityClass.getSimpleName() + "ByCriteria"),
				arg, page.toRowBounds());
		return list.toArray(Entity.getKeyBuffer(entityClass, list.size()));
	}

	public void setTemplate(SqlSessionTemplate template) {
		this.template = template;
	}

	public void setBatchTemplate(SqlSessionTemplate batchTemplate) {
		this.batchTemplate = batchTemplate;
	}
}
