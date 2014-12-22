package net.butfly.albacore.dao;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.List;

import net.butfly.albacore.dbo.criteria.Criteria;
import net.butfly.albacore.dbo.criteria.Page;
import net.butfly.albacore.entity.Entity;
import net.butfly.albacore.exception.SystemException;

import org.mybatis.spring.SqlSessionTemplate;

public abstract class EntityDAOBase extends DAOBase implements EntityDAO {
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
		this.batchTemplate.insert(this.getSqlId(DAOContext.INSERT_STATMENT_ID + entity.getClass().getSimpleName()), entity);
		return entity.getId();
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
		this.batchTemplate.delete(this.getSqlId(DAOContext.DELETE_STATMENT_ID + entityClass.getSimpleName()), key);
		return e;
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

	@SuppressWarnings("unchecked")
	@Override
	public <K extends Serializable, E extends Entity<K>> E update(E entity) {
		E existed = (E) this.select(entity.getClass(), entity.getId());
		if (null != existed)
			this.batchTemplate.update(this.getSqlId(DAOContext.UPDATE_STATMENT_ID + entity.getClass().getSimpleName()), entity);
		return existed;
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
				this.getSqlId(DAOContext.DELETE_STATMENT_ID + entityClass.getSimpleName() + "ByCriteria"), criteria);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <K extends Serializable, E extends Entity<K>> int update(E entity, Criteria criteria) {
		int c = 0;
		Class<E> entityClass = (Class<E>) entity.getClass();
		for (K k : this.selectKeys(entityClass, criteria, Page.ALL_RECORD)) {
			entity.setId(k);
			if (null != this.update(entity)) c++;
		}
		return c;
	}

	@Override
	public <K extends Serializable, E extends Entity<K>> K[] selectKeys(Class<E> entityClass, Criteria criteria, Page page) {
		if (null == page) throw new SystemException("Query must be limited by page.");
		if (page.getTotal() < 0) { // dirty page
			Object total = this.batchTemplate.selectOne(
					this.getSqlId(DAOContext.COUNT_STATMENT_ID + entityClass.getSimpleName() + "ByCriteria"), criteria);
			page.setTotal(null != total ? ((Number) total).intValue() : 0);
		}
		List<K> list = this.batchTemplate.selectList(
				this.getSqlId(DAOContext.SELECT_STATMENT_ID + entityClass.getSimpleName() + "ByCriteria"), criteria,
				page.toRowBounds());

		return list.toArray(Entity.getKeyBuffer(entityClass, list.size()));
	}

	@Override
	public <K extends Serializable, E extends Entity<K>> E[] select(Class<E> entityClass, Criteria criteria, Page page) {
		return this.select(entityClass, this.selectKeys(entityClass, criteria, page));
	}

	@Override
	public <K extends Serializable, E extends Entity<K>> int count(Class<E> entityClass, Criteria criteria) {
		Object r = this.batchTemplate.selectOne(
				this.getSqlId(DAOContext.COUNT_STATMENT_ID + entityClass.getSimpleName() + "ByCriteria"), criteria);
		return null == r ? 0 : ((Number) r).intValue();
	}

	public void setTemplate(SqlSessionTemplate template) {
		this.template = template;
	}

	public void setBatchTemplate(SqlSessionTemplate batchTemplate) {
		this.batchTemplate = batchTemplate;
	}
}
