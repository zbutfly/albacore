package net.butfly.albacore.dao;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import net.butfly.albacore.dbo.criteria.Criteria;
import net.butfly.albacore.dbo.criteria.OrderedRowBounds;
import net.butfly.albacore.dbo.criteria.Page;
import net.butfly.albacore.entity.AbstractEntity;
import net.butfly.albacore.entity.Key;
import net.butfly.albacore.exception.SystemException;

import org.mybatis.spring.SqlSessionTemplate;

public abstract class EntityBasicDAOBase extends DAOBase implements EntityDAO {
	private static final long serialVersionUID = -2472419986526183766L;
	@SuppressWarnings("unused")
	private SqlSessionTemplate template, batchTemplate;

	public void setTemplate(SqlSessionTemplate template) {
		this.template = template;
	}

	public void setBatchTemplate(SqlSessionTemplate batchTemplate) {
		this.batchTemplate = batchTemplate;
	}

	protected final <K extends Serializable, E extends AbstractEntity<K>> List<K> insert(String sqlId, List<E> entities) {
		List<K> keys = new ArrayList<K>();
		if (null != entities) for (E e : entities)
			if (e.getId() == null || !(e.getId() instanceof Key) || batchTemplate.selectOne(sqlId, e.getId()) == null) {
				batchTemplate.insert(sqlId, e);
				keys.add(e.getId());
			} else entities.remove(e);
		return keys;
	}

	protected final <K extends Serializable, E extends AbstractEntity<K>> List<E> delete(String selectSqlId,
			String deleteSqlId, Class<E> entityClass, List<K> keys) {
		List<E> deleted = new ArrayList<E>();
		for (K key : keys) {
			E e = batchTemplate.selectOne(selectSqlId, key);
			if (null != e) {
				batchTemplate.delete(deleteSqlId, e.getId());
				deleted.add(e);
			}
		}
		return deleted;
	}

	protected final <K extends Serializable, E extends AbstractEntity<K>> List<E> update(String selectSqlId,
			String updateSqlId, List<E> entity) {
		List<E> updated = new ArrayList<E>();
		for (E e : entity)
			if (null != e) {
				E existed = batchTemplate.selectOne(selectSqlId, e.getId());
				if (null != existed) {
					batchTemplate.update(updateSqlId, e);
					updated.add(existed);
				}
			}
		return updated;
	}

	protected final <K extends Serializable, E extends AbstractEntity<K>> List<E> select(String sqlId, Class<E> entityClass,
			List<K> keys) {
		List<E> list = new ArrayList<E>();
		for (K key : keys) {
			E e = batchTemplate.selectOne(sqlId, key);
			list.add(e);
		}
		return list;
	}

	/* ********************************************** */

	protected final <K extends Serializable, E extends AbstractEntity<K>> List<E> delete(String selectListSqlId,
			String selectItemSqlId, String deleteSqlId, Class<E> entityClass, Criteria criteria) {
		List<E> list = this.select(selectListSqlId, selectItemSqlId, entityClass, criteria, Page.ALL_RECORD());
		for (E e : list)
			batchTemplate.delete(deleteSqlId, e.getId());
		return list;
	}

	protected final <K extends Serializable, E extends AbstractEntity<K>> List<E> update(String selectListSqlId,
			String selectItemSqlId, String updateSqlId, E entity, Criteria criteria) {
		List<E> list = this.select(selectListSqlId, selectItemSqlId, entityClass(entity), criteria, Page.ALL_RECORD());
		for (E e : list) {
			entity.setId(e.getId());
			batchTemplate.update(updateSqlId, e);
		}
		return list;
	}

	protected final <K extends Serializable, E extends AbstractEntity<K>> int count(String sqlId, Class<E> entityClass,
			Criteria criteria) {
		Object r = batchTemplate.selectOne(sqlId, criteria);
		return null == r ? 0 : ((Number) r).intValue();
	}

	protected final <K extends Serializable, E extends AbstractEntity<K>> List<K> selectKeys(String sqlId,
			Class<E> entityClass, Criteria criteria, Page page) {
		if (null == page) throw new SystemException("Query must be limited by page.");
		// dirty page
		if (page.getTotal() < 0) page.setTotal(this.count(entityClass, criteria));
		OrderedRowBounds rb = new OrderedRowBounds(page.getStart(), page.getSize(), criteria.getOrderFields());
		return batchTemplate.selectList(sqlId, criteria, rb);
	}

	protected final <K extends Serializable, E extends AbstractEntity<K>> List<E> select(String selectListSqlId,
			String selectItemSqlId, Class<E> entityClass, Criteria criteria, Page page) {
		return this.select(selectItemSqlId, entityClass, this.selectKeys(selectListSqlId, entityClass, criteria, page));
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

	@SuppressWarnings("unchecked")
	private <K extends Serializable, E extends AbstractEntity<K>> Class<E> entityClass(E entity) {
		return (Class<E>) entity.getClass();
	}
}
