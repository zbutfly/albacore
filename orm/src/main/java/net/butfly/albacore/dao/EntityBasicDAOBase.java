package net.butfly.albacore.dao;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import net.butfly.albacore.dao.SQLBuild.Verb;
import net.butfly.albacore.dbo.criteria.Criteria;
import net.butfly.albacore.dbo.criteria.OrderedRowBounds;
import net.butfly.albacore.dbo.criteria.Page;
import net.butfly.albacore.entity.AbstractEntity;
import net.butfly.albacore.entity.Key;
import net.butfly.albacore.exception.SystemException;
import net.butfly.albacore.utils.GenericUtils;

import org.mybatis.spring.SqlSessionTemplate;

public class EntityBasicDAOBase extends DAOBase implements EntityBasicDAO {
	private static final long serialVersionUID = -2472419986526183766L;
	@SuppressWarnings("unused")
	private SqlSessionTemplate template, batchTemplate;

	public void setTemplate(SqlSessionTemplate template) {
		this.template = template;
	}

	public void setBatchTemplate(SqlSessionTemplate batchTemplate) {
		this.batchTemplate = batchTemplate;
	}

	public <K extends Serializable, E extends AbstractEntity<K>> K insert(SQLBuild<E> sql, E entity) {
		if (null == entity) return null;
		if (entity.getId() == null || !(entity.getId() instanceof Key) || this.select(sql, entity.getId()) == null) {
			batchTemplate.insert(sql.verb(Verb.insert).toString(), entity);
			return entity.getId();
		} else return null;
	}

	public <K extends Serializable, E extends AbstractEntity<K>> K[] insert(SQLBuild<E> sql, E... entities) {
		List<K> keys = new ArrayList<K>();
		if (null != entities) for (E e : entities) {
			K k = this.insert(sql, e);
			if (null != k) keys.add(k);
		}
		// XXX: optimize
		Class<K> keyClass = GenericUtils.getGenericParamClass(sql.entityClass(), AbstractEntity.class, "K");
		return GenericUtils.toArray(keys, keyClass);
	}

	public <K extends Serializable, E extends AbstractEntity<K>> E delete(SQLBuild<E> sql, K key) {
		if (null == key) return null;
		E e = this.select(sql, key);
		if (null != e) {
			batchTemplate.delete(sql.verb(Verb.delete).toString(), e.getId());
			return e;
		} else return null;
	}

	public <K extends Serializable, E extends AbstractEntity<K>> E[] delete(SQLBuild<E> sql, K... keys) {
		List<E> deleted = new ArrayList<E>();
		for (K key : keys) {
			E d = this.delete(sql, key);
			if (null != d) deleted.add(d);
		}
		return GenericUtils.toArray(deleted, sql.entityClass());
	}

	public <K extends Serializable, E extends AbstractEntity<K>> E update(SQLBuild<E> sql, E entity) {
		if (null == entity) return null;
		E existed = this.select(sql, entity.getId());
		if (null != existed) {
			batchTemplate.update(sql.verb(Verb.update).toString(), entity);
			return existed;
		} else return null;
	}

	public <K extends Serializable, E extends AbstractEntity<K>> E[] update(SQLBuild<E> sql, E... entity) {
		List<E> updated = new ArrayList<E>();
		for (E e : entity) {
			E u = this.update(sql, e);
			if (null != u) updated.add(u);
		}
		return GenericUtils.toArray(updated, sql.entityClass());
	}

	public <K extends Serializable, E extends AbstractEntity<K>> E select(SQLBuild<E> sql, K key) {
		if (null == key) return null;
		return batchTemplate.selectOne(sql.verb(Verb.select).toString(), key);
	}

	public <K extends Serializable, E extends AbstractEntity<K>> E[] select(SQLBuild<E> sql, K... keys) {
		List<E> list = new ArrayList<E>();
		for (K key : keys) {
			E e = this.select(sql, key);
			if (null != e) list.add(e);
		}
		return GenericUtils.toArray(list, sql.entityClass());
	}

	public <K extends Serializable, E extends AbstractEntity<K>> E[] delete(SQLBuild<E> sql, Criteria criteria) {
		E[] list = this.select(sql, criteria, Page.ALL_RECORD());
		for (E e : list)
			batchTemplate.delete(sql.verb(Verb.delete).toString(), e.getId());
		return list;
	}

	public <K extends Serializable, E extends AbstractEntity<K>> E[] update(SQLBuild<E> sql, E entity, Criteria criteria) {
		E[] list = this.select(sql, criteria, Page.ALL_RECORD());
		for (E e : list) {
			entity.setId(e.getId());
			batchTemplate.update(sql.verb(Verb.update).toString(), e);
		}
		return list;
	}

	public <K extends Serializable, E extends AbstractEntity<K>> int count(SQLBuild<E> sql, Criteria criteria) {
		Object r = batchTemplate.selectOne(sql.verb(Verb.count).toCriteriaString(), criteria);
		return null == r ? 0 : ((Number) r).intValue();
	}

	public <K extends Serializable, E extends AbstractEntity<K>> K[] selectKeys(SQLBuild<E> sql, Criteria criteria, Page page) {
		if (null == page) throw new SystemException("Query must be limited by page.");
		// dirty page
		if (page.getTotal() < 0) page.setTotal(this.count(sql, criteria));
		OrderedRowBounds rb = new OrderedRowBounds(page.getStart(), page.getSize(), criteria.getOrderFields());
		List<K> list = batchTemplate.selectList(sql.verb(Verb.select).toCriteriaString(), criteria, rb);
		// XXX: optimize
		Class<K> keyClass = GenericUtils.getGenericParamClass(sql.entityClass(), AbstractEntity.class, "K");
		return GenericUtils.toArray(list, keyClass);
	}

	public <K extends Serializable, E extends AbstractEntity<K>> E[] select(SQLBuild<E> sql, Criteria criteria, Page page) {
		return this.select(sql, this.selectKeys(sql, criteria, page));
	}
}
