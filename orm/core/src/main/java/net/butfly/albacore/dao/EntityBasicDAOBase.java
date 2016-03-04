package net.butfly.albacore.dao;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.mybatis.spring.SqlSessionTemplate;

import net.butfly.albacore.dao.DAO.SQL.Verb;
import net.butfly.albacore.dbo.criteria.Criteria;
import net.butfly.albacore.dbo.criteria.OrderedRowBounds;
import net.butfly.albacore.dbo.criteria.Page;
import net.butfly.albacore.entity.AbstractEntity;
import net.butfly.albacore.entity.Key;
import net.butfly.albacore.exception.SystemException;
import net.butfly.albacore.utils.Generics;

@SuppressWarnings("unchecked")
public class EntityBasicDAOBase extends DAOBase implements EntityBasicDAO {
	private static final long serialVersionUID = -2472419986526183766L;
	protected SqlSessionTemplate template;

	public void setTemplate(SqlSessionTemplate template) {
		this.template = template;
	}

	public <K extends Serializable, E extends AbstractEntity<K>> K insert(SQL<E> sql, E entity) {
		if (null == entity) return null;
		if (entity.getId() == null || !(entity.getId() instanceof Key) || this.select(sql, entity.getId()) == null) {
			this.template.insert(sql.verb(Verb.insert).toString(), entity);
			return entity.getId();
		} else return null;
	}

	public <K extends Serializable, E extends AbstractEntity<K>> K[] insert(SQL<E> sql, E... entities) {
		List<K> keys = new ArrayList<K>();
		if (null != entities) for (E e : entities) {
			K k = this.insert(sql, e);
			if (null != k) keys.add(k);
		}
		// XXX: optimize
		Class<K> keyClass = AbstractEntity.getKeyClass(sql.entityClass());
		return Generics.toArray(keys, keyClass);
	}

	public <K extends Serializable, E extends AbstractEntity<K>> E delete(SQL<E> sql, K key) {
		if (null == key) return null;
		E e = this.select(sql, key);
		if (null != e) {
			this.template.delete(sql.verb(Verb.delete).toString(), e.getId());
			return e;
		} else return null;
	}

	public <K extends Serializable, E extends AbstractEntity<K>> E[] delete(SQL<E> sql, K... keys) {
		List<E> deleted = new ArrayList<E>();
		for (K key : keys) {
			E d = this.delete(sql, key);
			if (null != d) deleted.add(d);
		}
		return Generics.toArray(deleted, sql.entityClass());
	}

	public <K extends Serializable, E extends AbstractEntity<K>> E update(SQL<E> sql, E entity) {
		if (null == entity) return null;
		E existed = this.select(sql, entity.getId());
		if (null != existed) {
			this.template.update(sql.verb(Verb.update).toString(), entity);
			return existed;
		} else return null;
	}

	public <K extends Serializable, E extends AbstractEntity<K>> E[] update(SQL<E> sql, E... entity) {
		List<E> updated = new ArrayList<E>();
		for (E e : entity) {
			E u = this.update(sql, e);
			if (null != u) updated.add(u);
		}
		return Generics.toArray(updated, sql.entityClass());
	}

	public <K extends Serializable, E extends AbstractEntity<K>> E select(SQL<E> sql, K key) {
		if (null == key) return null;
		return this.template.selectOne(sql.verb(Verb.select).toString(), key);
	}

	public <K extends Serializable, E extends AbstractEntity<K>> E[] select(SQL<E> sql, K... keys) {
		List<E> list = new ArrayList<E>();
		for (K key : keys) {
			E e = this.select(sql, key);
			if (null != e) list.add(e);
		}
		return Generics.toArray(list, sql.entityClass());
	}

	public <K extends Serializable, E extends AbstractEntity<K>> E[] delete(SQL<E> sql, Criteria criteria) {
		E[] list = this.select(sql, criteria, Page.ALL_RECORD());
		for (E e : list)
			this.template.delete(sql.verb(Verb.delete).toString(), e.getId());
		return list;
	}

	public <K extends Serializable, E extends AbstractEntity<K>> E[] update(SQL<E> sql, E entity, Criteria criteria) {
		E[] list = this.select(sql, criteria, Page.ALL_RECORD());
		for (E e : list) {
			entity.setId(e.getId());
			this.template.update(sql.verb(Verb.update).toString(), entity);
		}
		return list;
	}

	public <K extends Serializable, E extends AbstractEntity<K>> int count(SQL<E> sql, Criteria criteria) {
		Object r = this.template.selectOne(sql.verb(Verb.count).toCriteriaString(), criteria);
		return null == r ? 0 : ((Number) r).intValue();
	}

	public <K extends Serializable, E extends AbstractEntity<K>> K[] selectKeys(SQL<E> sql, Criteria criteria, Page page) {
		if (null == page) throw new SystemException("Query must be limited by page.");
		// dirty page
		if (page.getTotal() < 0) page.setTotal(this.count(sql, criteria));
		Class<K> keyClass = AbstractEntity.getKeyClass(sql.entityClass());
		if (page.getTotal() == 0) return Generics.toArray(new ArrayList<K>(), keyClass);
		OrderedRowBounds rb = new OrderedRowBounds(page.getOffset(), page.getLimit(), criteria.getOrderFields());
		List<K> list = this.template.selectList(sql.verb(Verb.select).toCriteriaString(), criteria, rb);
		// XXX: optimize
		return Generics.toArray(list, keyClass);
	}

	public <K extends Serializable, E extends AbstractEntity<K>> E[] select(SQL<E> sql, Criteria criteria, Page page) {
		return this.select(sql, this.selectKeys(sql, criteria, page));
	}
}
