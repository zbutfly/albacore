package net.butfly.albacore.dao;

import java.io.Serializable;

import net.butfly.albacore.dbo.criteria.Criteria;
import net.butfly.albacore.dbo.criteria.Page;
import net.butfly.albacore.entity.AbstractEntity;

@SuppressWarnings("unchecked")
public interface EntityDAO extends EntityBasicDAO {
	<K extends Serializable, E extends AbstractEntity<K>> K insert(final E entity);

	<K extends Serializable, E extends AbstractEntity<K>> K[] insert(final E... entity);

	<K extends Serializable, E extends AbstractEntity<K>> E delete(final Class<E> entityClass, final K key);

	<K extends Serializable, E extends AbstractEntity<K>> E[] delete(final Class<E> entityClass, final K... key);

	<K extends Serializable, E extends AbstractEntity<K>> E update(final E entity);

	<K extends Serializable, E extends AbstractEntity<K>> E[] update(final E... entity);

	<K extends Serializable, E extends AbstractEntity<K>> E select(final Class<E> entityClass, final K key);

	<K extends Serializable, E extends AbstractEntity<K>> E[] select(final Class<E> entityClass, final K... key);

	<K extends Serializable, E extends AbstractEntity<K>> E[] delete(final Class<E> entityClass, Criteria criteria);

	<K extends Serializable, E extends AbstractEntity<K>> E[] update(final E entity, Criteria criteria);

	<K extends Serializable, E extends AbstractEntity<K>> int count(final Class<E> entityClass, Criteria criteria);

	<K extends Serializable, E extends AbstractEntity<K>> K[] selectKeys(final Class<E> entityClass, Criteria criteria, Page page);

	<K extends Serializable, E extends AbstractEntity<K>> E[] select(final Class<E> entityClass, Criteria criteria, Page page);
}
