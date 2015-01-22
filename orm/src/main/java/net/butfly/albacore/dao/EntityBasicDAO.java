package net.butfly.albacore.dao;

import java.io.Serializable;

import net.butfly.albacore.dbo.criteria.Criteria;
import net.butfly.albacore.dbo.criteria.Page;
import net.butfly.albacore.entity.AbstractEntity;

public interface EntityBasicDAO extends DAO {
	<K extends Serializable, E extends AbstractEntity<K>> K insert(final SQLBuild<E> sql, final E entity);

	<K extends Serializable, E extends AbstractEntity<K>> K[] insert(final SQLBuild<E> sql, final E... entity);

	<K extends Serializable, E extends AbstractEntity<K>> E delete(final SQLBuild<E> sql, final K key);

	<K extends Serializable, E extends AbstractEntity<K>> E[] delete(final SQLBuild<E> sql, final K... key);

	<K extends Serializable, E extends AbstractEntity<K>> E update(final SQLBuild<E> sql, final E entity);

	<K extends Serializable, E extends AbstractEntity<K>> E[] update(final SQLBuild<E> sql, final E... entity);

	<K extends Serializable, E extends AbstractEntity<K>> E select(final SQLBuild<E> sql, final K key);

	<K extends Serializable, E extends AbstractEntity<K>> E[] select(final SQLBuild<E> sql, final K... key);

	<K extends Serializable, E extends AbstractEntity<K>> E[] delete(final SQLBuild<E> sql, Criteria criteria);

	<K extends Serializable, E extends AbstractEntity<K>> E[] update(final SQLBuild<E> sql, final E entity, Criteria criteria);

	<K extends Serializable, E extends AbstractEntity<K>> int count(final SQLBuild<E> sql, Criteria criteria);

	<K extends Serializable, E extends AbstractEntity<K>> K[] selectKeys(final SQLBuild<E> sql, Criteria criteria, Page page);

	<K extends Serializable, E extends AbstractEntity<K>> E[] select(final SQLBuild<E> sql, Criteria criteria, Page page);
}
