package net.butfly.albacore.dao;

import java.io.Serializable;

import net.butfly.albacore.dbo.criteria.Criteria;
import net.butfly.albacore.dbo.criteria.Page;
import net.butfly.albacore.entity.Entity;

public interface EntityDAO extends DAO {
	<K extends Serializable, E extends Entity<K>> int count(Class<E> entityClass, Criteria criteria);

	<K extends Serializable, E extends Entity<K>> K insert(E entity);

	<K extends Serializable, E extends Entity<K>> E delete(Class<E> entityClass, K key);

	<K extends Serializable, E extends Entity<K>> boolean update(E entity);

	/**
	 * Batch insert columns into table.
	 * 
	 * Default using statement with id "insertBatch" in mapper file. the statement should look like:
	 * "INSERT INTO tbl_name (a,b,c) VALUES(1,2,3),(4,5,6),(7,8,9);"
	 * 
	 * @param entities
	 * @return
	 */
	<K extends Serializable, E extends Entity<K>> int insert(E[] entities);

	/**
	 * Delete multiply items gracefully (with probably cache discarding).
	 * 
	 * @param keys
	 * @return
	 */
	public <K extends Serializable, E extends Entity<K>> int delete(Class<E> entityClass, K[] keys);

	/**
	 * Delete multiply items directly (without probably cache discarding).
	 * 
	 * @param criteria
	 * @return
	 */
	<K extends Serializable, E extends Entity<K>> int delete(Class<E> entityClass, Criteria criteria);

	<K extends Serializable, E extends Entity<K>> int update(E entity, Criteria criteria);

	<K extends Serializable, E extends Entity<K>> E select(Class<E> entityClass, K key);

	<K extends Serializable, E extends Entity<K>> E[] select(Class<E> entityClass, K[] key);

	<K extends Serializable, E extends Entity<K>> E[] select(Class<E> entityClass, Criteria criteria, Page page);

	<K extends Serializable, E extends Entity<K>> K[] selectKeys(Class<E> entityClass, Criteria criteria, Page page);
}
