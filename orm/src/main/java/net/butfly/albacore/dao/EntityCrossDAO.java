package net.butfly.albacore.dao;

import java.io.Serializable;

import net.butfly.albacore.dbo.criteria.Criteria;
import net.butfly.albacore.dbo.criteria.Page;
import net.butfly.albacore.entity.AbstractEntity;
import net.butfly.albacore.utils.GenericUtils;

public interface EntityCrossDAO extends EntityDAO {
	public static class SQLBuild<E extends AbstractEntity<?>> {
		private Class<? extends EntityDAO> entityDAOClass = null;
		private String suffix = "";
		private Class<E> entityClass;

		public SQLBuild() {
			this.entityClass = GenericUtils.getGenericParamClass(this.getClass(), SQLBuild.class, "E");
		}

		public Class<? extends EntityDAO> entityDAOClass() {
			return entityDAOClass;
		}

		public SQLBuild<E> entityDAOClass(Class<? extends EntityDAO> entityDAOClass) {
			this.entityDAOClass = entityDAOClass;
			return this;
		}

		public String suffix() {
			return suffix;
		}

		public SQLBuild<E> suffix(String suffix) {
			this.suffix = suffix;
			return this;
		}

		public Class<E> entityClass() {
			return entityClass;
		}
	}

	<K extends Serializable, E extends AbstractEntity<K>> K[] insert(final SQLBuild<E> sql, final E... entity);

	<K extends Serializable, E extends AbstractEntity<K>> E[] delete(final SQLBuild<E> sql, final K... key);

	<K extends Serializable, E extends AbstractEntity<K>> E[] update(final SQLBuild<E> sql, final E... entity);

	<K extends Serializable, E extends AbstractEntity<K>> E[] select(final SQLBuild<E> sql, final K... key);

	<K extends Serializable, E extends AbstractEntity<K>> E[] delete(final SQLBuild<E> sql, Criteria criteria);

	<K extends Serializable, E extends AbstractEntity<K>> E[] update(final SQLBuild<E> sql, final E entity, Criteria criteria);

	<K extends Serializable, E extends AbstractEntity<K>> int count(final SQLBuild<E> sql, Criteria criteria);

	<K extends Serializable, E extends AbstractEntity<K>> K[] selectKeys(final SQLBuild<E> sql, Criteria criteria, Page page);

	<K extends Serializable, E extends AbstractEntity<K>> E[] select(final SQLBuild<E> sql, Criteria criteria, Page page);
}
