package net.butfly.albacore.dao;

import java.io.Serializable;

import net.butfly.albacore.dbo.criteria.Criteria;
import net.butfly.albacore.dbo.criteria.Page;
import net.butfly.albacore.entity.AbstractEntity;
import net.butfly.albacore.utils.GenericUtils;

public class EntityDAOBase extends EntityBasicDAOBase implements EntityDAO {
	private static final long serialVersionUID = -1599466753909389837L;
	private String namespace;

	public EntityDAOBase() {
		this.namespace = this.getClass().getName().replaceAll("(?i)dao", "").replaceAll("(?i)impl", ".") + ".";
		namespace = namespace.replaceAll("\\.+", ".");
	}

	@Override
	public <K extends Serializable, E extends AbstractEntity<K>> K insert(final E entity) {
		return super.insert(new SQLBuild<E>(this.namespace, GenericUtils.entityClass(entity)), entity);
	}

	@Override
	public <K extends Serializable, E extends AbstractEntity<K>> K[] insert(final E... entity) {
		return super.insert(new SQLBuild<E>(this.namespace, GenericUtils.entityClass(entity)), entity);
	}

	@Override
	public <K extends Serializable, E extends AbstractEntity<K>> E delete(final Class<E> entityClass, final K key) {
		return super.delete(new SQLBuild<E>(this.namespace, entityClass), key);
	}

	@Override
	public <K extends Serializable, E extends AbstractEntity<K>> E[] delete(final Class<E> entityClass, final K... key) {
		return super.delete(new SQLBuild<E>(this.namespace, entityClass), key);
	}

	@Override
	public <K extends Serializable, E extends AbstractEntity<K>> E update(E entity) {
		return super.update(new SQLBuild<E>(this.namespace, GenericUtils.entityClass(entity)), entity);
	}

	@Override
	public <K extends Serializable, E extends AbstractEntity<K>> E[] update(E... entity) {
		return super.update(new SQLBuild<E>(this.namespace, GenericUtils.entityClass(entity)), entity);
	}

	@Override
	public <K extends Serializable, E extends AbstractEntity<K>> E select(Class<E> entityClass, K key) {
		return super.select(new SQLBuild<E>(this.namespace, entityClass), key);
	}

	@Override
	public <K extends Serializable, E extends AbstractEntity<K>> E[] select(Class<E> entityClass, K... key) {
		return super.select(new SQLBuild<E>(this.namespace, entityClass), key);
	}

	@Override
	public <K extends Serializable, E extends AbstractEntity<K>> E[] delete(Class<E> entityClass, Criteria criteria) {
		return super.delete(new SQLBuild<E>(this.namespace, entityClass), criteria);
	}

	@Override
	public <K extends Serializable, E extends AbstractEntity<K>> E[] update(E entity, Criteria criteria) {
		return super.update(new SQLBuild<E>(this.namespace, GenericUtils.entityClass(entity)), entity, criteria);
	}

	@Override
	public <K extends Serializable, E extends AbstractEntity<K>> int count(Class<E> entityClass, Criteria criteria) {
		return super.count(new SQLBuild<E>(this.namespace, entityClass), criteria);
	}

	@Override
	public <K extends Serializable, E extends AbstractEntity<K>> K[] selectKeys(Class<E> entityClass, Criteria criteria,
			Page page) {
		return super.selectKeys(new SQLBuild<E>(this.namespace, entityClass), criteria, page);
	}

	@Override
	public <K extends Serializable, E extends AbstractEntity<K>> E[] select(Class<E> entityClass, Criteria criteria, Page page) {
		return super.select(new SQLBuild<E>(this.namespace, entityClass), criteria, page);
	}
}
