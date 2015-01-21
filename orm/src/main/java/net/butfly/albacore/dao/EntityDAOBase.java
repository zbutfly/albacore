package net.butfly.albacore.dao;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import net.butfly.albacore.dbo.criteria.Criteria;
import net.butfly.albacore.dbo.criteria.Page;
import net.butfly.albacore.entity.AbstractEntity;
import net.butfly.albacore.utils.KeyUtils;

public class EntityDAOBase extends EntityBasicDAOBase implements EntityDAO {
	private static final long serialVersionUID = -1599466753909389837L;
	protected static final String BY_CRITERIA = "ByCriteria";
	protected String namespace;
	protected static Map<Class<? extends EntityDAO>, String> NAMESPACES_POOL = new ConcurrentHashMap<Class<? extends EntityDAO>, String>();

	public EntityDAOBase() {
		this.namespace = this.getClass().getName().replaceAll("(?i)dao", "").replaceAll("(?i)impl", ".") + ".";
		this.namespace = this.namespace.replaceAll("\\.+", ".");
		NAMESPACES_POOL.put(scanDAO(this.getClass()), this.namespace);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <K extends Serializable, E extends AbstractEntity<K>> K[] insert(final E... entity) {
		return (K[]) super.insert(getSqlId(Verb.insert, entity.getClass().getComponentType().getSimpleName()),
				Arrays.asList(entity)).toArray();
	}

	@SuppressWarnings("unchecked")
	@Override
	public <K extends Serializable, E extends AbstractEntity<K>> E[] delete(final Class<E> entityClass, final K... keys) {
		String target = entityClass.getSimpleName();
		return (E[]) super.delete(getSqlId(Verb.select, target), getSqlId(Verb.delete, target), entityClass,
				Arrays.asList(keys)).toArray();
	}

	@SuppressWarnings("unchecked")
	@Override
	public <K extends Serializable, E extends AbstractEntity<K>> E[] update(E... entity) {
		String target = entity.getClass().getComponentType().getSimpleName();
		return (E[]) super.update(getSqlId(Verb.select, target), getSqlId(Verb.delete, target), Arrays.asList(entity))
				.toArray();
	}

	@SuppressWarnings("unchecked")
	@Override
	public <K extends Serializable, E extends AbstractEntity<K>> E[] select(Class<E> entityClass, K... key) {
		return (E[]) super.select(getSqlId(Verb.select, entityClass.getSimpleName()), entityClass, Arrays.asList(key))
				.toArray();
	}

	@SuppressWarnings("unchecked")
	@Override
	public <K extends Serializable, E extends AbstractEntity<K>> E[] delete(Class<E> entityClass, Criteria criteria) {
		return (E[]) super.delete(getSqlId(Verb.select, entityClass.getSimpleName(), BY_CRITERIA),
				getSqlId(Verb.select, entityClass.getSimpleName()),
				getSqlId(Verb.delete, entityClass.getSimpleName(), BY_CRITERIA), entityClass, criteria).toArray();
	}

	@SuppressWarnings("unchecked")
	@Override
	public <K extends Serializable, E extends AbstractEntity<K>> E[] update(E entity, Criteria criteria) {
		return (E[]) super.update(getSqlId(Verb.select, entity.getClass().getSimpleName(), BY_CRITERIA),
				getSqlId(Verb.select, entity.getClass().getSimpleName()),
				getSqlId(Verb.update, entity.getClass().getSimpleName()), entity, criteria).toArray();
	}

	@Override
	public <K extends Serializable, E extends AbstractEntity<K>> int count(Class<E> entityClass, Criteria criteria) {
		return super.count(getSqlId(Verb.count, entityClass.getSimpleName(), BY_CRITERIA), entityClass, criteria);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <K extends Serializable, E extends AbstractEntity<K>> K[] selectKeys(Class<E> entityClass, Criteria criteria,
			Page page) {
		return (K[]) super.selectKeys(getSqlId(Verb.select, entityClass.getSimpleName(), BY_CRITERIA), entityClass, criteria,
				page).toArray();
	}

	@SuppressWarnings("unchecked")
	@Override
	public <K extends Serializable, E extends AbstractEntity<K>> E[] select(Class<E> entityClass, Criteria criteria, Page page) {
		return (E[]) super.select(getSqlId(Verb.select, entityClass.getSimpleName()),
				getSqlId(Verb.select, entityClass.getSimpleName(), BY_CRITERIA), entityClass, criteria, page).toArray();
	}

	@SuppressWarnings("unchecked")
	private static Class<? extends EntityDAO> scanDAO(Class<?> clazz) {
		if (clazz == null) throw new IllegalAccessError();
		Class<?>[] intfs = clazz.getInterfaces();
		for (Class<?> intf : intfs)
			if (EntityDAO.class.isAssignableFrom(intf)) return (Class<? extends EntityDAO>) intf;
		for (Class<?> intf : intfs) {
			Class<? extends EntityDAO> i = scanDAO(intf);
			if (null != i) return i;
		}
		throw new IllegalAccessError();
	}

	protected String getSqlId(Verb verb, String... segments) {
		return this.namespace + verb + KeyUtils.join(segments);
	}
}
