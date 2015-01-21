package net.butfly.albacore.dao;

import java.io.Serializable;
import java.util.Arrays;

import net.butfly.albacore.dbo.criteria.Criteria;
import net.butfly.albacore.dbo.criteria.Page;
import net.butfly.albacore.entity.AbstractEntity;
import net.butfly.albacore.utils.KeyUtils;

public class EntityCrossDAOBase extends EntityDAOBase implements EntityCrossDAO {
	private static final long serialVersionUID = 7417423834936045357L;

	@SuppressWarnings("unchecked")
	@Override
	public <K extends Serializable, E extends AbstractEntity<K>> K[] insert(SQLBuild<E> sql, final E... entity) {
		return (K[]) super.insert(getSqlId(Verb.insert, entity.getClass().getComponentType().getSimpleName()),
				Arrays.asList(entity)).toArray();
	}

	@SuppressWarnings("unchecked")
	@Override
	public <K extends Serializable, E extends AbstractEntity<K>> E[] delete(SQLBuild<E> sql, final K... keys) {
		String target = sql.entityClass().getSimpleName();
		return (E[]) super.delete(getSqlId(Verb.select, target), getSqlId(Verb.delete, target), sql.entityClass(),
				Arrays.asList(keys)).toArray();
	}

	@SuppressWarnings("unchecked")
	@Override
	public <K extends Serializable, E extends AbstractEntity<K>> E[] update(SQLBuild<E> sql, E... entity) {
		String target = entity.getClass().getComponentType().getSimpleName();
		return (E[]) super.update(getSqlId(Verb.select, target), getSqlId(Verb.delete, target), Arrays.asList(entity))
				.toArray();
	}

	@SuppressWarnings("unchecked")
	@Override
	public <K extends Serializable, E extends AbstractEntity<K>> E[] select(SQLBuild<E> sql, K... key) {
		return (E[]) super.select(getSqlId(Verb.select, sql.entityClass().getSimpleName()), sql.entityClass(), Arrays.asList(key))
				.toArray();
	}

	@SuppressWarnings("unchecked")
	@Override
	public <K extends Serializable, E extends AbstractEntity<K>> E[] delete(SQLBuild<E> sql, Criteria criteria) {
		return (E[]) super.delete(getSqlId(Verb.select, sql.entityClass().getSimpleName(), BY_CRITERIA),
				getSqlId(Verb.select, sql.entityClass().getSimpleName()),
				getSqlId(Verb.delete, sql.entityClass().getSimpleName(), BY_CRITERIA), sql.entityClass(), criteria).toArray();
	}

	@SuppressWarnings("unchecked")
	@Override
	public <K extends Serializable, E extends AbstractEntity<K>> E[] update(SQLBuild<E> sql, E entity, Criteria criteria) {
		return (E[]) super.update(getSqlId(Verb.select, entity.getClass().getSimpleName(), BY_CRITERIA),
				getSqlId(Verb.select, entity.getClass().getSimpleName()),
				getSqlId(Verb.update, entity.getClass().getSimpleName()), entity, criteria).toArray();
	}

	@Override
	public <K extends Serializable, E extends AbstractEntity<K>> int count(SQLBuild<E> sql, Criteria criteria) {
		return super.count(getSqlId(Verb.count, sql.entityClass().getSimpleName(), BY_CRITERIA), sql.entityClass(), criteria);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <K extends Serializable, E extends AbstractEntity<K>> K[] selectKeys(SQLBuild<E> sql, Criteria criteria, Page page) {
		return (K[]) super.selectKeys(getSqlId(Verb.select, sql.entityClass().getSimpleName(), BY_CRITERIA), sql.entityClass(), criteria,
				page).toArray();
	}

	@SuppressWarnings("unchecked")
	@Override
	public <K extends Serializable, E extends AbstractEntity<K>> E[] select(SQLBuild<E> sql, Criteria criteria, Page page) {
		return (E[]) super.select(getSqlId(Verb.select, sql.entityClass().getSimpleName()),
				getSqlId(Verb.select, sql.entityClass().getSimpleName(), BY_CRITERIA), sql.entityClass(), criteria, page).toArray();
	}

	protected String getSqlId(Class<? extends EntityDAO> dao, Verb verb, String... segments) {
		return null == dao ? getSqlId(verb, segments) : NAMESPACES_POOL.get(dao) + verb + KeyUtils.join(segments);
	}
}
