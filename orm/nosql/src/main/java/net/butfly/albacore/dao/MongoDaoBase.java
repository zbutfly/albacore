package net.butfly.albacore.dao;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;

import net.butfly.albacore.dbo.MongoEntity;
import net.butfly.albacore.dbo.criteria.Criteria;
import net.butfly.albacore.dbo.criteria.OrderField;
import net.butfly.albacore.dbo.criteria.Page;
import net.butfly.albacore.utils.Generics;

import org.bson.types.ObjectId;
import org.mongodb.morphia.Datastore;
import org.mongodb.morphia.Key;
import org.mongodb.morphia.mapping.MappedField;
import org.mongodb.morphia.mapping.Mapper;
import org.mongodb.morphia.query.Query;
import org.mongodb.morphia.query.UpdateOperations;
import org.mongodb.morphia.query.UpdateResults;

public abstract class MongoDaoBase<E extends MongoEntity> extends DAOBase implements MongoDao<E> {
	private static final long serialVersionUID = -3173817485648589135L;
	protected MongoContext context;
	private Class<E> entityClass;

	public MongoDaoBase() {
		this.entityClass = Generics.resolveGenericParameter(this.getClass(), MongoDaoBase.class, "E");
	}

	@Override
	public void setContext(MongoContext context) {
		this.context = context;
	}

	@Override
	public int count(Query<E> criteria) {
		return (int) criteria.countAll();
	}

	@Override
	public ObjectId insert(E entity) {
		Key<E> k = this.context.store.save(entity);
		return (ObjectId) k.getId();
	}

	@Override
	public E delete(ObjectId key) {
		return this.context.store.findAndDelete(this.createIdQuery(key));
	}

	@Override
	public boolean update(E entity) {
		UpdateOperations<E> ops = this.createUpdateOptsFromEntity(entity);
		return this.context.store.findAndModify((Query<E>) this.createIdQuery(entity.getId()), ops, true, false) != null;
	}

	@Override
	public int insert(E[] entities) {
		this.context.store.save(entities);
		return entities.length;
	}

	@Override
	public int delete(ObjectId[] keys) {
		return this.context.store.delete(entityClass, Arrays.asList(keys)).getN();
	}

	@Override
	public int delete(Query<E> query) {
		return this.context.store.delete(query).getN();
	}

	@Override
	public int update(E entity, Query<E> query) {
		UpdateResults r = this.context.store.update(query, this.createUpdateOptsFromEntity(entity));
		return r.getUpdatedCount();
	}

	@Override
	public E select(ObjectId key) {
		return this.context.store.get(entityClass, key);
	}

	@SuppressWarnings("unchecked")
	@Override
	public E[] select(ObjectId[] key) {
		List<E> l = this.context.store.get(entityClass, Arrays.asList(key)).asList();
		return l.toArray((E[]) Array.newInstance(entityClass, l.size()));
	}

	@SuppressWarnings("unchecked")
	@Override
	public E[] select(Query<E> query, Page page) {
		page.setTotal((int) query.countAll());
		return query.offset(page.getOffset()).limit(page.getLimit()).asList().toArray((E[]) Array.newInstance(query.getEntityClass(), 0));
	}

	protected Datastore getStore() {
		return this.context.store;
	}

	protected Query<E> createIdQuery(ObjectId... keys) {
		if (keys.length == 1) return this.context.store.createQuery(entityClass).field(Mapper.ID_KEY).equal(keys[0]);
		else return this.context.store.createQuery(entityClass).field(Mapper.ID_KEY).in(Arrays.asList(keys));
	}

	protected UpdateOperations<E> createUpdateOptsFromEntity(E entity) {
		UpdateOperations<E> opts = (UpdateOperations<E>) this.context.store.createUpdateOperations(entityClass);
		for (MappedField f : this.context.getAllFields(entity.getClass())) {
			String name = f.getNameToStore();
			if (Mapper.ID_KEY.equals(name)) continue;
			Object value = f.getFieldValue(entity);
			if (null == value || (String.class.equals(value.getClass()) && "".equals(value))) continue;
			opts.set(name, value);
		}
		return opts;
	}

	protected Query<E> createSimpleQuery(Criteria criteria) {
		Query<E> q = this.context.store.createQuery(entityClass);
		for (Entry<String, Object> e : criteria.getParameters().entrySet())
			q.field(this.context.getMongoField(entityClass, e.getKey())).equal(e.getValue());
		for (OrderField f : criteria.getOrderFields()) {
			String o = f.field();
			if (f.desc()) o = "-" + o;
			q.order(o);
		}
		return q;
	}

	protected Query<E> createQuery() {
		return this.context.store.createQuery(entityClass);
	}
}
