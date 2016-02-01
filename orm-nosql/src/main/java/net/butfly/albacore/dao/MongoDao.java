package net.butfly.albacore.dao;

import net.butfly.albacore.dbo.MongoEntity;
import net.butfly.albacore.dbo.criteria.Page;

import org.bson.types.ObjectId;
import org.mongodb.morphia.query.Query;

public interface MongoDao<E extends MongoEntity> extends DAO {
	void setContext(MongoContext context);

	public int count(Query<E> criteria);

	public ObjectId insert(E entity);

	public E delete(ObjectId key);

	public boolean update(E entity);

	public int insert(E[] entities);

	public int delete(ObjectId[] keys);

	public int delete(Query<E> query);

	public int update(E entity, Query<E> query);

	public E select(ObjectId key);

	public E[] select(ObjectId[] key);

	public E[] select(Query<E> query, Page page);
}
