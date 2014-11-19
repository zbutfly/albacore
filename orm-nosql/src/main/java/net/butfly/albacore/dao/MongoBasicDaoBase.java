package net.butfly.albacore.dao;


public abstract class MongoBasicDaoBase extends MongoDaoBase implements MongoDao {
	private static final long serialVersionUID = -3173817485648589135L;
//	private static Map<Class<? extends Entity>, BasicDAO<? extends Entity, ObjectId>> DAO_POOL = new HashMap<Class<? extends Entity>, BasicDAO<? extends Entity, ObjectId>>();

//
//	private <E extends Entity> BasicDAO<E, ObjectId> createDAO(Class<E> clazz) {
//		return new BasicDAO<E, ObjectId>(clazz, this.store);
//	}
//
//	public <E extends Entity> long count(Class<E> entityClass, Query<E> criteria) {
//		return this.getDAO(entityClass).count(criteria);
//	}
//
//	public <E extends Entity> ObjectId insert(E entity) {
//		return this.getDAO(entity.getClass()).save(entity);
//	}
//
//	public <E extends Entity> E delete(Class<E> entityClass, ObjectId key) {
//		DAO_POOL.get(entityClass).deleteById(key);
//		return null;
//	}
//
//	public <E extends Entity> boolean update(E entity) {
//		DAO_POOL.get(entity.getClass()).update(q, ops);
//		return false;
//	}
//
//	public <E extends Entity> int insert(E[] entities) {
//		// TODO Auto-generated method stub
//		return 0;
//	}
//
//	public <E extends Entity> int delete(Class<E> entityClass, ObjectId[] keys) {
//		// TODO Auto-generated method stub
//		return 0;
//	}
//
//	public <E extends Entity> int delete(Class<E> entityClass, Query Query) {
//		org.mongodb.morphia.query.Query c = new ;
//		this.store.delete(this.store.createQuery(entityClass).and(c));
//		return 0;
//	}
//
//	@SuppressWarnings("unchecked")
//	public <E extends Entity> int update(E entity, Query criteria) {
//		UpdateOperations<E> opts = (UpdateOperations<E>) this.store.createUpdateOperations(entity.getClass());
//		for (Entry<String, ?> entry : criteria.getParameters().entrySet())
//			opts.add(entry.getKey(), entry.getValue());
//		UpdateResults res = this.store.update(entity, opts);
//		return res.getUpdatedCount() + res.getInsertedCount();
//	}
//
//	public <E extends Entity> E select(Class<E> entityClass, ObjectId key) {
//		return this.store.get(entityClass, key);
//	}
//
//	@SuppressWarnings("unchecked")
//	public <E extends Entity> E[] select(Class<E> entityClass, ObjectId[] key) {
//		List<E> l = this.store.get(entityClass, Arrays.asList(key)).asList();
//		return l.toArray((E[]) Array.newInstance(entityClass, l.size()));
//	}
//
//	public <E extends Entity> E[] select(Class<E> entityClass, Query criteria, Page page) {
//		// TODO Auto-generated method stub
//		return null;
//	}
//
//	@SuppressWarnings("unchecked")
//	protected <E extends Entity> BasicDAO<E, ObjectId> getDAO(Class<E> entityClass) {
//		return (BasicDAO<E, ObjectId>) DAO_POOL.get(entityClass);
//	}
}
