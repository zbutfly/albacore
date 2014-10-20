package net.butfly.albacore.dao;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import net.butfly.albacore.dbo.criteria.Criteria;
import net.butfly.albacore.dbo.criteria.Page;
import net.butfly.albacore.dbo.mongo.Entity;

import org.bson.types.ObjectId;
import org.mongodb.morphia.Datastore;
import org.mongodb.morphia.Morphia;
import org.mongodb.morphia.dao.BasicDAO;
import org.mongodb.morphia.mapping.MappedClass;
import org.mongodb.morphia.mapping.Mapper;
import org.mongodb.morphia.mapping.MapperOptions;
import org.mongodb.morphia.query.UpdateOperations;
import org.mongodb.morphia.query.UpdateResults;

import com.mongodb.MongoClient;
import com.mongodb.WriteResult;

@SuppressWarnings("rawtypes")
public abstract class MongoDaoBase extends DAOBase implements MongoDao {
	private static final long serialVersionUID = -3173817485648589135L;
	private static Map<Class, BasicDAO> DAO_POOL = new HashMap<Class, BasicDAO>();
	private MongoClient client;
	private Morphia morphia;
	private Datastore store;

	public void setMongoClient(MongoClient client) {
		this.client = client;
	}

	public void setMapperPackage(String mapperPackage) throws ClassNotFoundException {
		MapperOptions opts = new MapperOptions();
		Mapper mapper = new Mapper(opts);
		this.morphia = new Morphia(mapper).mapPackage(mapperPackage, true);
	}

	@SuppressWarnings("unchecked")
	public void setMapperDatabase(String database) {
		this.store = this.morphia.createDatastore(this.client, database);
		for (MappedClass clazz : this.morphia.getMapper().getMappedClasses())
			DAO_POOL.put(clazz.getClazz(), new BasicDAO(clazz.getClass(), this.store));
	}

	public <E extends Entity> int count(Class<E> entityClass, Criteria criteria) {
		// TODO Auto-generated method stub
		return 0;
	}

	public <E extends Entity> ObjectId insert(E entity) {
		// TODO Auto-generated method stub
		return null;
	}

	public <E extends Entity> E delete(Class<E> entityClass, ObjectId key) {
		DAO_POOL.get(entityClass).deleteById(key);
		return null;
	}

	public <E extends Entity> boolean update(E entity) {
		DAO_POOL.get(entity.getClass()).update(q, ops);
		return false;
	}

	public <E extends Entity> int insert(E[] entities) {
		// TODO Auto-generated method stub
		return 0;
	}

	public <E extends Entity> int delete(Class<E> entityClass, ObjectId[] keys) {
		// TODO Auto-generated method stub
		return 0;
	}

	public <E extends Entity> int delete(Class<E> entityClass, Criteria criteria) {
		org.mongodb.morphia.query.Criteria c = new ;
		this.store.delete(this.store.createQuery(entityClass).and(c));
		return 0;
	}

	@SuppressWarnings("unchecked")
	public <E extends Entity> int update(E entity, Criteria criteria) {
		UpdateOperations<E> opts = (UpdateOperations<E>) this.store.createUpdateOperations(entity.getClass());
		for (Entry<String, ?> entry : criteria.getParameters().entrySet())
			opts.add(entry.getKey(), entry.getValue());
		UpdateResults res = this.store.update(entity, opts);
		return res.getUpdatedCount() + res.getInsertedCount();
	}

	public <E extends Entity> E select(Class<E> entityClass, ObjectId key) {
		return this.store.get(entityClass, key);
	}

	@SuppressWarnings("unchecked")
	public <E extends Entity> E[] select(Class<E> entityClass, ObjectId[] key) {
		List<E> l = this.store.get(entityClass, Arrays.asList(key)).asList();
		return l.toArray((E[]) Array.newInstance(entityClass, l.size()));
	}

	public <E extends Entity> E[] select(Class<E> entityClass, Criteria criteria, Page page) {
		// TODO Auto-generated method stub
		return null;
	}
}
