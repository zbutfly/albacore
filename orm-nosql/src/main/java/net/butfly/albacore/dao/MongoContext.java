package net.butfly.albacore.dao;

import java.util.HashMap;
import java.util.Map;

import net.butfly.albacore.dbo.MongoEntity;

import org.mongodb.morphia.Datastore;
import org.mongodb.morphia.Morphia;
import org.mongodb.morphia.mapping.MappedClass;
import org.mongodb.morphia.mapping.MappedField;
import org.mongodb.morphia.mapping.Mapper;
import org.mongodb.morphia.mapping.MapperOptions;

import com.mongodb.MongoClient;

public final class MongoContext {
	MongoClient client;
	Mapper mapper;
	Morphia morphia;
	Datastore store;
	Map<String, MappedClass> mcmap;
	Map<String, MappedField[]> mfmap;

	public MongoContext(MongoClient client, String database, String mapperPackage) {
		this.client = client;
		MapperOptions opts = new MapperOptions();
		this.mapper = new Mapper(opts);
		this.morphia = new Morphia(mapper).mapPackage(mapperPackage, true);
		this.mcmap = this.mapper.getMCMap();
		this.mfmap = new HashMap<String, MappedField[]>();
		for (String className : this.mcmap.keySet())
			this.mfmap.put(className, this.mcmap.get(className).getPersistenceFields().toArray(new MappedField[0]));
		this.store = this.morphia.createDatastore(this.client, database);
	}

	public MappedField[] getAllFields(Class<? extends MongoEntity> entityClass) {
		return this.mfmap.get(entityClass.getName());
	}

	public String getMongoField(Class<? extends MongoEntity> entityClass, String javaFieldName) {
		return this.mcmap.get(entityClass.getName()).getMappedFieldByJavaField(javaFieldName).getNameToStore();
	}
}
