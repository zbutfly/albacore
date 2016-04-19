package net.butfly.albacore.calculus.datasource;

import org.apache.hadoop.conf.Configuration;

import com.mongodb.MongoClientURI;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.util.MongoClientURIBuilder;

import net.butfly.albacore.calculus.factor.Factor.Type;

public class MongoDataDetail<F> extends DataDetail<F> {
	private static final long serialVersionUID = 4206637701358532787L;

	public MongoDataDetail(Class<F> factor, String filter, String... table) {
		super(Type.MONGODB, factor, filter, table);
	}

	@Override
	public String toString() {
		return "[Table: " + tables[0] + ", FactorFilter: " + filter + "]";
	}

	@Override
	@SuppressWarnings("rawtypes")
	public <DS extends DataSource> Configuration outputConfig(DS ds) {
		Configuration outputConfig = super.outputConfig(ds);
		outputConfig.set("mongo.job.output.format", MongoOutputFormat.class.getName());
		MongoClientURI muri = new MongoClientURI(((MongoDataSource) ds).uri);
		outputConfig.set("mongo.output.uri", new MongoClientURIBuilder(muri).collection(muri.getDatabase(), tables[0]).build().toString());
		return outputConfig;
	}
}
