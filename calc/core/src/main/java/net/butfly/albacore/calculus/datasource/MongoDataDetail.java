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
	public Configuration outputConfiguration(@SuppressWarnings("rawtypes") DataSource ds) {
		Configuration outputConfig = super.outputConfiguration(ds);
		outputConfig.set("mongo.job.output.format", MongoOutputFormat.class.getName());
		MongoClientURI muri = new MongoClientURI(MongoDataSource.class.cast(ds).uri);
		outputConfig.set("mongo.output.uri", new MongoClientURIBuilder(muri).collection(muri.getDatabase(), tables[0]).build().toString());
		return outputConfig;
	}
}
