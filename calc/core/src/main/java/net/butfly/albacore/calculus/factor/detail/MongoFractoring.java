package net.butfly.albacore.calculus.factor.detail;

import org.apache.hadoop.conf.Configuration;

import com.mongodb.MongoClientURI;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.util.MongoClientURIBuilder;
import com.mongodb.hadoop.util.MongoConfigUtil;

import net.butfly.albacore.calculus.datasource.DataSource;
import net.butfly.albacore.calculus.datasource.DataSource.Type;
import net.butfly.albacore.calculus.datasource.MongoDataSource;
import net.butfly.albacore.calculus.factor.FactroingConfig;

public class MongoFractoring<F> extends FactroingConfig<F> {
	private static final long serialVersionUID = 4206637701358532787L;

	public MongoFractoring(Class<F> factor, String source, String table, String query) {
		super(Type.MONGODB, factor, source, table, query);
	}

	@Override
	public Configuration outputConfiguration(@SuppressWarnings("rawtypes") DataSource ds) {
		Configuration mconf = new Configuration();
		mconf.set(MongoConfigUtil.JOB_OUTPUT_FORMAT, MongoOutputFormat.class.getName());
		MongoClientURI muri = new MongoClientURI(MongoDataSource.class.cast(ds).uri);
		mconf.set(MongoConfigUtil.OUTPUT_URI, new MongoClientURIBuilder(muri).collection(muri.getDatabase(), table).build().toString());
		return mconf;
	}
}
