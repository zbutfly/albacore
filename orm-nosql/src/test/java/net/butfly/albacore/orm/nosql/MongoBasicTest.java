package net.butfly.albacore.orm.nosql;

import org.jmingo.JMingoTemplate;
import org.jmingo.context.Context;

public class MongoBasicTest {
	public static void main(String[] args) {
		Context context = ContextLoader.getInstance().load("/mingo/mingo-context.xml");
		JMingoTemplate mingoTemplate = new JMingoTemplate(context);
		IReviewRepository reviewRepository = new ReviewRepository(mingoTemplate);
	}
}
