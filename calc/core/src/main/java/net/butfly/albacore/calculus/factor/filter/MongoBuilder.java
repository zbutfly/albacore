package net.butfly.albacore.calculus.factor.filter;

import org.bson.BSONObject;

import net.butfly.albacore.calculus.factor.Factor;
import net.butfly.albacore.calculus.marshall.MongoMarshaller;

public class MongoBuilder<F extends Factor<F>> extends Builder<BSONObject, Object, F> {
	public MongoBuilder(Class<F> factor, MongoMarshaller marshaller) {
		super(factor, marshaller);
	}

	private static final long serialVersionUID = 7966027908028498095L;

	@Override
	public BSONObject filter(FactorFilter... filter) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected BSONObject filterOne(FactorFilter filter) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected Object expression(Class<?> type, Object value) {
		// TODO Auto-generated method stub
		return null;
	}

}
