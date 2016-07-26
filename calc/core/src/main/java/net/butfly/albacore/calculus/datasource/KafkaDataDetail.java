package net.butfly.albacore.calculus.datasource;

import net.butfly.albacore.calculus.factor.Factor.Type;

public class KafkaDataDetail<F> extends DataDetail<F> {
	private static final long serialVersionUID = -3136910829803418814L;

	public KafkaDataDetail(Class<F> factor, String source, String... topics) {
		super(Type.KAFKA, factor, source, null, topics);
	}
}
