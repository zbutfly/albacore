package net.butfly.albacore.calculus.factor.detail;

import net.butfly.albacore.calculus.factor.FactroingConfig;
import net.butfly.albacore.calculus.factor.Factoring.Type;

public class KafkaFractoring<F> extends FactroingConfig<F> {
	private static final long serialVersionUID = -3136910829803418814L;

	public KafkaFractoring(Class<F> factor, String source, String topics, String query) {
		// TODO: support query?
		super(Type.KAFKA, factor, source, topics, query);
	}
}
