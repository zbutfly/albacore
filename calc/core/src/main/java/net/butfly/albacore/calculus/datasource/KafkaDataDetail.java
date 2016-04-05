package net.butfly.albacore.calculus.datasource;

import net.butfly.albacore.calculus.factor.Factor.Type;

public class KafkaDataDetail extends DataDetail {
	private static final long serialVersionUID = -3136910829803418814L;

	public KafkaDataDetail(String... topics) {
		super(Type.KAFKA, null, topics);
	}

	@Override
	public String toString() {
		return "[Table: " + String.join(",", tables) + "]";
	}
}
