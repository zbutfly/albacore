package net.butfly.albacore.calculus.datasource;

import net.butfly.albacore.calculus.factor.Factor.Type;

public class KafkaDetail extends Detail {
	private static final long serialVersionUID = -3136910829803418814L;
	// kafka
	public String[] kafkaTopics;

	public KafkaDetail(String... kafkaTopics) {
		super(Type.KAFKA);
		this.kafkaTopics = kafkaTopics;
	}

	@Override
	public String toString() {
		return "[Table: " + String.join(",", kafkaTopics) + "]";
	}
}
