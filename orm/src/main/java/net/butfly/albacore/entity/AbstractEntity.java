package net.butfly.albacore.entity;

import java.io.Serializable;

import net.butfly.albacore.support.ObjectSupport;

public abstract class AbstractEntity extends ObjectSupport<AbstractEntity> implements Serializable {
	private static final long serialVersionUID = 2566809992909078376L;
	// protected String schema = null;
	//
	// public String getSchema() {
	// return schema;
	// }
	//
	// public void setSchema(String schema) {
	// this.schema = schema;
	// }
}
