package net.butfly.albacore.dbo;

import net.butfly.albacore.entity.AbstractEntity;

import org.bson.types.ObjectId;
import org.mongodb.morphia.annotations.Id;

public class MongoEntity extends AbstractEntity {
	private static final long serialVersionUID = 3105047393832057088L;
	@Id
	protected ObjectId id;

	public ObjectId getId() {
		return id;
	}

	public void setId(ObjectId id) {
		this.id = id;
	}
}
