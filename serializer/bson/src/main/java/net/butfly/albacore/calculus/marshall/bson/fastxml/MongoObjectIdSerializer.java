package net.butfly.albacore.calculus.marshall.bson.fastxml;

import java.io.IOException;

import org.bson.types.ObjectId;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

public class MongoObjectIdSerializer extends JsonSerializer<ObjectId> {
	@Override
	public void serialize(ObjectId value, JsonGenerator gen, SerializerProvider serializers) throws IOException, JsonProcessingException {}
}
