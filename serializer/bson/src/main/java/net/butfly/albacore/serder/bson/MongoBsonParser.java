package net.butfly.albacore.serder.bson;

import java.io.IOException;
import java.io.InputStream;

import org.bson.types.BSONTimestamp;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.io.IOContext;

import de.undercouch.bson4jackson.BsonParser;
import de.undercouch.bson4jackson.types.ObjectId;
import de.undercouch.bson4jackson.types.Timestamp;

class MongoBsonParser extends BsonParser {

	public MongoBsonParser(IOContext ctxt, int jsonFeatures, int bsonFeatures, InputStream in) {
		super(ctxt, jsonFeatures, bsonFeatures, in);
	}

	@Override
	public Object getEmbeddedObject() throws IOException, JsonParseException {
		Object object = super.getEmbeddedObject();
		if (object instanceof ObjectId) { return convertToNativeObjectId((ObjectId) object); }
		if (object instanceof Timestamp) { return convertToBSONTimestamp((Timestamp) object); }
		return object;
	}

	private Object convertToBSONTimestamp(Timestamp ts) {
		return new BSONTimestamp(ts.getTime(), ts.getInc());
	}

	private org.bson.types.ObjectId convertToNativeObjectId(ObjectId id) {
		return org.bson.types.ObjectId.createFromLegacyFormat(id.getTime(), id.getMachine(), id.getInc());
	}
}
