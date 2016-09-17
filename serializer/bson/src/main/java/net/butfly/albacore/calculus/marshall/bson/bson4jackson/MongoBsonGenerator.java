package net.butfly.albacore.calculus.marshall.bson.bson4jackson;

import java.io.IOException;
import java.io.OutputStream;

import org.bson.types.BSONTimestamp;
import org.bson.types.Binary;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;

import de.undercouch.bson4jackson.BsonConstants;
import de.undercouch.bson4jackson.BsonGenerator;

class MongoBsonGenerator extends BsonGenerator {

	public MongoBsonGenerator(int jsonFeatures, int bsonFeatures, OutputStream out) {
		super(jsonFeatures, bsonFeatures, out);
	}

	public void writeNativeObjectId(org.bson.types.ObjectId objectId) throws IOException {
		_writeArrayFieldNameIfNeeded();
		_verifyValueWrite("write datetime");
		_buffer.putByte(_typeMarker, BsonConstants.TYPE_OBJECTID);
		_buffer.putBytes(objectId.toByteArray());
		flushBuffer();
	}

	public void writeBSONTimestamp(BSONTimestamp timestamp) throws IOException {
		_writeArrayFieldNameIfNeeded();
		_verifyValueWrite("write timestamp");
		_buffer.putByte(_typeMarker, BsonConstants.TYPE_TIMESTAMP);
		_buffer.putInt(timestamp.getInc());
		_buffer.putInt(timestamp.getTime());
		flushBuffer();
	}

	public void writeMinKey(MinKey key) throws IOException {
		_writeArrayFieldNameIfNeeded();
		_verifyValueWrite("write int");
		_buffer.putByte(_typeMarker, BsonConstants.TYPE_MINKEY);
		flushBuffer();
	}

	public void writeMaxKey(MaxKey key) throws IOException {
		_writeArrayFieldNameIfNeeded();
		_verifyValueWrite("write boolean");
		_buffer.putByte(_typeMarker, BsonConstants.TYPE_MAXKEY);
		flushBuffer();
	}

	public void writeBinary(Binary binary) throws IOException {
		_writeArrayFieldNameIfNeeded();
		_verifyValueWrite("write binary");
		byte[] bytes = binary.getData();
		_buffer.putByte(_typeMarker, BsonConstants.TYPE_BINARY);
		_buffer.putInt(bytes.length);
		_buffer.putByte(binary.getType());
		_buffer.putBytes(binary.getData());
		flushBuffer();
	}
}
