package net.butfly.albacore.serder.bson;

import java.util.Date;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;

import org.bson.BSON;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.BsonUndefined;
import org.bson.types.BSONTimestamp;
import org.bson.types.Binary;
import org.bson.types.Code;
import org.bson.types.CodeWScope;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.bson.types.ObjectId;
import org.bson.types.Symbol;

import com.mongodb.DBRef;
import com.mongodb.DefaultDBEncoder;

public final class DBEncoder extends DefaultDBEncoder {
	@SuppressWarnings("rawtypes")
	protected void _putObjectField(final String name, final Object initialValue) {
		if ("_transientFields".equals(name)) { return; }
		if (name.contains("\0")) { throw new IllegalArgumentException(
				"Document field names can't have a NULL character. (Bad MapReduceKey: '" + name + "')"); }

		if ("$where".equals(name) && initialValue instanceof String) {
			putCode(name, new Code((String) initialValue));
		}

		Object value = BSON.applyEncodingHooks(initialValue);
		if (value == null || value instanceof BsonUndefined) putNull(name);
		else if (value instanceof Date) putDate(name, (Date) value);
		else if (value instanceof Number) putNumber(name, (Number) value);
		else if (value instanceof Character) putString(name, value.toString());
		else if (value instanceof String) putString(name, value.toString());
		else if (value instanceof ObjectId) putObjectId(name, (ObjectId) value);
		else if (value instanceof Boolean) putBoolean(name, (Boolean) value);
		else if (value instanceof Pattern) putPattern(name, (Pattern) value);
		else if (value instanceof Iterable) putIterable(name, (Iterable) value);
		else if (value instanceof BSONObject) putObject(name, (BSONObject) value);
		else if (value instanceof Map) putMap(name, (Map) value);
		else if (value instanceof byte[]) putBinary(name, (byte[]) value);
		else if (value instanceof Binary) putBinary(name, (Binary) value);
		else if (value instanceof UUID) putUUID(name, (UUID) value);
		else if (value.getClass().isArray()) putArray(name, value);
		else if (value instanceof Symbol) putSymbol(name, (Symbol) value);
		else if (value instanceof BSONTimestamp) putTimestamp(name, (BSONTimestamp) value);
		else if (value instanceof CodeWScope) putCodeWScope(name, (CodeWScope) value);
		else if (value instanceof Code) putCode(name, (Code) value);
		else if (value instanceof DBRef) {
			BSONObject temp = new BasicBSONObject();
			temp.put("$ref", ((DBRef) value).getCollectionName());
			temp.put("$key", ((DBRef) value).getId());
			putObject(name, temp);
		} else if (value instanceof MinKey) putMinKey(name);
		else if (value instanceof MaxKey) putMaxKey(name);
		else if (putSpecial(name, value)) ; // no-op
		else {
			throw new IllegalArgumentException("Can't serialize " + value.getClass());
		}
	}
}