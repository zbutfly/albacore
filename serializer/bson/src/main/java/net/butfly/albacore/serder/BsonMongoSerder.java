package net.butfly.albacore.serder;

import java.util.Map;

import org.apache.commons.beanutils.BeanMap;
import org.bson.BSONCallback;
import org.bson.BSONDecoder;
import org.bson.BSONEncoder;
import org.bson.BasicBSONCallback;
import org.bson.BasicBSONDecoder;
import org.bson.BasicBSONEncoder;
import org.bson.BasicBSONObject;

import net.butfly.albacore.serder.support.BeanSerder;
import net.butfly.albacore.serder.support.BinarySerder;
import net.butfly.albacore.serder.support.ClassInfoSerder;
import net.butfly.albacore.serder.support.ContentTypeSerderBase;
import net.butfly.albacore.serder.support.ContentTypes;

public class BsonMongoSerder<T> extends ContentTypeSerderBase implements BinarySerder<T>, BeanSerder<T, byte[]>,
		ClassInfoSerder<T, byte[]> {
	private static final long serialVersionUID = -4877674648803659927L;
	@SuppressWarnings("rawtypes")
	public static final BsonMongoSerder<Map> BSON_MAPPER = new BsonMongoSerder<Map>();

	private final BSONEncoder encoder;
	private final BSONDecoder decoder;

	public BsonMongoSerder() {
		this.contentType = ContentTypes.APPLICATION_BSON;
		encoder = new BasicBSONEncoder();
		decoder = new BasicBSONDecoder();
	}

	@Override
	public byte[] ser(Object from) {
		return encoder.encode(new BasicBSONObject(new BeanMap(from)));
	}

	@SuppressWarnings("unchecked")
	@Override
	public <TT extends T> TT der(byte[] from) {
		BSONCallback cb = new BasicBSONCallback();
		decoder.decode(from, cb);
		return (TT) cb.get();
	}

	@Override
	public Object[] der(byte[] from, Class<?>... tos) {
		return this.der(from);
	}

	@Override
	public <TT extends T> TT der(byte[] from, Class<TT> to) {
		return this.der(from);
	}
}
