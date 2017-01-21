package net.butfly.albacore.serder;

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

public class BsonMongoSerder extends ContentTypeSerderBase implements BinarySerder<Object>, BeanSerder<Object, byte[]>,
		ClassInfoSerder<Object, byte[]> {
	private static final long serialVersionUID = -4877674648803659927L;

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
	public <TT> TT der(byte[] from) {
		BSONCallback cb = new BasicBSONCallback();
		decoder.decode(from, cb);
		return (TT) cb.get();
	}

	@Override
	public Object[] der(byte[] from, Class<?>... tos) {
		return this.der(from);
	}

	@Override
	public <T> T der(byte[] from, Class<T> to) {
		return this.der(from);
	}
}
