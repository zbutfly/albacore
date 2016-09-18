package net.butfly.albacore.serder.bson;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.io.IOUtils;
import org.apache.http.entity.ContentType;
import org.bson.BSON;
import org.bson.BSONObject;
import org.bson.LazyBSONCallback;
import org.bson.io.BasicOutputBuffer;
import org.bson.io.OutputBuffer;
import org.bson.types.BasicBSONList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.LazyDBObject;

import net.butfly.albacore.calculus.marshall.bson.bson4jackson.MongoBsonFactory;
import net.butfly.albacore.calculus.marshall.bson.fastxml.UpperCaseWithUnderscoresStrategy;
import net.butfly.albacore.serder.BinarySerder;
import net.butfly.albacore.serder.ContentSerderBase;
import net.butfly.albacore.serder.support.ContentTypes;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class BsonSerder extends ContentSerderBase<Object, byte[]> implements BinarySerder<Object> {
	private static final long serialVersionUID = 6664350391207228363L;

	private static final Logger logger = LoggerFactory.getLogger(BsonSerder.class);
	private static ObjectMapper bsoner = new ObjectMapper(MongoBsonFactory.createFactory()).setPropertyNamingStrategy(
			new UpperCaseWithUnderscoresStrategy()).disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES).disable(
					MapperFeature.USE_GETTERS_AS_SETTERS).disable(SerializationFeature.WRITE_NULL_MAP_VALUES).setSerializationInclusion(
							Include.NON_NULL).configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true).configure(
									JsonParser.Feature.IGNORE_UNDEFINED, true);

	public BsonSerder() {
		super(ContentTypes.APPLICATION_BSON);
	}

	public BsonSerder(ContentType contentType) {
		super(contentType);
	}

	@Override
	public byte[] serialize(Object from) {
		if (null == from) return null;
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try {
			bsoner.writer().writeValue(baos, from);
		} catch (IOException e) {
			logger.error("BSON marshall failure from " + from.getClass().toString(), e);
			return null;
		}
		DBObject r = new BasicDBObject();
		r.putAll(new LazyDBObject(baos.toByteArray(), new LazyBSONCallback()));
		return BSON.encode(r);
	}

	@Override
	public Object deserialize(byte[] from, Class to) {
		if (null == from) return null;
		return fromBSON(BSON.decode(from), to);
	}

	@Override
	public Object[] deserialize(byte[] from, Class<?>[] tos) {
		if (null == from) return null;
		if (from.length == 0) return new Object[0];
		BSONObject bo = BSON.decode(from);
		if (!(bo instanceof BasicBSONList)) return new Object[] { fromBSON(bo, tos[0]) };
		BasicBSONList bl = (BasicBSONList) bo;
		Object[] r = new Object[Math.min(bl.size(), tos.length)];
		for (int i = 0; i < r.length; i++)
			r[i] = fromBSON((BSONObject) bl.get(Integer.toString(i)), tos[i]);
		return r;
	}

	@Override
	public void serialize(OutputStream out, Object from) throws IOException {
		out.write(serialize(from));
	}

	@Override
	public Object deserialize(InputStream in, Class to) throws IOException {
		return deserialize(IOUtils.toByteArray(in), to);
	}

	@SuppressWarnings("deprecation")
	private <T> T fromBSON(BSONObject bson, Class<T> to) {
		OutputBuffer buf = new BasicOutputBuffer();
		try {
			try {
				new DBEncoder().writeObject(buf, bson);
			} catch (Exception ex) {
				logger.error("BSON unmarshall failure from " + to.toString(), ex);
				return null;
			}
			try {
				return bsoner.reader(to).readValue(buf.toByteArray());
			} catch (IOException ex) {
				logger.error("BSON unmarshall failure from " + to.toString(), ex);
				return null;
			}
		} finally {
			buf.close();
		}
	}

	public static void main(String... args) {}
}
