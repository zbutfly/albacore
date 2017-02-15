package net.butfly.albacore.serder;

import java.util.concurrent.LinkedBlockingQueue;

import org.bson.BSONCallback;
import org.bson.BSONDecoder;
import org.bson.BSONEncoder;
import org.bson.BSONObject;
import org.bson.BasicBSONCallback;
import org.bson.BasicBSONDecoder;
import org.bson.BasicBSONEncoder;
import org.bson.BasicBSONObject;

import net.butfly.albacore.serder.support.ContentTypeSerder;
import net.butfly.albacore.serder.support.ContentTypeSerderBase;
import net.butfly.albacore.serder.support.ContentTypes;
import scala.Tuple2;

public final class BsonObjectSerder extends ContentTypeSerderBase implements Serder<BSONObject, byte[]>, ContentTypeSerder {
	private static final long serialVersionUID = 6664350391207228363L;
	public static final BsonObjectSerder DEFAULT = new BsonObjectSerder();

	private final LinkedBlockingQueue<Tuple2<BSONEncoder, BasicBSONObject>> encoders;
	private final LinkedBlockingQueue<Tuple2<BSONDecoder, BSONCallback>> decoders;

	public BsonObjectSerder() {
		this(50);
	}

	public BsonObjectSerder(int parallelism) {
		this.contentType = ContentTypes.APPLICATION_BSON;
		encoders = new LinkedBlockingQueue<>(parallelism);
		decoders = new LinkedBlockingQueue<>(parallelism);
		for (int i = 0; i < parallelism; i++) {
			encoders.offer(new Tuple2<>(new BasicBSONEncoder(), new BasicBSONObject()));
			decoders.offer(new Tuple2<>(new BasicBSONDecoder(), new BasicBSONCallback()));
		}
	}

	@Override
	public byte[] ser(BSONObject from) {
		Tuple2<BSONEncoder, BasicBSONObject> t;
		try {
			t = encoders.take();
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
		BasicBSONObject b = t._2;
		b.putAll(from);
		try {
			return t._1.encode(b);
		} finally {
			b.clear();
			try {
				encoders.put(t);
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T extends BSONObject> T der(byte[] from, Class<T> to) {
		Tuple2<BSONDecoder, BSONCallback> t;
		try {
			t = decoders.take();
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
		try {
			t._1.decode(from, t._2);
			return ((T) t._2.get());
		} finally {
			try {
				decoders.put(t);
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		}
	}
}
