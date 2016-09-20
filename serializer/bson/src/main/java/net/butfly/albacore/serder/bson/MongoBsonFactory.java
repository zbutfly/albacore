package net.butfly.albacore.serder.bson;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.core.io.IOContext;

import de.undercouch.bson4jackson.BsonFactory;
import de.undercouch.bson4jackson.BsonGenerator;
import de.undercouch.bson4jackson.BsonParser;

public class MongoBsonFactory extends BsonFactory {
	private static final long serialVersionUID = 4777821292145741519L;

	public static BsonFactory createFactory() {
		BsonFactory factory = new MongoBsonFactory();
		factory.enable(BsonParser.Feature.HONOR_DOCUMENT_LENGTH);
		return factory;
	}

	@Override
	protected BsonParser _createParser(InputStream in, IOContext ctxt) {
		BsonParser p = new MongoBsonParser(ctxt, _parserFeatures, _bsonParserFeatures, in);
		ObjectCodec codec = getCodec();
		if (codec != null) {
			p.setCodec(codec);
		}
		return p;
	}

	@Override
	public BsonGenerator createGenerator(OutputStream out, JsonEncoding enc) throws IOException {
		IOContext ctxt = _createContext(out, true);
		ctxt.setEncoding(enc);
		if (enc == JsonEncoding.UTF8 && _outputDecorator != null) {
			out = _outputDecorator.decorate(ctxt, out);
		}
		BsonGenerator g = new MongoBsonGenerator(_generatorFeatures, _bsonGeneratorFeatures, out);
		ObjectCodec codec = getCodec();
		if (codec != null) {
			g.setCodec(codec);
		}
		return g;
	}
}
