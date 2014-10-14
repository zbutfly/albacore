package net.butfly.albacore.utils.serialize;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

public class JSONSerializer implements Serializer {
	private Gson gson = new Gson();
	private JsonParser parser = new JsonParser();

	@Override
	public void write(Writer writer, Object obj) throws IOException {
		gson.toJson(obj, writer);
		writer.flush();
	}

	@Override
	public void write(OutputStream os, Object obj) throws IOException {
		new OutputStreamWriter(os).write(gson.toJson(obj));
		os.flush();
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> T read(Reader reader, Class<?>... types) throws IOException {
		try {
			JsonElement ele = parser.parse(reader);
			if (ele.isJsonNull()) return null;
			if (ele.isJsonObject() || ele.isJsonPrimitive()) {
				if (types.length < 1) throw new IllegalArgumentException();
				return (T) gson.fromJson(ele, types[0]);
			}
			if (ele.isJsonArray()) {
				JsonArray arr = ele.getAsJsonArray();
				int len = Math.min(arr.size(), types.length);
				Object[] args = new Object[len];
				for (int i = 0; i < len; i++)
					args[i] = gson.fromJson(arr.get(i), types[i]);
				return (T) args;
			}
			throw new IllegalArgumentException();
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	@Override
	public <T> T read(InputStream is, Class<?>... types) throws IOException {
		return read(new InputStreamReader(is), types);
	}

	@Override
	public void read(Reader reader, Writer writer, Class<?>... types) throws IOException {
		write(writer, read(reader, types));
	}

	@Override
	public void read(InputStream is, OutputStream os, Class<?>... types) throws IOException {
		read(new InputStreamReader(is), new OutputStreamWriter(os), types);
	}

	@Override
	public boolean supportHTTPStream() {
		return true;
	}
}
