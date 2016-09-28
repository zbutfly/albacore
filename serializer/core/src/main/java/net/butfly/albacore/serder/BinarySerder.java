package net.butfly.albacore.serder;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.google.common.reflect.TypeToken;

import net.butfly.albacore.serder.support.ByteArray;

public interface BinarySerder<PRESENT> extends ContentSerder<PRESENT, ByteArray> {
	<T extends PRESENT> void ser(T from, OutputStream to) throws IOException;

	<T extends PRESENT> T der(InputStream from, TypeToken<T> to) throws IOException;
}
