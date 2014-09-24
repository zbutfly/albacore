package net.butfly.albacore.utils.storage.swift.io;

import java.io.IOException;
import java.io.OutputStream;

public class UploadStream extends OutputStream {
	public UploadStream(String authUrl, String account, String password, String container, String objectName) {}

	public UploadStream(String authUrl, String account, String password, String container, String objectName, long bufferSize) {}

	@Override
	public void write(int arg0) throws IOException {
		// TODO Auto-generated method stub
	}

	@Override
	public void flush() throws IOException {
		// TODO Auto-generated method stub
	}
}
