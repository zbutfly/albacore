package net.butfly.albacore.utils.storage.swift.io;

import java.io.IOException;
import java.io.InputStream;

public class DownloadStream extends InputStream {
	private final static long DEFAULT_BUFFER_SIZE = 4096;

	public DownloadStream(String authUrl, String account, String password, String container, String objectName) {
		this(authUrl, account, password, container, objectName, DEFAULT_BUFFER_SIZE);
	}

	public DownloadStream(String authUrl, String account, String password, String container, String objectName, long bufferSize) {}

	@Override
	public int read() throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
	}
}
