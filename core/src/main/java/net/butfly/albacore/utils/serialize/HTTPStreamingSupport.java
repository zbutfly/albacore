package net.butfly.albacore.utils.serialize;

public abstract class HTTPStreamingSupport {
	public static final String DEFAULT_CONTENT_TYPE = "text/plain; charset=utf-8";

	public boolean supportHTTPStream() {
		return true;
	}

	public String[] getContentTypes() {
		return new String[] { DEFAULT_CONTENT_TYPE };
	}

	public String getOutputContentType() {
		return this.getContentTypes()[0];
	};
}
