package net.butfly.albacore.serder.support;

import java.io.Serializable;
import java.nio.charset.Charset;

import org.apache.http.entity.ContentType;

public interface ContentTypeSerder extends Serializable {
	default ContentType contentType() {
		return ContentTypes.TEXT_PLAIN;
	}

	default void charset(Charset charset) {}
}
