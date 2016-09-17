package net.butfly.albacore.serializer;

import java.util.Set;

import org.apache.http.entity.ContentType;

import com.google.common.base.Charsets;

public interface ContentSerializer<D> extends Serializer<D> {
	default ContentType contentType() {
		return ContentType.WILDCARD.withCharset(Charsets.UTF_8);
	}

	ContentType contentType(String mimeType);

	Set<String> supportedMimeTypes();
}
