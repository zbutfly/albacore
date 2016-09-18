package net.butfly.albacore.serder;

import java.util.Set;

import org.apache.http.entity.ContentType;

import com.google.common.base.Charsets;

public interface ContentSerder<S, D> extends ArrableSerder<S, D> {
	default ContentType contentType() {
		return ContentType.WILDCARD.withCharset(Charsets.UTF_8);
	}

	ContentType contentType(String mimeType);

	Set<String> supportedMimeTypes();
}
