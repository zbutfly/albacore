package net.butfly.albacore.serder;

import org.apache.http.entity.ContentType;

public abstract class TextSerderBase<PRESENT> extends ContentSerderBase<PRESENT, CharSequence> implements TextSerder<PRESENT> {
	private static final long serialVersionUID = -2918455325248020382L;

	public TextSerderBase() {
		enable(ContentType.DEFAULT_TEXT);
	}

	public TextSerderBase(ContentType... contentType) {
		super(contentType);
		enable(ContentType.DEFAULT_TEXT);
	}
}
