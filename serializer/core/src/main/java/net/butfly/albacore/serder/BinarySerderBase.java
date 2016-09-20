package net.butfly.albacore.serder;

import org.apache.http.entity.ContentType;

public abstract class BinarySerderBase<PRESENT> extends ContentSerderBase<PRESENT, byte[]> implements BinarySerder<PRESENT> {
	private static final long serialVersionUID = -2918455325248020382L;

	public BinarySerderBase() {
		super(ContentType.DEFAULT_BINARY);
	}

	public BinarySerderBase(ContentType... contentType) {
		super(ContentType.DEFAULT_BINARY, contentType);
	}

	protected BinarySerderBase(ContentType base, ContentType[] supported) {
		super(base, supported);
	}
}
