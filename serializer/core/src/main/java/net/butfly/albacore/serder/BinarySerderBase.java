package net.butfly.albacore.serder;

import org.apache.http.entity.ContentType;

public abstract class BinarySerderBase<PRESENT> extends ContentSerderBase<PRESENT, byte[]> implements BinarySerder<PRESENT> {
	private static final long serialVersionUID = -2918455325248020382L;

	public BinarySerderBase(ContentType... contentType) {
		super(contentType);
		enable(ContentType.DEFAULT_BINARY);
	}
}
