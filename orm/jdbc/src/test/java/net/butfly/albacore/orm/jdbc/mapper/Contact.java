package net.butfly.albacore.orm.jdbc.mapper;

public class Contact extends ContactKey {
	private static final long serialVersionUID = 4480350096757054202L;

	String content;

	@Override
	public ContactKey getId() {
		return super.getId();
	}

	@Override
	public void setId(ContactKey id) {
		super.setId(id);
	}

	public Contact() {
		super();
	}

	public Contact(String userId, ContactKey.Type type, String content) {
		super(userId, type);
		this.content = content;
	}

	public String getContent() {
		return content;
	}

	public void setContent(String content) {
		this.content = content;
	}
}
