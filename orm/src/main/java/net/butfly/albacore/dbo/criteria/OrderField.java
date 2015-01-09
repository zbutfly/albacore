package net.butfly.albacore.dbo.criteria;

public class OrderField {
	private String field;
	private boolean desc;
	private String ascv;

	public OrderField(String field) {
		this(field, false);
	}

	public OrderField(String field, boolean desc) {
		this.field = field;
		this.desc = desc;
		this.ascv = desc ? "DESC" : "ASC";
	}

	public String field() {
		return field;
	}

	public boolean desc() {
		return desc;
	}

	public String toString() {
		return field + " " + ascv;
	}
}
