package net.butfly.albacore.dbo.criteria;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class CriteriaMap extends HashMap<String, Object> {
	private static final long serialVersionUID = -3706761903287282271L;

	public enum QueryType {
		UNDEFINED, COUNT, LIST
	}

	private QueryType type;
	private List<OrderField> orderFields;

	public CriteriaMap() {
		super();
		this.type = QueryType.UNDEFINED;
		this.orderFields = new ArrayList<OrderField>();
	}

	public QueryType getType() {
		return type;
	}

	public void setType(QueryType type) {
		this.type = type;
	}

	public OrderField[] getOrderFields() {
		return orderFields.toArray(new OrderField[this.orderFields.size()]);
	}

	public void addOrder(String orderField, boolean asc) {
		this.orderFields.add(new OrderField(orderField, asc));
	}

	public void addOrder(String orderField) {
		this.orderFields.add(new OrderField(orderField));
	}
}
