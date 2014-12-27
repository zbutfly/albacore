package net.butfly.albacore.dbo;

import java.util.ArrayList;
import java.util.List;

import net.butfly.albacore.dbo.criteria.Criteria;

public class MongoCriteria extends Criteria {
	private static final long serialVersionUID = -6226474049518759675L;
	protected List<OrderField> orderFields;

	public MongoCriteria() {
		super();
		this.orderFields = new ArrayList<OrderField>();
	};

	public List<OrderField> getOrderFields() {
		return orderFields;
	}

	public MongoCriteria addOrder(String orderField, boolean asc) {
		this.orderFields.add(new OrderField(orderField, asc));
		return this;
	}

	public static final class OrderField {
		private String field;
		private boolean asc;
		private String ascv;

		private OrderField(String field, boolean asc) {
			this.field = field;
			this.asc = asc;
			this.ascv = asc ? "ASC" : "DESC";
		}

		public String getField() {
			return field;
		}

		public String getAsc() {
			return ascv;
		}

		public boolean desc() {
			return !asc;
		}
	}
}
