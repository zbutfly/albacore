package com.greenpineyu.fel.function.operator;

import java.util.List;

import com.greenpineyu.fel.FelEngine;
import com.greenpineyu.fel.common.NumberUtil;
import com.greenpineyu.fel.common.ReflectUtil;
import com.greenpineyu.fel.compile.FelMethod;
import com.greenpineyu.fel.compile.SourceBuilder;
import com.greenpineyu.fel.context.FelContext;
import com.greenpineyu.fel.function.Function;
import com.greenpineyu.fel.function.TolerantFunction;
import com.greenpineyu.fel.parser.FelNode;
import com.greenpineyu.fel.parser.Stable;

/**
 * 包名 .script.function.operator 类名 RelationalOperator.java 创建日期 Oct 26, 20103:04:25 PM 作者 版权
 */
public class LessThen implements Stable, Function {

	// private final String operator;

	// private RelationalOperator(String operator) {
	// this.operator = operator;
	// }
	//
	// public static final String LESSTHEN_STR = "<";
	// public static final String GREATERTHAN_STR = ">";
	// public static final String LESSTHENOREQUALS_STR = "<=";
	// public static final String GREATERTHANOREQUALS_STR = ">=";
	//
	// public static final RelationalOperator LESSTHEN;
	// public static final RelationalOperator GREATERTHAN;
	// public static final RelationalOperator LESSTHENOREQUALS;
	// public static final RelationalOperator GREATERTHANOREQUALS;

	// static {
	// LESSTHEN = new RelationalOperator(LESSTHEN_STR);
	// GREATERTHAN = new RelationalOperator(GREATERTHAN_STR);
	// LESSTHENOREQUALS = new RelationalOperator(LESSTHENOREQUALS_STR);
	// GREATERTHANOREQUALS = new RelationalOperator(GREATERTHANOREQUALS_STR);
	// }

	@Override
	public Object call(FelNode node, FelContext context) {
		List<FelNode> children = node.getChildren();
		if (children != null && children.size() == 2) {
			Object left = TolerantFunction.eval(context, children.get(0));
			Object right = TolerantFunction.eval(context, children.get(1));
			return compare(left, right);
		}
		throw new NullPointerException("传入参数数组为空或者参数个数不正确!");
	}

	// @Override
	// public Object call(Object[] arguments) {
	// boolean result = false;
	// if(arguments != null && arguments.length == 2){
	// Object left = arguments[0];
	// Object right = arguments[1];
	// if(this == LESSTHEN){
	// result = lessThan(left, right);
	// }else if(this == GREATERTHAN){
	// result = greaterThan(left, right);
	// }else if(this == LESSTHENOREQUALS){
	// result = lessThanOrEqual(left, right);
	// }else if(this == GREATERTHANOREQUALS){
	// result = greaterThanOrEqual(left, right);
	// }
	// return new Boolean(result);
	// }
	// throw new NullPointerException("传入参数数组为空或者参数个数不正确!");
	// }

	/**
	 * 小于
	 * 
	 * @param left
	 * @param right
	 * @return
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public boolean compare(Object left, Object right) {
		if (left == right) return false;

		if (left == null || right == null) return false;

		if (left instanceof Number && right instanceof Number) return NumberUtil.toDouble((Number) left) < NumberUtil.toDouble(
				(Number) right);

		if (left instanceof Comparable && right instanceof Comparable) return ((Comparable) left).compareTo(right) < 0;
		throw new IllegalArgumentException("参数[type:" + left.getClass() + ";value:" + left + "]和参数[type:" + right.getClass() + ";value:"
				+ right + "]不能进行比较[" + getName() + "]运算");
	}

	public StringBuilder buildRelationExpr(FelNode node, FelContext ctx, String operator) {
		List<FelNode> child = node.getChildren();
		FelNode leftNode = child.get(0);
		FelNode rightNode = child.get(1);
		SourceBuilder leftM = leftNode.toMethod(ctx);
		SourceBuilder rightM = rightNode.toMethod(ctx);
		Class<?> leftType = leftM.returnType(ctx, leftNode);
		Class<?> rightType = rightM.returnType(ctx, rightNode);
		String left = "(" + leftM.source(ctx, leftNode) + ")";
		String right = "(" + rightM.source(ctx, rightNode) + ")";

		StringBuilder sb = new StringBuilder();
		if (ReflectUtil.isFloatType(leftType) && ReflectUtil.isDoubleType(rightType)) {
			// 将float转在double会有精度问题，要特殊处理
			sb.append("NumberUtil.toDouble(" + left + ")");
			sb.append(operator);
			sb.append(right);
		} else if (ReflectUtil.isFloatType(rightType) && ReflectUtil.isDoubleType(leftType)) {
			// 将float转在double会有精度问题，要特殊处理
			sb.append(left);
			sb.append(operator);
			sb.append("NumberUtil.toDouble(" + right + ")");

		} else if (ReflectUtil.isPrimitiveOrWrapNumber(leftType) && ReflectUtil.isPrimitiveOrWrapNumber(rightType)) {
			sb.append(left);
			sb.append(operator);
			sb.append(right);
		} else
		/*
		 * // 只要有一个是数值型，就将另一个也转成值型。 if (Number.class.isAssignableFrom(leftType)) { sb.append(left); sb.append(operator);
		 * appendNumber(rightType, right, sb); } else if (Number.class.isAssignableFrom(rightType)) { appendNumber(leftType, left, sb);
		 * sb.append(operator); sb.append(right); } else
		 */
		if (Comparable.class.isAssignableFrom(leftType) && Comparable.class.isAssignableFrom(rightType)) {
			sb.append("NumberUtil.compare(" + left + "," + right + ")" + operator + "0");
		} else {
			throw new UnsupportedOperationException("类型" + leftType + "与类型" + rightType + "不支持比较操作。");
		}
		return sb;
	}

	@Override
	public String getName() {
		return "<";
	}

	@Override
	public SourceBuilder toMethod(FelNode node, FelContext ctx) {
		StringBuilder code = buildRelationExpr(node, ctx, this.getName());
		return new FelMethod(Boolean.class, code.toString());
	}

	@Override
	public boolean stable() {
		return true;
	}

	public static void main(String[] args) {
		FelEngine engine = FelEngine.instance;
		System.out.println(engine.eval("6>=5"));

	}

}
