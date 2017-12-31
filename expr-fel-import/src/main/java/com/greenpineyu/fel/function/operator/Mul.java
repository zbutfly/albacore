package com.greenpineyu.fel.function.operator;

import java.util.List;

import com.greenpineyu.fel.common.NumberUtil;
import com.greenpineyu.fel.common.ReflectUtil;
import com.greenpineyu.fel.compile.FelMethod;
import com.greenpineyu.fel.compile.SourceBuilder;
import com.greenpineyu.fel.context.FelContext;
import com.greenpineyu.fel.exception.CompileException;
import com.greenpineyu.fel.exception.EvalException;
import com.greenpineyu.fel.function.StableFunction;
import com.greenpineyu.fel.parser.FelNode;

/**
 * 包名 .script.function.operator 类名 MultiplicativeOperator.java 创建日期 Oct 26, 20102:47:15 PM 作者 版权
 */
public class Mul extends StableFunction {
	@Override
	public Object call(FelNode node, FelContext context) {
		List<FelNode> children = node.getChildren();
		if (children.size() == 2) {
			FelNode left = children.get(0);
			Object leftValue = left.eval(context);
			FelNode right = children.get(1);
			Object rightValue = right.eval(context);
			if (leftValue instanceof Number && rightValue instanceof Number) {
				double l = NumberUtil.toDouble(leftValue);
				double r = NumberUtil.toDouble(rightValue);
				// Object calc = null;
				return calc(l, r);
				// throw new EvalException("执行"+this.operator+"出错，未知的操作符");
			}
			throw new EvalException("执行" + this.getName() + "出错，参数必须是数值型");
		}
		throw new EvalException("执行" + this.getName() + "出错，参数数量必须为2。");
	}

	Object calc(double l, double r) {
		return NumberUtil.parseNumber(l * r);
	}

	@Override
	public String getName() {
		return "*";
	}

	@Override
	public FelMethod toMethod(FelNode node, FelContext ctx) {
		String code = "";
		FelNode left = node.getChildren().get(0);
		FelNode right = node.getChildren().get(1);
		SourceBuilder lm = left.toMethod(ctx);
		Class<?> leftType = lm.returnType(ctx, left);

		SourceBuilder rm = right.toMethod(ctx);
		Class<?> rightType = lm.returnType(ctx, right);
		Class<?> type = null;
		if (ReflectUtil.isPrimitiveOrWrapNumber(leftType) && ReflectUtil.isPrimitiveOrWrapNumber(rightType)) {
			type = NumberUtil.arithmeticClass(leftType, rightType);
		} else {
			throw new CompileException("不支持的类型[" + ReflectUtil.getClassName(leftType) + "、" + ReflectUtil.getClassName(rightType) + "]。["
					+ this.getName() + "]运算只支持数值类型");
		}
		code = "(" + lm.source(ctx, left) + ")" + this.getName() + "(" + rm.source(ctx, right) + ")";
		FelMethod m = new FelMethod(type, code);
		return m;
	}

	@Override
	public boolean stable() {
		return true;
	}

}
