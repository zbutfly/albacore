package com.greenpineyu.fel;

import com.greenpineyu.fel.common.FelBuilder;
import com.greenpineyu.fel.compile.CompileService;
import com.greenpineyu.fel.context.ArrayCtxImpl;
import com.greenpineyu.fel.context.FelContext;
import com.greenpineyu.fel.context.Var;
import com.greenpineyu.fel.function.FunMgr;
import com.greenpineyu.fel.function.Function;
import com.greenpineyu.fel.optimizer.Optimizer;
import com.greenpineyu.fel.optimizer.VarVisitOpti;
import com.greenpineyu.fel.parser.AntlrParser;
import com.greenpineyu.fel.parser.FelNode;
import com.greenpineyu.fel.parser.Parser;
import com.greenpineyu.fel.security.SecurityMgr;

/**
 * 执行引擎
 * 
 * @author yqs
 * 
 */
public class FelEngineImpl implements FelEngine {
	private FelContext context;
	private CompileService compiler;
	private Parser parser;
	private FunMgr funMgr;
	private SecurityMgr securityMgr;

	@Override
	public SecurityMgr getSecurityMgr() {
		return securityMgr;
	}

	@Override
	public void setSecurityMgr(SecurityMgr securityMgr) {
		this.securityMgr = securityMgr;
	}

	public FelEngineImpl(FelContext context) {
		this.context = context;
		compiler = new CompileService();
		parser = new AntlrParser(this);
		this.funMgr = new FunMgr();
	}

	{
		this.securityMgr = FelBuilder.newSecurityMgr();
	}

	public FelEngineImpl() {
		this(new ArrayCtxImpl());
		// this(new MapContext());
	}

	@Override
	public FelNode parse(String exp) {
		return parser.parse(exp);
	}

	@Override
	public Object eval(String exp) {
		return this.eval(exp, this.context);
	}

	public Object eval(String exp, Var... vars) {
		FelNode node = parse(exp);
		Optimizer opt = new VarVisitOpti(vars);
		node = opt.call(context, node);
		return node.eval(context);
	}

	@Override
	public Object eval(String exp, FelContext ctx) {
		return parse(exp).eval(ctx);
	}

	public Expression compile(String exp, Var... vars) {
		return compile(exp, null, new VarVisitOpti(vars));
	}

	@Override
	public Expression compile(String exp, FelContext ctx, Optimizer... opts) {
		if (ctx == null) {
			ctx = this.context;
		}
		FelNode node = parse(exp);
		if (opts != null) {
			for (Optimizer opt : opts) {
				if (opt != null) {
					node = opt.call(ctx, node);
				}
			}
		}
		return compiler.compile(ctx, node, exp);
	}

	@Override
	public String toString() {
		return "FelEngine";
	}

	@Override
	public void addFun(Function fun) {
		this.funMgr.add(fun);
	}

	@Override
	public FelContext getContext() {
		return this.context;
	}

	@Override
	public CompileService getCompiler() {
		return compiler;
	}

	@Override
	public void setCompiler(CompileService compiler) {
		this.compiler = compiler;
	}

	@Override
	public Parser getParser() {
		return parser;
	}

	@Override
	public void setParser(Parser parser) {
		this.parser = parser;
	}

	@Override
	public FunMgr getFunMgr() {
		return funMgr;
	}

	@Override
	public void setFunMgr(FunMgr funMgr) {
		this.funMgr = funMgr;
	}

	@Override
	public void setContext(FelContext context) {
		this.context = context;
	}

}
