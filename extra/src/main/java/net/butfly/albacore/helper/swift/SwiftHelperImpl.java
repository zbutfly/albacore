package net.butfly.albacore.helper.swift;

import java.io.InputStream;

import net.butfly.albacore.helper.HelperBase;
import net.butfly.albacore.helper.SwiftHelper;
import net.butfly.albacore.helper.swift.exception.AuthenticationFailureException;
import net.butfly.albacore.helper.swift.exception.OperationFailureException;
import net.butfly.albacore.helper.swift.exception.UnknownResponseException;
import net.butfly.albacore.utils.storage.swift.SwiftContext;

import org.springframework.beans.factory.InitializingBean;

@Deprecated
public class SwiftHelperImpl extends HelperBase implements SwiftHelper, InitializingBean {
	private static final long serialVersionUID = -2409046854069215490L;
	private String authUrl;
	private String username;
	private String password;
	private SwiftContext context;

	public void setAuthUrl(String authUrl) {
		this.authUrl = authUrl;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		this.context = new SwiftContext(this.authUrl, this.username, this.password);
	}

	@Override
	public InputStream fetch(String container, String path) throws OperationFailureException, UnknownResponseException,
			AuthenticationFailureException {
		return this.context.cat(container, path, null);
	}

	@Override
	public void upload(InputStream srcStream, String dstContainer, String dstPath) throws OperationFailureException,
			UnknownResponseException, AuthenticationFailureException {
		this.context.cp(srcStream, dstContainer, dstPath);
	}

	@Override
	public void upload(InputStream srcStream, String dstContainer, String dstPath, long bytes)
			throws OperationFailureException, UnknownResponseException, AuthenticationFailureException {
		this.context.cp(srcStream, dstContainer, dstPath, bytes);
	}

	@Override
	public void delete(String container, String path) throws OperationFailureException, UnknownResponseException,
			AuthenticationFailureException {
		this.context.rm(container, path);
	}
}
