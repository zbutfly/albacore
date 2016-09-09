package net.butfly.albacore.helper;

import java.io.InputStream;

import net.butfly.albacore.helper.Helper;
import net.butfly.albacore.helper.swift.exception.AuthenticationFailureException;
import net.butfly.albacore.helper.swift.exception.OperationFailureException;
import net.butfly.albacore.helper.swift.exception.UnknownResponseException;

public interface SwiftHelper extends Helper {
	InputStream fetch(String container, String path)
			throws OperationFailureException, UnknownResponseException, AuthenticationFailureException;

	void upload(InputStream srcStream, String dstContainer, String dstPath)
			throws OperationFailureException, UnknownResponseException, AuthenticationFailureException;

	void upload(InputStream srcStream, String dstContainer, String dstPath, long bytes)
			throws OperationFailureException, UnknownResponseException, AuthenticationFailureException;

	void delete(String container, String path) throws OperationFailureException, UnknownResponseException, AuthenticationFailureException;
}
