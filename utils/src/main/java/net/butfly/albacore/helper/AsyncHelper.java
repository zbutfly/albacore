package net.butfly.albacore.helper;

import net.butfly.albacore.helper.async.AsyncTaskBase;
import net.butfly.albacore.helper.async.AsyncTaskException;

public interface AsyncHelper extends Helper {
	Thread execute(AsyncTaskBase task) throws AsyncTaskException;
}
