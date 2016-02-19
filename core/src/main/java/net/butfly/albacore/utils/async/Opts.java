package net.butfly.albacore.utils.async;

import com.google.common.base.Joiner;

import net.butfly.albacore.utils.async.Options.ForkMode;

public class Opts {
	private static final char MULTI_OPTS_SPLITTER = '|';

	public String format(Options options) {
		String[] fields = new String[7];
		fields[0] = Integer.toString(options.mode.ordinal());
		fields[1] = Long.toString(options.timeout);
		fields[2] = Boolean.toString(options.unblock);
		fields[3] = Integer.toString(options.repeat);
		fields[4] = Integer.toString(options.retry);
		fields[5] = Integer.toString(options.concurrence);
		fields[6] = Long.toString(options.interval);
		return Joiner.on(":").join(fields);
	}

	public Options parse(String format) {
		Options options = new Options();
		String[] fields = format.split(":");
		options.mode = ForkMode.values()[Integer.parseInt(fields[0])];
		options.timeout = Long.parseLong(fields[1]);
		options.unblock = Boolean.parseBoolean(fields[2]);
		options.repeat = Integer.parseInt(fields[3]);
		options.retry = Integer.parseInt(fields[4]);
		options.concurrence = Integer.parseInt(fields[5]);
		options.interval = Long.parseLong(fields[6]);
		return options;
	}

	protected void mode(Options opts, String mode) {
		opts.mode(ForkMode.valueOf(mode));
	}

	public String format(Options... options) {
		String[] opstrs = new String[options.length];
		for (int i = 0; i < opstrs.length; i++)
			opstrs[i] = format(options[i]);
		return Joiner.on(MULTI_OPTS_SPLITTER).join(opstrs);
	}

	public Options[] parses(String string) {
		String[] opstrs = string.split("\\|");
		Options[] options = new Options[opstrs.length];
		for (int i = 0; i < opstrs.length; i++)
			options[i] = parse(opstrs[i]);
		return options;
	}
}
