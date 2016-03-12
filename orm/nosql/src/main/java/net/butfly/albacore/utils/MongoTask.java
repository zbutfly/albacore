package net.butfly.albacore.utils;

import com.mongodb.ConnectionString;

public abstract class MongoTask {
	private ConnectionString conn;

	public MongoTask(ConnectionString conn) {
		this.conn = conn;
	}

	abstract protected void executeAsync(com.mongodb.async.client.MongoDatabase db);

	abstract protected void execute(com.mongodb.client.MongoDatabase db);

	public void execute() {
		this.execute(Instances.fetch(() -> new com.mongodb.MongoClient(new com.mongodb.MongoClientURI(MongoTask.this.conn.toString())),
				MongoTask.this.conn).getDatabase(MongoTask.this.conn.getDatabase()));
	}

	public void executeAsync() {
		this.executeAsync(Instances.fetch(() -> com.mongodb.async.client.MongoClients.create(MongoTask.this.conn), MongoTask.this.conn)
				.getDatabase(MongoTask.this.conn.getDatabase()));
	}

	public static ConnectionString buildConnectionString(String host, int port, String db, String username, String password) {
		// mongodb://[username:password@]host1[:port1][,host2[:port2],...[,hostN[:portN]]][/[database][?options]]
		StringBuilder sb = new StringBuilder("mongodb://");
		if (!Texts.isEmpty(username) && !Texts.isEmpty(password)) sb.append(username).append(":").append("@");
		sb.append(host);
		if (port >= 0) sb.append(":").append(port);
		if (!Texts.isEmpty(db)) sb.append("/").append(db);
		return new ConnectionString(sb.toString());
	}
}