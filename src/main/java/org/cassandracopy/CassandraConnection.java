package org.cassandracopy;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.ConstantReconnectionPolicy;
import com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.datastax.driver.core.ProtocolOptions;

public class CassandraConnection {
	private String CASSANDRA_CONNECTION_PROPERTIES = "CassandraConnection.properties";

	private static final String DEFAULT_KEYSPACE = "DefaultKeyspace";
	private static final String USERNAME = "Username";
	private static final String PASSWORD = "Password";
	private static final String NODES = "Nodes";
	private static CassandraConnection instance;

	private static final Logger LOGGER = LoggerFactory
			.getLogger(CassandraConnection.class);

	private String[] nodes = { "", "" };
	private String defaultKeyspace = "";
	private String username = "";
	private String password = "";

	private Cluster cluster;
	private Map<String, Session> sessions = new HashMap<String, Session>();

	public void init() {
		readProperty();

		PoolingOptions poolingOptions = new PoolingOptions();
		Builder builder = Cluster.builder().withPoolingOptions(
				poolingOptions.setCoreConnectionsPerHost(HostDistance.LOCAL,
						poolingOptions
								.getMaxConnectionsPerHost(HostDistance.LOCAL)));

		cluster = builder
				.addContactPoints(nodes)
				.withRetryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE)
				.withReconnectionPolicy(new ConstantReconnectionPolicy(100L))
				.withCredentials(username, password).build();

/* Slight performance degradation using a TokenAware policy in this scenario.
 * Therefore taken this out.
		.withLoadBalancingPolicy(
				new TokenAwarePolicy(new RoundRobinPolicy()))
*/
		cluster.getConfiguration().getProtocolOptions()
				.setCompression(ProtocolOptions.Compression.LZ4);

		Metadata metadata = cluster.getMetadata();
		LOGGER.info(String.format("Connected to cluster: %s\n",
				metadata.getClusterName()));
		for (Host host : metadata.getAllHosts()) {
			LOGGER.info(String.format("Datatacenter: %s; Host: %s; Rack: %s\n",
					host.getDatacenter(), host.getAddress(), host.getRack()));
		}
		cluster.init();
	}

	public Session connect() {
		return cluster.connect(defaultKeyspace);
	}

	private Session connect(String keyspace) {
		return cluster.connect(keyspace);
	}

	public static Session getSession(String keyspace) {
		return getInstance().getInternalSession(keyspace);
	}

	public static Session getSession() {
		return getInstance().getInternalSession(
				getInstance().getDefaultKeyspace());
	}

	private Session getInternalSession(String keyspace) {

		if (!sessions.containsKey(keyspace)) {
			Session session = connect(keyspace);
			sessions.put(keyspace, session);
		}

		return sessions.get(keyspace);
	}

	public static CassandraConnection getInstance() {
		if (instance == null) {
			instance = new CassandraConnection();
			instance.init();
		}

		return instance;
	}

	public void close() {
		cluster.close();
	}

	private void readProperty() {
		Properties prop = new Properties();
		InputStream input = null;
		input = CassandraConnection.class.getClassLoader().getResourceAsStream(
				CASSANDRA_CONNECTION_PROPERTIES);

		try {
			// load a properties file
			prop.load(input);
		} catch (IOException e) {
			LOGGER.warn("Could not load Cassandra properties", e);
		}

		if (prop.getProperty(NODES) != null) {
			String nodesString = prop.getProperty(NODES);
			nodes = nodesString.split(",");
			for (int i = 0; i < nodes.length; i++) {
				nodes[i] = nodes[i].trim();
			}
			LOGGER.info("Property " + NODES + "=" + nodesString);
		}

		if (prop.getProperty(DEFAULT_KEYSPACE) != null) {
			defaultKeyspace = prop.getProperty(DEFAULT_KEYSPACE);
			LOGGER.info("Property " + DEFAULT_KEYSPACE + "=" + defaultKeyspace);
		}

		if (prop.getProperty(USERNAME) != null) {
			username = prop.getProperty(USERNAME);
			LOGGER.info("Property " + USERNAME + "=" + username);
		}

		if (prop.getProperty(PASSWORD) != null) {
			password = prop.getProperty(PASSWORD);
			LOGGER.info("Property " + PASSWORD + "=" + password);
		}
	}

	public String[] getNodes() {
		return nodes;
	}

	public void setNodes(String[] nodes) {
		this.nodes = nodes;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public Cluster getCluster() {
		return cluster;
	}

	public void setCluster(Cluster cluster) {
		this.cluster = cluster;
	}

	public String getDefaultKeyspace() {
		return defaultKeyspace;
	}

	public void setDefaultKeyspace(String defaultKeyspace) {
		this.defaultKeyspace = defaultKeyspace;
	}

	public void setCassandraConnectionPropertiesFile(String fileName) {
		CASSANDRA_CONNECTION_PROPERTIES = fileName;
	}

}
