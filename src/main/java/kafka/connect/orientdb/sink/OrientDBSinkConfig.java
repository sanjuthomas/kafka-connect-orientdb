package kafka.connect.orientdb.sink;

import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author Sanju Thomas
 *
 */
public class OrientDBSinkConfig extends AbstractConfig {
    
    private static final Logger logger = LoggerFactory.getLogger(OrientDBSinkConfig.class);
	
	public static final String ORIENTDB_HOST = "orientdb.host";
	private static final String ORIENTDB_HOST_DOC = "orientdb host ip";
	
	public static final String ORIENTDB_PORT = "orientdb.port";
    private static final String ORIENTDB_PORT_DOC = "orientdb port";
    
	public static final String CONNECTION_USER = "orientdb.user";
	private static final String CONNECTION_USER_DOC = "orientdb connection user.";

	public static final String CONNECTION_PASSWORD = "orientdb.password";
	private static final String CONNECTION_PASSWORD_DOC = "orientdb connection password";
	
	public static final String DATABASE_NAME = "orientdb.database.name";
	private static final String DATABASE_NAME_DOC = "orientdb database name";
	
	public static final String COLLECTION_NAME = "orientdb.collection.name";
	private static final String  COLLECTION_NAME_DOC = "orientdb collection name";

	public static final String BATCH_SIZE = "orientdb.batch.size";
	private static final String BATCH_SIZE_DOC = "orientdb batch size";
	
	public static final String WRITER_IMPL = "orientdb.writer.impl";
	private static final String WRITER_IMPL_DOC = "orientdb writer impl";
	
	public static final String MAX_RETRIES = "orientdb.max.retries";
	private static final String MAX_RETRIES_DOC =  "The maximum number of times to retry on errors/exception before failing the task.";
	
	public static final String RETRY_BACKOFF_MS = "retry.backoff.ms";
    private static final int RETRY_BACKOFF_MS_DEFAULT = 10000;
	private static final String RETRY_BACKOFF_MS_DOC = "The time in milliseconds to wait following an error/exception before a retry attempt is made.";
	
	public static ConfigDef CONFIG_DEF = new ConfigDef()
			.define(ORIENTDB_HOST, Type.STRING, Importance.HIGH, ORIENTDB_HOST_DOC)
			.define(ORIENTDB_PORT, Type.INT, Importance.HIGH, ORIENTDB_PORT_DOC)
			.define(CONNECTION_USER, Type.STRING, Importance.HIGH, CONNECTION_USER_DOC)
			.define(CONNECTION_PASSWORD, Type.STRING, Importance.LOW, CONNECTION_PASSWORD_DOC)
			.define(BATCH_SIZE, Type.INT, Importance.MEDIUM, BATCH_SIZE_DOC)
			.define(MAX_RETRIES, Type.INT, Importance.MEDIUM, MAX_RETRIES_DOC)
			.define(DATABASE_NAME, Type.STRING, Importance.MEDIUM, DATABASE_NAME_DOC)
			.define(COLLECTION_NAME, Type.STRING, Importance.MEDIUM, COLLECTION_NAME_DOC)
			.define(WRITER_IMPL, Type.STRING, Importance.MEDIUM, WRITER_IMPL_DOC)
			.define(RETRY_BACKOFF_MS, Type.INT, RETRY_BACKOFF_MS_DEFAULT, Importance.MEDIUM, RETRY_BACKOFF_MS_DOC);

	public OrientDBSinkConfig(final Map<?, ?> originals) {
		
		super(CONFIG_DEF, originals, false);
		logger.info("Original Configs {}", originals);
	}

}
