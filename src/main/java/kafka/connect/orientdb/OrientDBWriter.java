package kafka.connect.orientdb;

import java.util.Collection;
import java.util.Map;

import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.orientechnologies.orient.core.record.impl.ODocument;

import kafka.connect.orientdb.sink.OrientDBSinkConfig;


/**
 * 
 * @author Sanju Thomas
 *
 */
public class OrientDBWriter implements Writer{
	
	private static final Logger logger = LoggerFactory.getLogger(OrientDBWriter.class);
	
	private final String host;
	private final String dbName;
	private final String username;
	private final String password;
	
	
	public OrientDBWriter(final Map<String, String> config){
	    host = config.get(OrientDBSinkConfig.ORIENTDB_HOST);
	    dbName = config.get(OrientDBSinkConfig.DATABASE_NAME);
	    username = config.get(OrientDBSinkConfig.CONNECTION_USER);
	    password = config.get(OrientDBSinkConfig.CONNECTION_PASSWORD);
	}
	

	@SuppressWarnings("unchecked")
    @Override
    public void write(final Collection<SinkRecord> records) {
    	
    		for(final SinkRecord record : records) {
			final Map<Object, Object> mapRecord = (Map<Object, Object>) record.value();
    			final ODocument document = new ODocument(record.topic(), mapRecord);
    		}
    	
    }
}
