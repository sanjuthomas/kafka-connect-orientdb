package kafka.connect.orientdb;

import java.util.Collection;
import java.util.Map;

import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;

import kafka.connect.orientdb.sink.OrientDBSinkConfig;

/**
 * 
 * @author Sanju Thomas
 *
 */
public class OrientDBWriter implements Writer {

	private static final Logger logger = LoggerFactory.getLogger(OrientDBWriter.class);
	private static final ObjectMapper MAPPER = new ObjectMapper();

	private final String connectionString;
	private final String username;
	private final String password;

	public OrientDBWriter(final Map<String, String> config) {
		connectionString = config.get(OrientDBSinkConfig.CONNECTION_STRING);
		username = config.get(OrientDBSinkConfig.CONNECTION_USER);
		password = config.get(OrientDBSinkConfig.CONNECTION_PASSWORD);
	}

	@Override
	public void write(final Collection<SinkRecord> records) {

		logger.debug("number of records received to write {}", records.size());
		ODatabaseDocumentTx db = null;
		try {
			db = new ODatabaseDocumentTx(connectionString);
			db.open(username, password);
			db.begin();
			for (final SinkRecord r : records) {
				db.save(new ODocument(r.topic()).fromJSON(MAPPER.writeValueAsString(r.value())));
			}
			db.commit();
		}
		catch(JsonProcessingException e) {
			logger.error(e.getMessage(), e);
			throw new RuntimeException(e.getMessage(), e);
		}
		catch (Exception e) {
			logger.error(e.getMessage(), e);
			throw new RetriableException(e);
		} finally {
			if (null != db) {
				db.close();
			}
		}
	}
}
