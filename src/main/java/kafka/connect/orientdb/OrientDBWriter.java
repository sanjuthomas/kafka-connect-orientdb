package kafka.connect.orientdb;

import java.util.Collection;
import java.util.Map;

import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

	private final String connectionString;
	private final String username;
	private final String password;

	public OrientDBWriter(final Map<String, String> config) {
		connectionString = config.get(OrientDBSinkConfig.CONNECTION_STRING);
		username = config.get(OrientDBSinkConfig.CONNECTION_USER);
		password = config.get(OrientDBSinkConfig.CONNECTION_PASSWORD);
	}

	@SuppressWarnings({ "unchecked", "resource" })
	@Override
	public void write(final Collection<SinkRecord> records) {

		ODatabaseDocumentTx db = null;
		try {
			db = new ODatabaseDocumentTx(connectionString).open(username, password);
			db.begin();
			for (final SinkRecord record : records) {
				final Map<Object, Object> mapRecord = (Map<Object, Object>) record.value();
				final ODocument document = new ODocument(record.topic(), mapRecord);
				db.save(document);
			}

		} catch (Exception e) {
			db.rollback();
			logger.error(e.getMessage(), e);
			throw new RetriableException(e);
		} finally {
			if (null != db) {
				db.commit();
				db.close();
			}
		}
	}
}
