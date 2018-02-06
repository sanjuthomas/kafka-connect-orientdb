package kafka.connect.orientdb;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.iterator.ORecordIteratorClass;
import com.orientechnologies.orient.core.record.impl.ODocument;

import kafka.connect.IntegrationTest;
import kafka.connect.beans.Account;
import kafka.connect.beans.Client;
import kafka.connect.beans.QuoteRequest;
import kafka.connect.orientdb.sink.OrientDBSinkConfig;

/**
 * 
 * @author Sanju Thomas
 *
 */
@Category(IntegrationTest.class)
public class TestOrientDBWriter {
	
	private Writer writer;
	private final Map<String, String> conf = new HashMap<>();
	private static final ObjectMapper MAPPER = new ObjectMapper();
	private ODatabaseDocumentTx db;
	
	@SuppressWarnings("resource")
	@Before
    public void setup(){
        conf.put(OrientDBSinkConfig.CONNECTION_STRING, "plocal:/tmp/databases/petshop");
        conf.put(OrientDBSinkConfig.CONNECTION_USER, "admin");
        conf.put(OrientDBSinkConfig.CONNECTION_PASSWORD, "admin");
        writer = new OrientDBWriter(conf);
        db = new ODatabaseDocumentTx (conf.get(OrientDBSinkConfig.CONNECTION_STRING)).create();
        db.save(new ODocument("Topic"));
        db.close();
    }
	
	@After
	public void tearDown() {
		db = new ODatabaseDocumentTx(conf.get(OrientDBSinkConfig.CONNECTION_STRING));
		db.open(conf.get(OrientDBSinkConfig.CONNECTION_USER), conf.get(OrientDBSinkConfig.CONNECTION_PASSWORD));
		db.drop();
	}
	
	@Test
	public void shouldWrite() {
		
		final List<SinkRecord> documents = new ArrayList<SinkRecord>();
		final Account account = new Account("A1");
		final Client client = new Client("C1", account);
		final QuoteRequest quoteRequest = new QuoteRequest("Q1", "APPL", 100, client, new Date());
		
		documents.add(new SinkRecord("Topic", 1, null, null, null, MAPPER.convertValue(quoteRequest, Map.class), 0));
		writer.write(documents);
		
		final ODatabaseDocumentTx db = new ODatabaseDocumentTx(conf.get(OrientDBSinkConfig.CONNECTION_STRING));
		db.open(conf.get(OrientDBSinkConfig.CONNECTION_USER), conf.get(OrientDBSinkConfig.CONNECTION_PASSWORD));
			
	    final ORecordIteratorClass<ODocument> topics = db.browseClass("Topic");
	    final Iterator<ODocument> i = topics.iterator();
	    while(i.hasNext()) {
	    		ODocument next = i.next();
			assertEquals(0, next.fieldNames().length);
			next = i.next();
	    		assertEquals(5, next.fieldNames().length);
	    		assertEquals("Topic", next.getClassName());
	    		assertEquals("Q1", next.field("id"));
	    }
	    db.close();
	}

}
