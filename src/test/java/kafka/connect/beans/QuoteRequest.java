package kafka.connect.beans;

import java.util.Date;

/**
 *
 * @author Sanju Thomas
 *
 */
public class QuoteRequest{

	public QuoteRequest() {}

	public QuoteRequest(final String id, final String symbol, final int quantity, final Client client,
			final Date timestamp) {
		this.id = id;
		this.symbol = symbol;
		this.quantity = quantity;
		this.client = client;
		this.timestamp = timestamp;
	}


	private String id;
	private String symbol;
	private int quantity;
	private Client client;
	private Date timestamp;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}
	
	public Date getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Date timestamp) {
		this.timestamp = timestamp;
	}

	public Client getClient() {
		return this.client;
	}

	public void setClient(final Client client) {
		this.client = client;
	}

	public String getSymbol() {
		return this.symbol;
	}

	public void setSymbol(final String symbol) {
		this.symbol = symbol;
	}

	public int getQuantity() {
		return this.quantity;
	}

	public void setQuantity(final int quantity) {
		this.quantity = quantity;
	}

}
