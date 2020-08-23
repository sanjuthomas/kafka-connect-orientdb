package com.sanjuthomas.orientdb.bean;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * @author Sanju Thomas
 */
@AllArgsConstructor
@Getter
public class Client {
	private String id;
	private Account account;
}