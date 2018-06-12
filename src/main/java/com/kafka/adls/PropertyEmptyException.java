package com.kafka.adls;

public class PropertyEmptyException extends Exception {

	private static final long serialVersionUID = 1L;
	
	public PropertyEmptyException(String message) {
		super(message);
	}
}
