package com.github.brainlag.nsq.exceptions;

public class DisconnectedException extends NSQException {

	public DisconnectedException(String message, Throwable cause) {
		super(message, cause);
	}
}
