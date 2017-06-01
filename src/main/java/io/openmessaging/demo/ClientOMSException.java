package io.openmessaging.demo;

import io.openmessaging.exception.OMSRuntimeException;

public class ClientOMSException extends OMSRuntimeException {

	private static final long serialVersionUID = -1552534791444600429L;
	public String message;
    public ClientOMSException(String message) {
        this.message = message;
    }
    public ClientOMSException(String message, Throwable throwable) {
        this.initCause(throwable);
        this.message = message;
    }

    @Override
    public String getMessage() {
        return message;
    }



}
