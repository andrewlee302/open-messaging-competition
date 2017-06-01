package io.openmessaging.demo;

public class SegmentFullException extends Exception {

	private static final long serialVersionUID = 5662752401348616113L;
	
	/**
	 * meta is not enough
	 */
	boolean metaNotEnought = true;

	public SegmentFullException(boolean metaNotEnought) {
		super();
		this.metaNotEnought = metaNotEnought;
	}

}