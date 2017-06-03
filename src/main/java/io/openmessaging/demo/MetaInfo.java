package io.openmessaging.demo;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

//all meta info
public class MetaInfo extends HashMap<String, BucketMeta> {
	private static final long serialVersionUID = 8853036030285630462L;

	int queuesSize;
	int topicsSize;
	Set<String> queues, topics;

	/**
	 * the number of segment groups, each of which consists consecutive segments
	 * within the same bucket.
	 */
	int numMetaRecord;

	/**
	 * consecutive segments occurs
	 */
	int sequentialOccurs;
	int numTotalSegs;
	int numTotalMsgs;
	int numDataFiles;
	int numSuperSegs;
	HashMap<Integer, FileSuperSeg> fileSuperSegMap;

	/**
	 * the bucket partition values of sequential segments in the data store. we
	 * can partition the segments of the same bucket to the same encoder service
	 * for the message order. Use byte of its hashcode.
	 */
	// ArrayList<Byte> bucketParts = new ArrayList<>(90000);
	transient int cursorOfBucketParts = 0;

	public MetaInfo(int numBuckets) {
		super(numBuckets);
	}

	@Override
	public String toString() {
		StringBuffer sbq = new StringBuffer();
		for (String q : queues) {
			sbq.append(q);
			sbq.append(", ");
		}

		StringBuffer sbt = new StringBuffer();
		for (String t : topics) {
			sbt.append(t);
			sbt.append(", ");
		}
		return String.format(
				"queueSize = %d, topicsSize = %d, sequentialOccurs = %d, numTotalSegs = %d, numTotalMsgs = %d, numDataFiles = %d, numSuperSegs = %d, queues = %s, topics = %s",
				queuesSize, topicsSize, sequentialOccurs, numTotalSegs, numTotalMsgs, numDataFiles, numSuperSegs,
				sbq.toString(), sbt.toString());

	}

	// public void addBucketInfo(String bucket) {
	// bucketParts.add((byte) bucket.hashCode());
	// }
	//
	// public Byte getNextBucketPart() {
	// if (cursorOfBucketParts < bucketParts.size()) {
	// return bucketParts.get(cursorOfBucketParts++);
	// } else {
	// return null;
	// }
	// }
	//
	// public void setBucketParts(ArrayList<Byte> bucketParts) {
	// this.bucketParts = bucketParts;
	// }

	public HashMap<Integer, FileSuperSeg> getFileSuperSegMap() {
		return fileSuperSegMap;
	}

	public void setFileSuperSegMap(HashMap<Integer, FileSuperSeg> fileSuperSegMap) {
		this.fileSuperSegMap = fileSuperSegMap;
	}

	public int getSequentialOccurs() {
		return sequentialOccurs;
	}

	public void setSequentialOccurs(int sequentialOccurs) {
		this.sequentialOccurs = sequentialOccurs;
	}

	public int getNumTotalSegs() {
		return numTotalSegs;
	}

	public void setNumTotalSegs(int numTotalSegs) {
		this.numTotalSegs = numTotalSegs;
	}

	public Set<String> getQueues() {
		return queues;
	}

	public void setQueues(Set<String> queues) {
		this.queues = queues;
	}

	public Set<String> getTopics() {
		return topics;
	}

	public void setTopics(Set<String> topics) {
		this.topics = topics;
	}

	public int getQueuesSize() {
		return queuesSize;
	}

	public void setQueuesSize(int queuesSize) {
		this.queuesSize = queuesSize;
	}

	public int getTopicsSize() {
		return topicsSize;
	}

	public void setTopicsSize(int topicsSize) {
		this.topicsSize = topicsSize;
	}

	public int getNumTotalMsgs() {
		return numTotalMsgs;
	}

	public void setNumTotalMsgs(int numTotalMsgs) {
		this.numTotalMsgs = numTotalMsgs;
	}

	public int getNumDataFiles() {
		return numDataFiles;
	}

	public void setNumDataFiles(int numDataFiles) {
		this.numDataFiles = numDataFiles;
	}

	public int getNumSuperSegs() {
		return numSuperSegs;
	}

	public void setNumSuperSegs(int numSuperSegs) {
		this.numSuperSegs = numSuperSegs;
	}

	public int getNumMetaRecord() {
		return numMetaRecord;
	}

	public void setNumMetaRecord(int numMetaRecord) {
		this.numMetaRecord = numMetaRecord;
	}
}

class SequentialSegs implements Serializable {
	private static final long serialVersionUID = 7105778422113016149L;
	String bucket;
	int numSegs;

	public SequentialSegs(String bucket, int numSegs) {
		super();
		this.bucket = bucket;
		this.numSegs = numSegs;
	}
}

class FileSuperSeg implements Serializable {
	private static final long serialVersionUID = -5590252735101016136L;
	long compressedSize;
	int numSegsInSuperSeg;
	ArrayList<SequentialSegs> sequentialSegs;

	public FileSuperSeg(int numSegsInSuperSeg) {
		super();
		this.numSegsInSuperSeg = numSegsInSuperSeg;
		this.sequentialSegs = new ArrayList<>(Config.REQ_BATCH_COUNT_THRESHOLD);
	}
}

// One bucket, one segment meta
class BucketMeta implements Serializable {

	private static final long serialVersionUID = -867852685475432061L;

	int numSegs;

	/**
	 * index is the sequential number of message in the bucket. file is ID of
	 * the data file. offset is the segment offset in the file. Unit is one
	 * segment. length is the number of segments. |index, file, offset, length |
	 * index, file, offset, length | ... |
	 */
	ArrayList<Integer> content;
	public transient final static int META_INT_UNIT_SIZE = 4;

	BucketMeta() {
		content = new ArrayList<>(40 * META_INT_UNIT_SIZE);
	}

	/**
	 * Append the meta info of the bucket in one file
	 * 
	 * @param index
	 *            message index
	 * @param fileId
	 *            id of the disk file
	 * @param offset
	 *            segment unit offset in the file
	 * @param length
	 *            the number of segments
	 */
	public void addMetaRecord(int index, int fileId, int offset, int length) {
		content.add(index);
		content.add(fileId);
		content.add(offset);
		content.add(length);
	}

	public ArrayList<Integer> getContent() {
		return content;
	}

	public void setContent(ArrayList<Integer> content) {
		this.content = content;
	}

	public int getNumSegs() {
		return numSegs;
	}

	public int addNumSegs(int delta) {
		this.numSegs += delta;
		return this.numSegs;
	}

	public void setNumSegs(int numSegs) {
		this.numSegs = numSegs;
	}
}