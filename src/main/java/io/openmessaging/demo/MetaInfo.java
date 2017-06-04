package io.openmessaging.demo;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

//all meta info
public class MetaInfo implements Serializable {
	private static final long serialVersionUID = 8853036030285630462L;

	int queuesSize;
	int topicsSize;
	Set<String> queues, topics;

	/**
	 * the number of segment groups, each of which consists consecutive segments
	 * within the same bucket.
	 */
	int[] numMetaRecords;

	/**
	 * consecutive segments occurs
	 */
	int[] sequentialOccurs;
	int[] numTotalSegs;
	int[] numDataFiles;
	int[] numSuperSegs;
	HashMap<Integer, FileSuperSeg>[] fileSuperSegMaps;
	HashMap<String, BucketMeta> bucketMetaMap;

	/**
	 * the bucket partition values of sequential segments in the data store. we
	 * can partition the segments of the same bucket to the same encoder service
	 * for the message order. Use byte of its hashcode.
	 */
	// ArrayList<Byte> bucketParts = new ArrayList<>(90000);
	transient int cursorOfBucketParts = 0;

	public MetaInfo() {
		numMetaRecords = new int[Config.PARTITION_NUM];
		sequentialOccurs = new int[Config.PARTITION_NUM];
		numTotalSegs = new int[Config.PARTITION_NUM];
		numDataFiles = new int[Config.PARTITION_NUM];
		numSuperSegs = new int[Config.PARTITION_NUM];
		fileSuperSegMaps = new HashMap[Config.PARTITION_NUM];
		bucketMetaMap = new HashMap<>();
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
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < Config.PARTITION_NUM; i++) {
			sb.append(String.format(
					"queueSize = %d, topicsSize = %d, sequentialOccurs = %d, numTotalSegs = %d, numDataFiles = %d, numSuperSegs = %d, queues = %s, topics = %s",
					queuesSize, topicsSize, sequentialOccurs[i], numTotalSegs[i], numDataFiles[i], numSuperSegs[i],
					sbq.toString(), sbt.toString()));
		}
		return sb.toString();

	}

	public HashMap<Integer, FileSuperSeg> getFileSuperSegMap(int rank) {
		return fileSuperSegMaps[rank];
	}

	public void setFileSuperSegMap(int rank, HashMap<Integer, FileSuperSeg> fileSuperSegMap) {
		this.fileSuperSegMaps[rank] = fileSuperSegMap;
	}

	public int getSequentialOccurs(int rank) {
		return sequentialOccurs[rank];
	}

	public void setSequentialOccurs(int rank, int sequentialOccurs) {
		this.sequentialOccurs[rank] = sequentialOccurs;
	}

	public int getNumTotalSegs(int rank) {
		return numTotalSegs[rank];
	}

	public void setNumTotalSegs(int rank, int numTotalSegs) {
		this.numTotalSegs[rank] = numTotalSegs;
	}

	public int getNumDataFiles(int rank) {
		return numDataFiles[rank];
	}

	public void setNumDataFiles(int rank, int numDataFiles) {
		this.numDataFiles[rank] = numDataFiles;
	}

	public int getNumSuperSegs(int rank) {
		return numSuperSegs[rank];
	}

	public void setNumSuperSegs(int rank, int numSuperSegs) {
		this.numSuperSegs[rank] = numSuperSegs;
	}

	public int getNumMetaRecord(int rank) {
		return numMetaRecords[rank];
	}

	public void setNumMetaRecord(int rank, int numMetaRecord) {
		this.numMetaRecords[rank] = numMetaRecord;
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