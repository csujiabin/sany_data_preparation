package model;

public class Slice {
	public int sliceNo;
	public long startTime;
	public long endTime;
	public int paraCount;
	
	public Slice(long start, long end) {
		startTime = start;
		endTime = end;
	}
}
