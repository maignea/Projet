package com.runmycode.mapred;


public class DataIndex 
{	
	private JobID jobID;
	private String offset;
	private int row;
	private int column = 0;
	private int length;
	private boolean isMap;
	
	public DataIndex(JobID jobID) 
	{
		this.jobID = jobID;		
	}

	public DataIndex() 
	{
		
	}
	
	public String getOffset() 
	{
		return offset;
	}
	
	public void setOffset(String offset) 
	{
		this.offset = offset;
	}
	
	public int getRow() 
	{
		return row;
	}
	
	public void setRow(int row) 
	{
		this.row = row;
	}
	
	public int getLength() 
	{
		return length;
	}
	
	public void setLength(int length) 
	{
		this.length = length;
	}

	public JobID getJobID() 
	{
		return jobID;
	}

	public void setJobID(JobID jobID) 
	{
		this.jobID = jobID;
	}


	public boolean isMap() 
	{
		return isMap;
	}

	public void setMap(boolean isMap) 
	{
		this.isMap = isMap;
	}

	public int getColumn() 
	{
		return column;
	}

	public void setColumn(int column) 
	{
		this.column = column;
	}
}
