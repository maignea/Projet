package com.runmycode.mapred;

public class TaskInProgressID extends ID
{
	private JobID jobID;

	/**
	 * Constructs a TaskInProgressID object from given {@link JobID}.  
	 * @param jobId JobID that this tip belongs to 
	 * @param id the tip number
	 */
	public TaskInProgressID(JobID jobId, String id) 
	{
		super(id);
		if(jobId == null)
		{
			throw new IllegalArgumentException("jobId cannot be null");
		}
		this.jobID = jobId;
	}
	
	public TaskInProgressID()
	{
		jobID = new JobID();
	}
	
	/** Returns the {@link JobID} object that this tip belongs to */	 
	public JobID getJobID()
	{	    		 
		return jobID;
	}
	
	@Override
	public boolean equals(Object o)
	{
		if(!super.equals(o))
		{
			return false;
		}
		TaskInProgressID that = (TaskInProgressID) o;
		return this.jobID.equals(that.jobID);
		
	}
	
	/** Construct a TaskID object from given string 
	 * @return constructed TaskID object or null if the given String is null
	 * @throws IllegalArgumentException if the given string is malformed
	 */
	public static TaskInProgressID forName(String str) throws IllegalArgumentException 
	{
		if(str == null)
		{
			throw new IllegalArgumentException("str shloud not be null");
		}
		String[] parts = str.split("_");
		JobID jobID = new JobID(parts[0]+parts[1]+parts[2]);
		return new TaskInProgressID(jobID, parts[3]);
	}
}
