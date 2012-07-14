package com.runmycode.mapred;

public class TaskID extends ID
{
	private JobID jobID;

	/**
	 * Constructs a TaskID object from given {@link JobID}.  
	 * @param jobId JobID that this tip belongs to 
	 * @param id the tip number
	 */
	public TaskID(JobID jobId, String id) 
	{
		super(id);
		if(jobId == null)
		{
			throw new IllegalArgumentException("jobId cannot be null");
		}
		this.jobID = jobId;
	}
	
	public TaskID()
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
		TaskID that = (TaskID) o;
		return this.jobID.equals(that.jobID);
		
	}
	
	/** Construct a TaskID object from given string 
	 * @return constructed TaskID object or null if the given String is null
	 * @throws IllegalArgumentException if the given string is malformed
	 */
	public static TaskID forName(String str) throws IllegalArgumentException 
	{
		return null;
	}
}
