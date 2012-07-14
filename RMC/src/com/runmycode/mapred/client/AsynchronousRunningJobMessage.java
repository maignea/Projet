package com.runmycode.mapred.client;

import java.io.Serializable;

import com.runmycode.mapred.JobID;

public class AsynchronousRunningJobMessage implements Serializable
{
	private JobID jobID;
	private String methodName;
	private Object[] methodParameters;
	
	public AsynchronousRunningJobMessage(JobID jobID, String methodName, Object[] methodParameters)
	{
		super();
		this.jobID = jobID;
		this.methodName = methodName;
		this.methodParameters = methodParameters;
	}

	public JobID getJobID()
	{
		return jobID;
	}

	public String getMethodName()
	{
		return methodName;
	}

	public Object[] getMethodParameters()
	{
		return methodParameters;
	}
}
