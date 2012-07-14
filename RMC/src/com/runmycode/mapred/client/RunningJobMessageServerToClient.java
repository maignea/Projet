package com.runmycode.mapred.client;

import java.io.Serializable;
import java.util.Arrays;

import com.runmycode.mapred.JobID;

public class RunningJobMessageServerToClient implements Serializable
{
	private String methodName;
	private JobID jodID;
	private Object[] methodParameters;
	
	public RunningJobMessageServerToClient(JobID jodID, String methodName, Object[] methodParameters) 
	{
		super();
		this.methodName = methodName;
		this.jodID = jodID;
		this.methodParameters = methodParameters;
	}

	public String getMethodName() 
	{
		return methodName;
	}

	public JobID getJobID() 
	{
		return jodID;
	}

	public Object[] getMethodParameters() 
	{
		return methodParameters;
	}

	@Override
	public String toString() 
	{
		return "RunningJobMessage [methodName=" + methodName + ", jodID="
				+ jodID + ", methodParameters="
				+ Arrays.toString(methodParameters) + "]";
	}
}
