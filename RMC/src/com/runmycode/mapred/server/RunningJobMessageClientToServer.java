package com.runmycode.mapred.server;

import java.io.Serializable;
import java.util.Arrays;

import com.runmycode.mapred.JobID;

public class RunningJobMessageClientToServer implements Serializable
{
	private String methodName;
	private JobID jodID;
	private Object[] methodParameters;
	private String responsMethodName;
	
	public RunningJobMessageClientToServer(JobID jodID, String methodName, String responsMethodName, Object[] methodParameters) 
	{
		super();
		this.methodName = methodName;
		this.jodID = jodID;
		this.methodParameters = methodParameters;
		this.responsMethodName = responsMethodName;
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

	public String getResponsMethodName() 
	{
		return responsMethodName;
	}
}
