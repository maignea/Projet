package com.runmycode.mapred.client;

import java.io.Serializable;

import com.runmycode.mapred.server.RunningJobMessageClientToServer;

/**
 * @author charroux
 *
 */
public class RunningJobMessageClientToServeRespons implements Serializable
{
	private RunningJobMessageClientToServer question;
	private Object returnValue;
	private Class<? extends Object> theClass;
	
	public RunningJobMessageClientToServeRespons(RunningJobMessageClientToServer question, Object returnValue, Class<? extends Object> theClass) 
	{
		this.question = question;
		this.returnValue = returnValue;
		this.theClass = theClass;
	}

	public RunningJobMessageClientToServer getQuestion()
	{
		return question;
	}

	public void setQuestion(RunningJobMessageClientToServer question) 
	{
		this.question = question;
	}

	public Object getReturnValue() 
	{
		return returnValue;
	}

	public void setReturnValue(Object returnValue) 
	{
		this.returnValue = returnValue;
	}

	public Class<? extends Object> getTheClass() 
	{
		return theClass;
	}

	public void setTheClass(Class<? extends Object> theClass) 
	{
		this.theClass = theClass;
	}
}
