package com.runmycode.mapred.server;

import java.io.Serializable;

import com.runmycode.mapred.Counter;
import com.runmycode.mapred.InputSplit;
import com.runmycode.mapred.Reporter;
import com.runmycode.mapred.TaskID;
import com.runmycode.mapred.TaskInProgressID;
import com.runmycode.mapred.TaskStatus;

public class ComputingReportationMessage implements Reporter, Serializable
{
	private TaskID taskID;
	private TaskStatus taskStatus;
	private TaskInProgressID taskInProgressID;
	
	public ComputingReportationMessage(TaskInProgressID taskInProgressID, TaskID taskID, TaskStatus taskStatus)
	{
		super();
		this.taskID = taskID;
		this.taskStatus = taskStatus;
		this.taskInProgressID = taskInProgressID;
	}

	public void setTaskID(TaskID taskID)
	{
		this.taskID = taskID;
	}
	
	public TaskID getTaskID()
	{
		return taskID;
	}

	@Override
	public void progress() 
	{
		
	}

	@Override
	public void setTaskStatus(TaskStatus status)
	{
		this.taskStatus = status;		
	}
	
	@Override
	public Counter getCounter(Enum<?> name) 
	{
		return null;
	}

	@Override
	public Counter getCounter(String group, String name) 
	{
		return null;
	}

	@Override
	public void incrCounter(Enum<?> key, long amount) 
	{
		
	}

	@Override
	public void incrCounter(String group, String counter, long amount) 
	{
		
	}

	@Override
	public InputSplit getInputSplit() throws UnsupportedOperationException 
	{
		return null;
	}

	@Override
	public TaskStatus getTaskStatus()
	{
		return this.taskStatus;
	}

	public TaskInProgressID getTaskInProgressID() 
	{
		return this.taskInProgressID;
	}

	public void setTaskInProgressID(TaskInProgressID taskInProgressID)
	{
		this.taskInProgressID = taskInProgressID;
	}
}
