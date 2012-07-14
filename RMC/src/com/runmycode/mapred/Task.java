/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.runmycode.mapred;

import java.io.IOException;
import java.io.Serializable;

import org.springframework.data.annotation.Id;

import com.runmycode.mapred.JobStatus.State;

/** 
 * Base class for tasks.
 * 
 * This is NOT a public interface.
 */
public class Task implements Serializable
{
	TaskStatus taskStatus = new TaskStatus();
	private TaskID taskID;
	private String jobFile;
	private Counters counters;
	private JobID jobID;
	private boolean skipping = false;
	private boolean taskCleanup = false;
	private boolean splitTask = false;
	private boolean mapTask = false;
	private boolean reduceTask = false;
	private String user;
	
	@Id
	private String id;

	////////////////////////////////////////////
	// Constructors
	////////////////////////////////////////////

	public Task(TaskID taskID) 
	{
		this.taskID = taskID;
		id = taskID.getId();
		
		this.taskStatus.setRunState(TaskStatus.State.COMMIT_PENDING);
	}

	////////////////////////////////////////////
	// Accessors
	////////////////////////////////////////////
	public void setJobFile(String jobFile) 
	{  
		this.jobFile = jobFile;
	}

	public String getJobFile()
	{
		return jobFile;  
	}
	
	public TaskID getTaskID() 
	{
		return taskID;  
	}

	Counters getCounters() 
	{
		return counters; 
	}

	/**
	 * Get the job name for this task.
	 * @return the job name
	 */
	public JobID getJobID() 
	{
		return jobID;
	}

	/**
	 * Return current phase of the task. 
	 * needs to be synchronized as communication thread sends the phase every second
	 * @return the curent phase of the task
	 */
	public synchronized TaskStatus.Phase getPhase()
	{
		return this.taskStatus.getPhase();
	}
	/**
	 * Set current phase of the task. 
	 * @param phase task phase 
	 */
	public synchronized void setPhase(TaskStatus.Phase phase)
	{
		if(phase == TaskStatus.Phase.SPLIT)
		{
			splitTask = true;
			mapTask = false;
			reduceTask = false;
		}
		else if(phase == TaskStatus.Phase.MAP)
		{
			splitTask = false;
			mapTask = true;
			reduceTask = false;		  
		}
		else if(phase == TaskStatus.Phase.REDUCE)
		{
			splitTask = false;
			mapTask = false;
			reduceTask = true;		  
		}
		
		this.taskStatus.setPhase(phase);
	}

	/**
	 * Report a fatal error to the parent (task) tracker.
	 */
	protected void reportFatalError(TaskID id, Throwable throwable, String logMsg) 
	{
		
	}
  
	/**
	 * Is Task in skipping mode.
	 */
	public boolean isSkipping() 
	{
		return this.skipping;
	}

	/**
	 * Sets whether to run Task in skipping mode.
	 * @param skipping
	 */
	public void setSkipping(boolean skipping) 
	{
		this.skipping = skipping;	      
	}

	/**
	  * Return current state of the task. 
	 * needs to be synchronized as communication thread 
	 * sends the state every second
	 * @return
	 */
	synchronized TaskStatus.State getState()
	{
		return this.taskStatus.getRunState();
	}
	/**
	 * Set current state of the task. 
	 * @param state
	 */
	synchronized void setState(TaskStatus.State state)
	{
		this.taskStatus.setRunState(state);
	}

	void setTaskCleanupTask() 
	{
		taskCleanup = true;
	}
	   
	boolean isTaskCleanupTask()
	{
		return taskCleanup;  
	}
	
	boolean isMapOrReduce() 
	{
		return mapTask && reduceTask; 
	}
	
	void setUser(String user) 
	{
		this.user = user;
  	}

	/**
	 * Get the name of the user running the job/task. TaskTracker needs task's
	 * user name even before it's JobConf is localized. So we explicitly serialize
	 * the user name.
	 * 
	 * @return user
	 */
	public String getUser()
	{
	return this.user;
	}

	/**
	 * Localize the given JobConf to be specific for this task.
	 */
	public void localizeConfiguration(JobConf conf) throws IOException 
	{
		
	}
	
	public boolean isMapTask()
	{
		return mapTask;
	}

	private synchronized void updateCounters()
	{
		
	}

	/**
	 * Checks if this task has anything to commit, depending on the
	 * type of task, as well as on whether the {@link OutputCommitter}
	 * has anything to commit.
	 * 
	 * @return true if the task has to commit
	 * @throws IOException
	 */
	boolean isCommitRequired() throws IOException
	{
		return false;
	}

	/**
	 * Calculates the size of output for this task.
	 * 
	 * @return -1 if it can't be found.
	 */
	private long calculateOutputSize() throws IOException
	{
		return 0;

	}

	public boolean isSplitTask() 
	{	
		return splitTask;
	}

	public boolean isReduceTask() 
	{	
		return reduceTask;	
	}
}
