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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import com.runmycode.mapred.TaskStatus.Phase;
import com.runmycode.mapred.TaskStatus.State;

/**************************************************
 * Describes the current status of a task.  This is
 * not intended to be a comprehensive piece of data.
 *
 **************************************************/
public class TaskStatus implements Serializable
{
	
	//enumeration for reporting current phase of a task. 
	public static enum Phase implements Serializable
	{
		SPLIT,
		MAP,
		REDUCE,	 
		COMBINE,
	}

	// what state is the task in?
	public static enum State implements Serializable
	{
		RUNNING,
		SUCCEEDED,
		FAILED,
		KILLED,
		COMMIT_PENDING,  
	}

	
	private State currentState;
	private Phase currentPhase;
	
	public TaskStatus()
	{
		
	}
	
	public TaskID getTaskID()
	{
		return null; 
	}
	
	public boolean getIsMap()
	{
		if(this.currentPhase.equals(TaskStatus.Phase.MAP))
		{
			return true;
		}
		else
		{
			return false;
		}
	}

	public float getProgress()
	{
		return 0;
	}
  
	public void setProgress(float progress)
	{
		
	}

	public State getRunState() 
	{
		return this.currentState;  
	}
	
	
	public void setRunState(State runState) 
	{
		this.currentState = runState;
	}
  
	public String getDiagnosticInfo()
	{
		return null;
		}
  
	public void setDiagnosticInfo(String info)
	{ 
   
	}
  
    
	/**
	 * Get task finish time. if shuffleFinishTime and sortFinishTime 
	 * are not set before, these are set to finishTime. It takes care of 
	 * the case when shuffle, sort and finish are completed with in the 
	 * heartbeat interval and are not reported separately. if task state is 
	 * TaskStatus.FAILED then finish time represents when the task failed.
	 * @return finish time of the task. 
	 */
	public long getFinishTime()
	{
		return 0;
	}

	/**
	 * Sets finishTime for the task status if and only if the
	 * start time is set and passed finish time is greater than
	 * zero.
	 * 
	 * @param finishTime finish time of task.
	 */
	void setFinishTime(long finishTime)
	{
		
	}
	
	/**
	 * Get start time of the task. 
	 * @return 0 is start time is not set, else returns start time. 
	 */
	public long getStartTime()
	{
		return 0;
	}

	/**
	 * Set startTime of the task if start time is greater than zero.
	 * @param startTime start time
	 */
	void setStartTime(long startTime)
	{
		//Making the assumption of passed startTime to be a positive
		//long value explicit.
		if (startTime > 0)
		{
			
		}
		else
		{
			//Using String utils to get the stack trace.
		}
	}
  
	/**
	 * Get current phase of this task. Phase.Map in case of map tasks, 
	 * for reduce one of Phase.SHUFFLE, Phase.SORT or Phase.REDUCE. 
	 * @return . 
	 */
	public Phase getPhase()
	{
		return this.currentPhase;
	}
	
	/**
 	  * Set current phase of this task.  
 	  * @param phase phase of this task
 	  */
	void setPhase(Phase phase)
	{
		this.currentPhase = phase;
	}

	public boolean getIncludeCounters()
	{
		return false;
	}
  
	public void setIncludeCounters(boolean send)
	{

	}
  
	/**
	 * Get task's counters.
	 */
	public Counters getCounters()
	{
		return null;
	}
	
	/**
	 * Set the task's counters.
	 * @param counters
	 */
	public void setCounters(Counters counters)
	{
		
	}
	

	/**
	 * Update the status of the task.
	 * 
	 * This update is done by ping thread before sending the status. 
	 * 
	 * @param progress
	 * @param state
	 * @param counters
	 */
	synchronized void statusUpdate(float progress, String state, Counters counters) 
	{
		setProgress(progress);
		setCounters(counters);
	}

	/**
	 * Update the status of the task.
	 * 
	 * @param status updated status
	 */
	synchronized void statusUpdate(TaskStatus status) 
	{
		
	}

	/**
	 * Clear out transient information after sending out a status-update
	 * from either the {@link Task} to the {@link TaskTracker} or from the
	 * {@link TaskTracker} to the {@link JobTracker}. 
	 */
	synchronized void clearStatus()
	{
		// Clear diagnosticInfo
	}

	@Override
	public Object clone()
	{
		try
		{
			return super.clone();
		}
		catch (CloneNotSupportedException cnse)
		{
			// Shouldn't happen since we do implement Clonable
			throw new InternalError(cnse.toString());
		}
	}
  
	//////////////////////////////////////////////////////////////////////////////
	// Factory-like methods to create/read/write appropriate TaskStatus objects
	//////////////////////////////////////////////////////////////////////////////
	
	static TaskStatus createTaskStatus(DataInput in, TaskID taskId, float progress, int numSlots, String diagnosticInfo, String stateString, String taskTracker, Counters counters) throws IOException
	{
		return null;
	}
  
	static TaskStatus createTaskStatus(boolean isMap, TaskID taskId, float progress, int numSlots, String diagnosticInfo, String stateString, String taskTracker, Counters counters)
	{
		return null; 
	}
  
	static TaskStatus createTaskStatus(boolean isMap)
	{
		return null;
	}

	static TaskStatus readTaskStatus(DataInput in) throws IOException
	{
		return null;
	}

	static void writeTaskStatus(DataOutput out, TaskStatus taskStatus)
	{
		
	}

	public void setRunPhase(Phase phase) {
		this.currentPhase = phase;
	}
}

