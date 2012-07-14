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

import java.io.Serializable;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;

/** A report on the state of a task. */
public class TaskReport  implements Serializable
{
	private TaskID taskID;
	private float progress;
	private String state;
	private String[] diagnostics;
	private Counters counters;
	private TIPStatus tipStatus;
	private long finishTime;
	private long startTime;
	
	public TaskReport(TaskID taskID, float progress, String state, String[] diagnostics, Counters counters, TIPStatus tipStatus, long finishTime, long startTime) 
	{
		super();
		this.taskID = taskID;
		this.progress = progress;
		this.state = state;
		this.diagnostics = diagnostics;
		this.counters = counters;
		this.tipStatus = tipStatus;
		this.finishTime = finishTime;
		this.startTime = startTime;
	}

	/** The id of the task. */
	public TaskID getTaskID() 
	{
		return taskID;  
	}
  
	/** The amount completed, between zero and one. */
	public float getProgress()
	{
		return progress;  
	}
  
	/** The most recent state, reported by a {@link Reporter}. */
	public String getState()
	{
		return state; 
	}
  
	/** A list of error messages. */
	public String[] getDiagnostics()
	{
		return diagnostics;  
	}
  
	/** A table of counters. */
	public Counters getCounters()
	{
		return counters;  
	}
  
	/** The current status */
	public TIPStatus getCurrentStatus()
	{
		return tipStatus;
	}
	/**
	 * Get finish time of task. 
	 * @return 0, if finish time was not set else returns finish time.
	 */
	public long getFinishTime()
	{
		return finishTime;
	}

	/** 
	 * set finish time of task. 
	 * @param finishTime finish time of task. 
	 */
	void setFinishTime(long finishTime)
	{
		this.finishTime = finishTime;
	}

	/**
	 * Get start time of task. 
	 * @return 0 if start time was not set, else start time. 
	 */
	public long getStartTime()
	{
		return startTime;
	}

	/** 
	 * set start time of the task. 
	 */ 
	void setStartTime(long startTime)
	{
		this.startTime = startTime;
	}

	@Override
	public String toString()
	{
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		Calendar startDate = Calendar.getInstance();
		startDate.setTimeInMillis(startTime);
		Calendar endDate = Calendar.getInstance();
		endDate.setTimeInMillis(finishTime);
	
		return "TaskReport [taskID=" + taskID + ", progress=" + progress
				+ ", state=" + state + ", diagnostics="
				+ Arrays.toString(diagnostics) + ", counters=" + counters
				+ ", tipStatus=" + tipStatus + ", finishTime=" + dateFormat.format(endDate.getTime())
				+ ", startTime=" + dateFormat.format(startDate.getTime()) + "]";
	}
}
