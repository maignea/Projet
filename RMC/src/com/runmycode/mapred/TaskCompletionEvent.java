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

/**
 * This is used to track task completion events on 
 * job tracker. 
 */
public class TaskCompletionEvent
{
	static public enum Status
	{
		FAILED,
		KILLED,
		SUCCEEDED,
		OBSOLETE,
		TIPFAILED
	};
	
	private int eventId; 
	private String taskTrackerHttp;
	private int taskRunTime; // using int since runtime is the time difference
	private TaskID taskId;
	private Status status; 
	private boolean isMap = false;
	private int idWithinJob;
	public static final TaskCompletionEvent[] EMPTY_ARRAY = new TaskCompletionEvent[0];

	/**
	 * Default constructor for Writable.
	 *
	 */
	public TaskCompletionEvent()
	{
		taskId = new TaskID();
	}

	/**
	 * Constructor. eventId should be created externally and incremented
	 * per event for each job. 
	 * @param eventId event id, event id should be unique and assigned in
	 *  incrementally, starting from 0. 
	 * @param taskId task id
	 * @param status task's status 
	 * @param taskTrackerHttp task tracker's host:port for http. 
	 */
	public TaskCompletionEvent(int eventId, TaskID taskId, int idWithinJob, boolean isMap, Status status, String taskTrackerHttp)
	{
		this.taskId = taskId;
		this.idWithinJob = idWithinJob;
		this.isMap = isMap;
		this.eventId = eventId; 
		this.status =status; 
		this.taskTrackerHttp = taskTrackerHttp;
	}
	
	/**
	 * Returns event Id. 
	 * @return event id
	 */
	public int getEventId()
	{
		return eventId;
	}

	/**
	 * Returns task id. 
	 * @return task id
	 */
	public TaskID getTaskId() 
	{
		return taskId;
	}
	
	/**
	 * Returns enum Status.SUCESS or Status.FAILURE.
	 * @return task tracker status
	 */
	public Status getTaskStatus() 
	{
		return status;
	}
	
	/**
	 * http location of the tasktracker where this task ran. 
	 * @return http location of tasktracker user logs
	 */
	public String getTaskTrackerHttp() 
	{
		return taskTrackerHttp;
	}

	/**
	 * Returns time (in millisec) the task took to complete. 
	 */
	public int getTaskRunTime() 
	{
		return taskRunTime;
	}

	/**
	 * Set the task completion time
	 * @param taskCompletionTime time (in millisec) the task took to complete
	 */
	public void setTaskRunTime(int taskCompletionTime) 
	{
		this.taskRunTime = taskCompletionTime;
	}

	/**
	 * set event Id. should be assigned incrementally starting from 0. 
	 * @param eventId
	 */
	public void setEventId(int eventId)
	{
		this.eventId = eventId;
	}

	/**
	 * Sets task id. 
	 * @param taskId
	 */
	public void setTaskID(TaskID taskId)
	{
		this.taskId = taskId;
	}
	
	/**
	 * Set task status. 
	 * @param status
	 */
	public void setTaskStatus(Status status)
	{
		this.status = status;
	}
	
	/**
	 * Set task tracker http location. 
	 * @param taskTrackerHttp
	 */
	public void setTaskTrackerHttp(String taskTrackerHttp)
	{
		this.taskTrackerHttp = taskTrackerHttp;
	}
    
	@Override
	public String toString()
	{
		StringBuffer buf = new StringBuffer(); 
		buf.append("Task Id : "); 
		buf.append(taskId); 
		buf.append(", Status : ");  
		buf.append(status.name());
		return buf.toString();
	}
    
	@Override
	public boolean equals(Object o)
	{
		if(o == null)
			return false;
		if(o.getClass().equals(TaskCompletionEvent.class)) 
		{
			TaskCompletionEvent event = (TaskCompletionEvent) o;
			return this.isMap == event.isMapTask() && this.eventId == event.getEventId() && this.idWithinJob == event.idWithinJob() && this.status.equals(event.getTaskStatus()) && this.taskId.equals(event.getTaskId()) && this.taskRunTime == event.getTaskRunTime() && this.taskTrackerHttp.equals(event.getTaskTrackerHttp());
		}
		return false;
	}

	@Override
	public int hashCode() 
	{
		return toString().hashCode(); 
	}

	public boolean isMapTask()
	{
		return isMap;
	}
	
	public int idWithinJob()
	{
		return idWithinJob;
	}
}
