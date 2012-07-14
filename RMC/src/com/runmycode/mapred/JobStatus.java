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

import org.springframework.data.mongodb.core.MongoOperations;

import com.runmycode.mapred.mongoDB.MongoDBFactory;

/**************************************************
 * Describes the current status of a job.
 * 
 * @see JobProfile for some more information.
 **************************************************/


public class JobStatus implements Serializable, Cloneable 
{
	private transient MongoOperations mongoOperations = MongoDBFactory.getMongoOperations();

	/**
	 * Current state of the job 
	 */
	public static enum State 
	{
		RUNNING,
		SUCCEEDED,
		FAILED,
		PREP,
		KILLED;
	}

	/**
	 * Helper method to get human-readable state of the job.
	 * @param state job state
	 * @return human-readable state of the job
	 */
	public static String getJobRunState(int state)
	{
		return null;
	}

	private JobID jobid;
	private float mapProgress;
	private float reduceProgress;
	private State runState;
	private JobPriority jobPriority;

	public JobStatus() 
	{
		
	}

	/**
	 * Create a job status object for a given jobid.
	 * @param jobid The jobid of the job
	 * @param mapProgress The progress made on the maps
	 * @param reduceProgress The progress made on the reduces
	 * @param runState The current state of the job
	 */
	public JobStatus(JobID jobid, float mapProgress, float reduceProgress, State runState) 
	{
		this(jobid, mapProgress, reduceProgress, runState, JobPriority.NORMAL);
	}

	/**
	 * Create a job status object for a given jobid.
	 * @param jobid The jobid of the job
	 * @param setupProgress The progress made on the setup
	 * @param mapProgress The progress made on the maps
	 * @param reduceProgress The progress made on the reduces
	 * @param cleanupProgress The progress made on the cleanup
	 * @param runState The current state of the job
	 * @param jp Priority of the job.
	 */
	public JobStatus(JobID jobid, float mapProgress, float reduceProgress, State runState, JobPriority jp) 
	{
		this.jobid = jobid;
		this.mapProgress = mapProgress;
		this.reduceProgress = reduceProgress;
		this.runState = runState;
		this.jobPriority = jp;
	}

	/**
	 * @return The jobid of the Job
	 */
	public JobID getJobID() 
	{
		return this.jobid;  
	}

	/**
	 * @return Percentage of progress in maps 
	 */
	public synchronized float mapProgress() 
	{
		return mapProgress;
	}

	/**
	 * Sets the map progress of this job
	 * @param p The value of map progress to set to
	 */
	public synchronized void setMapProgress(float p) 
	{
		this.mapProgress = p;
	}

	/**
	 * @return Percentage of progress in reduce 
	 */
	public synchronized float reduceProgress() 
	{	
		return this.reduceProgress;  	 
	}

	/**
	 * Sets the reduce progress of this Job
	 * @param p The value of reduce progress to set to
	 */
	public synchronized void setReduceProgress(float p) 
	{
		this.reduceProgress = p;     
	}

	/**
	 * @return running state of the job
	 */
	public synchronized State getRunState() 
	{	
		return this.runState;  	
	}

	/**
	 * Change the current run state of the job.
	 */
	public synchronized void setRunState(State state) 
	{
		this.runState = state;
	}

	/** 
	 * Set the start time of the job
	 * @param startTime The startTime of the job
	 */
	synchronized void setStartTime(long startTime) 
	{
		
	}
    
	/**
	 * @return start time of the job
	 */
	synchronized public long getStartTime() 
	{		
		return 0; 	
	}

	/**
	 * @param user The username of the job
	 */
	public synchronized void setUsername(String userName) 
	{ 
		
	}

	/**
	 * @return the username of the job
	 */
	public synchronized String getUsername()
	{
		return null;
	}

	/**
	 * Gets the Scheduling information associated to a particular Job.
	 * @return the scheduling information of the job
	 */
	public synchronized String getSchedulingInfo()
	{
		return null;
	}

	/**
	 * gets any available info on the reason of failure of the job.
	 * @return diagnostic information on why a job might have failed.
	 */
	public synchronized String getFailureInfo()
	{
		return null;
	}

	/**
	 * set the reason for failuire of this job
	 * @param failureInfo the reason for failure of this job.
	 */
	public synchronized void setFailureInfo(String failureInfo)
	{
		
	}

	/**
	 * Used to set the scheduling information associated to a particular Job.
	 * 
	 * @param schedulingInfo Scheduling information of the job
	 */
	public synchronized void setSchedulingInfo(String schedulingInfo)
	{
		
	}

	/**
	 * Return the priority of the job
	 * @return job priority
	 */
	public synchronized JobPriority getJobPriority()
	{
		return this.jobPriority;  
	}

	/**
	 * Set the priority of the job, defaulting to NORMAL.
	 * @param jp new job priority
	 */
	public synchronized void setJobPriority(JobPriority jp) 
	{
		this.jobPriority = jp;
	}

	/**
	 * Returns true if the status is for a completed job.
	 */
	public synchronized boolean isJobComplete() 
	{	
		return this.runState.equals(State.SUCCEEDED) || this.runState.equals(State.FAILED) || this.runState.equals(State.KILLED);  
	}

	@Override
	public String toString()
	{
		return "JobStatuts [jobID=" + this.jobid + ", mapProgress=" + this.mapProgress
				+ ", reduceProgress=" + this.reduceProgress + ", state="
				+ this.runState + ", jobPriority=" + this.jobPriority + "]";
	}
}
