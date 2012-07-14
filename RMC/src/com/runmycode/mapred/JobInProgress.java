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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import com.runmycode.mapred.impl.Combiner;
import com.runmycode.mapred.impl.LocalCombinerTest;
import com.runmycode.mapred.impl.LocalSplitterTest;
import com.runmycode.mapred.impl.RunMapper;
import com.runmycode.mapred.impl.RunReducer;
import com.runmycode.mapred.impl.Splitter;
import com.runmycode.mapred.mongoDB.MongoDBFactory;


/*************************************************************
 * JobInProgress maintains all the info for keeping
 * a Job on the straight and narrow.  It keeps its JobProfile
 * and its latest JobStatus, plus a set of tables for 
 * doing bookkeeping of its Tasks.
 * ***********************************************************
 */
public class JobInProgress
{
	private transient MongoOperations mongoOperations = MongoDBFactory.getMongoOperations();
	private transient Logger logger = Logger.getLogger(com.runmycode.mapred.JobInProgress.class);
	
	public List<TaskInProgress> getMapTasks()
	{
		List<TaskInProgress> tasks = mongoOperations.find(Query.query(Criteria.where("mapTask").is(true).and("jobID._id").is(jobID.getId())), TaskInProgress.class);
		return tasks;
	}
		
	public List<TaskInProgress> getReduceTasks()
	{
		List<TaskInProgress> tasks = mongoOperations.find(Query.query(Criteria.where("reduceTask").is(true).and("jobID._id").is(jobID.getId())), TaskInProgress.class);
		return tasks;
	}
	
	public List<TaskInProgress> getSplitTasks()
	{
		List<TaskInProgress> tasks = mongoOperations.find(Query.query(Criteria.where("splitTask").is(true).and("jobID._id").is(jobID.getId())), TaskInProgress.class);
		return tasks;
	}
	
	private JobStatus jobStatus;
	
	private String user;
	//private long launchTime;
	
	/**
	 * Used when the a kill is issued to a job which is initializing.
	 */
	private long startTime;
	private long finishTime;
	private JobPriority priority;
	
	private Counters jobCounters;
	private JobID jobID;
	
	private int finishedMapTasks;
	private int finishedReduceTasks;

	transient JobInProgressFactory jobInProgressFactory;
	
	
	
	public JobInProgressFactory getJobInProgressFactory() {
		return jobInProgressFactory;
	}

	public void setJobInProgressFactory(JobInProgressFactory jobInProgressFactory) {
		this.jobInProgressFactory = jobInProgressFactory;
	}

	@Id
	private String id;
  
	JobInProgress() 
	{
		
	}

	/**
	 * Create an almost empty JobInProgress, which can be used only for tests
	 */  
	JobInProgress(JobID jobid) throws IOException 
	{	  
		this.jobID = jobid;
		this.jobStatus = new JobStatus(jobID, 0, 0, JobStatus.State.PREP);
		
		id = jobID.getId();
	}
  
	/**
	 * Get the user for the job
	 */
	public String getUser()
	{    
		return user;  
	}
  
	boolean hasRestarted()
	{	
		return false;  
	}

  
	/////////////////////////////////////////////////////  
	// Accessors for the JobInProgress  
	/////////////////////////////////////////////////////  

	public JobStatus getStatus() 
	{
		return jobStatus;  
	}
    
	public synchronized long getLaunchTime() 
	{
		if(this.isComplete())
		{
			return this.finishTime - this.startTime;
		}
		else
		{
			return System.currentTimeMillis() - this.startTime;
		}
	}
  
	public long getStartTime() 
	{	
		return startTime;  
	}
    
	public long getFinishTime() 
	{	
		return finishTime;  
	}
  
  
	public int desiredMaps() 
	{	
		return getJobConf().getNumMapTasks();  
	}
  
	public synchronized int finishedMaps() 
	{	
		return finishedMapTasks;  
	}
    
	public int desiredReduces() 
	{	
		return getJobConf().getNumReduceTasks();  
	}
  
  
	public synchronized int runningMaps() 
	{	
		return 0;  
	}
  
  
	public synchronized int runningReduces() 
	{
		return 0;  
	}
  
  
	public synchronized int finishedReduces() 
	{	
		return finishedMapTasks;  
	}
  
  
	public synchronized int pendingMaps() 
	{	
		return 0;  
	}
  
  
	public synchronized int pendingReduces() 
	{	
		return 0;  
	}
  
  
	/**
	 * Return total number of map and reduce tasks desired by the job.
	 * @return total number of map and reduce tasks desired by the job
	 */  
	public int desiredTasks() 
	{    
		return desiredMaps() + desiredReduces();  
	}
  
  
	public JobPriority getPriority() 
	{
		return priority;
	}
  
  
	public void setPriority(JobPriority priority) 
	{	  
		if(priority == null)
		{			
			this.priority = JobPriority.NORMAL;
		}
		else
		{
			this.priority = priority;
		}
	}	

	/** 
	 * Get all the tasks of the desired type in this job.
	 * @param type {@link TaskType} of the tasks required
	 * @return An array of {@link TaskInProgress} matching the given type. 
	 *         Returns an empty array if no tasks are found for the given type.  
	 */
	TaskInProgress[] getTasks(TaskType type) 
	{
		TaskInProgress[] tasks = null;
		switch (type) 
		{    	
			case SPLIT:
				tasks = getSplitTasks().toArray(new TaskInProgress[0]);
				break;
			case MAP:
				tasks = getMapTasks().toArray(new TaskInProgress[0]);
				break;
			case REDUCE:
				tasks = getReduceTasks().toArray(new TaskInProgress[0]);
				break;
			default:
			{
				tasks = new TaskInProgress[0];
			}
			break;
		}    
		return tasks;
	}
  
	/**
	 * Get the job configuration
	 * @return the job's configuration
	 */  
	JobConf getJobConf() 
	{	
		JobConf jobConf = mongoOperations.findById(id, JobConf.class);
		return jobConf;  
	}
    
	/**
	 * Return a vector of completed TaskInProgress objects
	 */
	public synchronized List<TaskInProgress> reportTasksInProgress(boolean shouldBeMap, boolean shouldBeComplete) 
	{
		TaskInProgress tip;
		List<TaskInProgress> tasks = new ArrayList<TaskInProgress>();
		
		Iterator<TaskInProgress> maps = getMapTasks().iterator();
		while(maps.hasNext()){
			tip = maps.next();
			if(shouldBeComplete == true){
				if(tip.isComplete()){
					tasks.add(tip);
				}
			} else {
				tasks.add(tip);
			}
		}
		
		if(shouldBeMap == true)
		{	
			return tasks;
		}
		
		
		Iterator<TaskInProgress> allTasks = getReduceTasks().iterator();
		
		while(allTasks.hasNext())
		{
			tip = allTasks.next();
			if(shouldBeComplete == true){
				if(tip.isComplete()){
					tasks.add(tip);
				}
			} else {
				tasks.add(tip);
			}
		}
		
		allTasks = getSplitTasks().iterator();
		while(allTasks.hasNext())
		{
			tip = allTasks.next();
			if(shouldBeComplete == true){
				if(tip.isComplete()){
					tasks.add(tip);
				}
			} else {
				tasks.add(tip);
			}
		}
	  	  
		return tasks;	 
	}
    

	/**
	 * Returns the job-level counters.
	 * 
	 * @return the job-level counters.
	 */
	public synchronized Counters getJobCounters() 
	{
		return jobCounters;
	}
  
	/**
	 *  Returns map phase counters by summing over all map tasks in progress.
	 *  This method returns true if counters are within limit or false.
	 */
	public synchronized boolean getMapCounters(Counters counters) 
	{  	  
		Iterator<TaskInProgress> allTasks = getMapTasks().iterator();
		TaskInProgress tip;
		while(allTasks.hasNext())
		{
			tip = allTasks.next();
			counters = Counters.sum(counters, tip.getCounters());
		}
		return true;
	}
    
	/**
	 *  Returns map phase counters by summing over all reduce tasks in progress.
	 *  This method returns true if counters are within limits and false otherwise.
	 */
	public synchronized boolean getReduceCounters(Counters counters) 
	{  
		Iterator<TaskInProgress> allTasks = getReduceTasks().iterator();
		TaskInProgress tip;
		while(allTasks.hasNext())
		{
			tip = allTasks.next();
			counters = Counters.sum(counters, tip.getCounters());
		}
		return true;    
	}
    
	/**
	 *  Returns the total job counters, by adding together the job, 
	 *  the map and the reduce counters. This method returns true if
	 *  counters are within limits and false otherwise.
	 */
	public synchronized boolean getCounters(Counters result) 
	{  
		this.getReduceCounters(result);
		this.getMapCounters(result);
	  
		result = Counters.sum(result, jobCounters);
	  
		return true;
	}
    
	/**
	 * Increments the counters with the counters from each task.
	 * @param counters the counters to increment
	 * @param tips the tasks to add in to counters
	 * @return counters the same object passed in as counters
	 */
	private Counters incrementTaskCounters(Counters counters, TaskInProgress[] tips)
	{
		for (TaskInProgress tip : tips) 
		{      
			counters.incrAllCounters(tip.getCounters());
		}
		return counters;
	}
     
  
	/**
	 * A taskid assigned to this JobInProgress has reported in successfully.
	 * @throws Exception 
	 */  
	public synchronized boolean completedTask(TaskInProgress tip, TaskStatus status) throws Exception
	{
		JobID jobID = tip.getTIPId().getJobID();
		
		//launchTime = System.currentTimeMillis() - startTime;
		
		//mongoOperations.updateFirst(Query.query(Criteria.where("jobID._id").is(jobID.getId())), Update.update("launchTime", launchTime), JobInProgress.class);
		
		if(status.getRunState() == TaskStatus.State.FAILED)
		{		  
			//Stopping other tasks
			this.kill();
			//Updating DB			
			mongoOperations.updateFirst(Query.query(Criteria.where("jobID").is(jobID)), Update.update("jobStatus.runState", JobStatus.State.FAILED), JobInProgress.class);
		}
		else if(status.getRunState() == TaskStatus.State.KILLED)
		{
			//Updating DB
			mongoOperations.updateFirst(Query.query(Criteria.where("jobID").is(jobID)), Update.update("jobStatus.runState", JobStatus.State.KILLED), JobInProgress.class);
		}
		else if(status.getRunState() == TaskStatus.State.SUCCEEDED)
		{
			if(tip.isSplitTask())
			{
				Splitter splitter = new LocalSplitterTest();
				splitter.cleanup(jobID);
				RunMapper runMapper = new RunMapper(jobInProgressFactory);
				//launch mapping			 				
				runMapper.runMap(jobID);
			}
			else if(tip.isMapTask())
			{		
				finishedMapTasks++;
								
				mongoOperations.updateFirst(Query.query(Criteria.where("jobID._id").is(jobID.getId())), Update.update("finishedMapTasks", finishedMapTasks), JobInProgress.class);
				
				float mapProgress = finishedMapTasks/(float)getJobConf().getNumMapTasks();
				mongoOperations.updateFirst(Query.query(Criteria.where("jobID._id").is(jobID.getId())), Update.update("jobStatus.mapProgress", mapProgress), JobInProgress.class);
				
				logger.debug("finishedMapTasks = " + finishedMapTasks);
				
				if(finishedMapTasks == getJobConf().getNumMapTasks())
				{
					logger.debug("maps terminees !");
					RunMapper runMapper = new RunMapper(jobInProgressFactory);
					//mapper cleanup
					runMapper.cleanup(jobID);	
					
					Combiner combiner = new LocalCombinerTest();
					combiner.runCombiner(jobInProgressFactory, jobID);			 
				}							
			}
			else if(tip.isCombineTask())
			{
				Combiner combiner = new LocalCombinerTest();
				combiner.cleanup(jobID);
				RunReducer runReducer = new RunReducer(jobInProgressFactory);
				//launching reducing
				runReducer.runReduce(jobID);
			}
			else if(tip.isReduceTask())
			{			
				finishedReduceTasks++;
				
				mongoOperations.updateFirst(Query.query(Criteria.where("jobID._id").is(jobID.getId())), Update.update("finishedReduceTasks", finishedReduceTasks), JobInProgress.class);
				
				float reduceProgress = finishedReduceTasks/(float)getJobConf().getNumReduceTasks();
				mongoOperations.updateFirst(Query.query(Criteria.where("jobID._id").is(jobID.getId())), Update.update("jobStatus.reduceProgress", reduceProgress), JobInProgress.class);
				
				logger.debug("finishedReduceTasks = " + finishedReduceTasks);
				
				if(finishedReduceTasks == getJobConf().getNumReduceTasks())
				{
					RunReducer runReducer = new RunReducer(jobInProgressFactory);
					runReducer.cleanup(jobID);
					
					logger.debug("reduces terminees !");
					
					//Updating DB
					finishTime = System.currentTimeMillis();
					mongoOperations.updateFirst(Query.query(Criteria.where("jobID._id").is(jobID.getId())), Update.update("finishTime", finishTime), JobInProgress.class);
					mongoOperations.updateFirst(Query.query(Criteria.where("jobID._id").is(jobID.getId())), Update.update("jobStatus.runState", JobStatus.State.SUCCEEDED), JobInProgress.class);					
				} 
			}	
		}	  
		return false;            
	}
  
 
	/**
	 * The job is done since all it's component tasks are either
	 * successful or have failed.
	 */
	/*private void jobComplete() 
	{
		
	}*/

	/*private synchronized void terminateJob(int jobTerminationState) 
	{
		
	}*/

	/**
	 * Terminate the job and all its component tasks.
	 * Calling this will lead to marking the job as failed/killed. Cleanup 
	 * tip will be launched. If the job has not inited, it will directly call 
	 * terminateJob as there is no need to launch cleanup tip.
	 * This method is reentrant.
	 * @param jobTerminationState job termination state
	 */
	/*private synchronized void terminate(int jobTerminationState)
	{
		
	}*/

	/**
	 * Kill the job and all its component tasks. This method should be called from 
	 * jobtracker and should return fast as it locks the jobtracker.
	 */
	public void kill()   
	{
		/* Iterator<TaskInProgress> allTasks = getJobSetupTasks().iterator();
		TaskInProgress tip;
		while(allTasks.hasNext())
		{
			tip = allTasks.next();
			tip.kill();
		}*/

		TaskInProgress tip;
		Iterator<TaskInProgress> allTasks = getMapTasks().iterator();
		while(allTasks.hasNext())
		{
			tip = allTasks.next();
			tip.kill();
		}
		
		allTasks = getReduceTasks().iterator();
		while(allTasks.hasNext())
		{
			tip = allTasks.next();
			tip.kill();
		}
		
		/* allTasks = getJobCleanupTasks().iterator();
		while(allTasks.hasNext())
		{
			tip = allTasks.next();
			tip.kill();
		}*/
	}

	/**
	 * Fails the job and all its component tasks. This should be called only from
	 * {@link JobInProgress} or {@link JobTracker}. Look at 
	 * {@link JobTracker#failJob(JobInProgress)} for more details.
	 */
	/*synchronized void fail()
	{
		
	}*/

	/**
	 * A task assigned to this JobInProgress has reported in as failed.
	 * Most of the time, we'll just reschedule execution.  However, after
	 * many repeated failures we may instead decide to allow the entire 
	 * job to fail or succeed if the user doesn't care about a few tasks failing.
	 *
	 * Even if a task has reported as completed in the past, it might later
	 * be reported as failed.  That's because the TaskTracker that hosts a map
	 * task might die before the entire job can complete.  If that happens,
	 * we need to schedule reexecution so that downstream reduce tasks can 
	 * obtain the map task's output.
	 */
	/*private void failedTask(TaskInProgress tip, TaskID taskid, TaskStatus status, TaskTracker taskTracker, boolean wasRunning, boolean wasComplete, boolean wasAttemptRunning)
	{
		
	}*/

	/**
	 * Fail a task with a given reason, but without a status object.
	 * 
	 * Assuming {@link JobTracker} is locked on entry.
	 * 
	 * @param tip The task's tip
	 * @param taskid The task id
	 * @param reason The reason that the task failed
	 * @param trackerName The task tracker the task failed on
	 */
	/*public void failedTask(TaskInProgress tip, TaskID taskid, String reason, TaskStatus.Phase phase, TaskStatus.State state, String trackerName)
	{
		
	}*/

	/**
	 * Return the TaskInProgress that matches the tipid.
	 */
	public synchronized TaskInProgress getTaskInProgress(TaskID tipid) 
	{
		return mongoOperations.findById(tipid.getId(), TaskInProgress.class);    
	}

	/**
	 * Find the details of someplace where a map has finished
	 * @param mapId the id of the map
	 * @return the task status of the completed task
	 */
	/*public synchronized TaskStatus findFinishedMap(int mapId)
	{
		return null;
	}*/

	/*synchronized int getNumTaskCompletionEvents()
	{
		return 0;
	}*/

	synchronized public Set<TaskCompletionEvent> getTaskCompletionEvents(int fromEventId, int maxEvents)
	{
		return null;
	}

	/**
	 * @return The JobID of this JobInProgress.
	 */
	public JobID getJobID() 
	{	
		return jobID;
	}

	/*public synchronized Object getSchedulingInfo()
	{
		return null;
	}

	public synchronized void setSchedulingInfo(Object schedulingInfo)
	{
		
	}*/

	/**
	 * To keep track of kill and initTasks status of this job. initTasks() take 
	 * a lock on JobInProgress object. kill should avoid waiting on 
	 * JobInProgress lock since it may take a while to do initTasks().
	 */
	private static class JobInitKillStatus
	{
		
	}

	boolean isComplete()
	{
		if(this.jobStatus.getRunState().equals(JobStatus.State.SUCCEEDED) || this.jobStatus.getRunState().equals(JobStatus.State.FAILED) || this.jobStatus.getRunState().equals(JobStatus.State.KILLED))
		{
			return true;
		}
		else
		{
			return false;
		}
	}
	
	/**
	 * Construct the splits, etc.  This is invoked from an async
	 * thread so that split-computation doesn't block anyone.
	 */
	public synchronized void initTasks()
	{
		this.startTime = System.currentTimeMillis();
	}

	/**
	 * Get the task type for logging it to {@link JobHistory}.
	 */
	/*private String getTaskType(TaskInProgress tip)
	{
		return null;
	}*/
}
