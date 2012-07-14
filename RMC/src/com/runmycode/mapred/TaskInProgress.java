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

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import com.runmycode.mapred.TaskStatus.State;
import com.runmycode.mapred.mongoDB.MongoDBFactory;

/*************************************************************
 * TaskInProgress maintains all the info needed for a
 * Task in the lifetime of its owning Job.  A given Task
 * might be speculatively executed or reexecuted, so we
 * need a level of indirection above the running-id itself.
 * <br>
 * A given TaskInProgress contains multiple taskids,
 * 0 or more of which might be executing at any one time.
 * (That's what allows speculative execution.)  A taskid
 * is now *never* recycled.  A TIP allocates enough taskids
 * to account for all the speculation and failures it will
 * ever have to handle.  Once those are up, the TIP is dead.
 * **************************************************************
 */
public class TaskInProgress
{
	private transient MongoOperations mongoOperations = MongoDBFactory.getMongoOperations();
	
	private List<TaskID> attemptTasks;
	
	private List<Task> getAttemptTasks()
	{
		List<Task> tasks = new ArrayList<Task>();
		
		for(int i=0; i<attemptTasks.size(); i++){
			tasks.add(mongoOperations.findById(attemptTasks.get(i).getId(), Task.class));
		}
		return tasks;
	}
	
	private Task getAAttemptedTaskByID(TaskID taskID)
	{
		Task task = mongoOperations.findById(taskID.getId(), Task.class);
		return task;
	}
	
	private Task getCurrentTask()
	{
		int size = attemptTasks.size();
		if(size == 0)
		{
			return null;
		}
		TaskID taskID = attemptTasks.get(attemptTasks.size()-1);
		return getAAttemptedTaskByID(taskID);
	}

	@Id
	private String id;
	
	private long startTime;
	private long execStartTime;
	private long execFinishTime;
	private JobID jobID;
	private Hashtable<TaskID, List<String>> diagnosticInfos;

	private double progress;

	private String user;

	private TaskInProgressID taskInProgressID;
	
	private TIPStatus.Phase phase;

	private boolean mapTask;
	private boolean reduceTask;
	private boolean splitTask;
	private boolean combineTask;

	private transient JobInProgressFactory jobInProgressFactory;

	
	
	public JobInProgressFactory getJobInProgressFactory()
	{
		return jobInProgressFactory;
	}

	public void setJobInProgressFactory(JobInProgressFactory jobInProgressFactory)
	{
		this.jobInProgressFactory = jobInProgressFactory;
	}

	public TaskInProgress(TaskInProgressID tipID, TIPStatus.Phase phase) 
	{
		attemptTasks = new ArrayList<TaskID>();
		this.jobID = tipID.getJobID();	
		this.taskInProgressID = tipID;
		id = tipID.getId();
		diagnosticInfos = new Hashtable<TaskID, List<String>>();
		this.phase = phase;
	 
		if(phase == TIPStatus.Phase.MAP)
		{
			this.mapTask = true;
		}
		else
		{
			this.mapTask = false;
		}
		if(phase == TIPStatus.Phase.REDUCE)
		{
			this.reduceTask = true;
		} 
		else
		{
			this.reduceTask = false;
		}
		if(phase == TIPStatus.Phase.SPLIT)
		{
			this.splitTask = true;
		} 
		else
		{
			this.splitTask = false;
		}
		if(phase == TIPStatus.Phase.COMBINE)
		{
			this.combineTask = true;
		}
		else
		{
			this.combineTask = false;
		}
	}
  
	public TaskInProgress()
	{

	}

	/**
	 * Set the max number of attempts before we declare a TIP as "failed"
	 */
	private void setMaxTaskAttempts() 
	{
		
	}
     
	public boolean isCommitPending(TaskID taskId) 
	{
		return this.getAAttemptedTaskByID(taskId).getState().equals(TaskStatus.State.COMMIT_PENDING);
	}
  
	/**
	 * Return the start time
	 */
	public long getStartTime() 
	{
		return startTime;
	}
  
	/**
	 * Return the exec start time
	 */
	public long getExecStartTime() 
	{
		return execStartTime;
	}
  
	/**
	 * Set the exec start time
	 */
	public void setExecStartTime(long startTime) 
	{
		this.execStartTime = startTime;
	}
  
	/**
	 * Return the exec finish time
	 */
	public long getExecFinishTime() 
	{
		return execFinishTime;
	}

	/**
	 * Set the exec finish time
	 */
	public void setExecFinishTime(long finishTime) 
	{
		this.execFinishTime = finishTime;
	}
  
	/**
	 * Return the parent job
	 * @throws Exception 
	 */
	public JobInProgress getJob() throws Exception 
	{	
		JobInProgress jobInProgress = jobInProgressFactory.getJobInProgress(jobID);
		return jobInProgress;
	}
 
	/**
	 * Return an ID for this task, not its component taskid-threads
	 */
	public TaskID getTIPId() 
	{
		return getCurrentTask().getTaskID();
	}
  
	/**
	 * Whether this is a map task
	 */
	public boolean isMapTask() 
	{
		return mapTask;
	}
    
	/**
	 * Returns the type of the {@link TaskAttemptID} passed. 
	 * The type of an attempt is determined by the nature of the task and not its 
	 * id. 
	 * For example,
	 * - Attempt 'attempt_123_01_m_01_0' might be a job-setup task even though it 
	 *   has a _m_ in its id. Hence the task type of this attempt is JOB_SETUP 
	 *   instead of MAP.
	 * - Similarly reduce attempt 'attempt_123_01_r_01_0' might have failed and is
	 *   now supposed to do the task-level cleanup. In such a case this attempt 
	 *   will be of type TASK_CLEANUP instead of REDUCE.
	 */
	TaskType getAttemptType (TaskID id) 
	{
		Task task = getAAttemptedTaskByID(id);
		if(task == null)
		{
			if(getCurrentTask().getTaskID().equals(id))
			{
				task = getCurrentTask();
			}
		}
		if(task.isSplitTask())
		{
			return TaskType.SPLIT;
		}	  
		if(task.isMapTask())
		{
			return TaskType.MAP;
		}
		if(task.isReduceTask())
		{
			return TaskType.REDUCE;
		}	  
		return null;
	}

	TaskType getFirstTaskType() 
	{
		return null;
	}
  
	/**
	 * Is the Task associated with taskid is the first attempt of the tip? 
	 * @param taskId
	 * @return Returns true if the Task is the first attempt of the tip
	 */  
	public boolean isFirstAttempt(TaskID taskId)
	{
		if(getCurrentTask().getTaskID().equals(taskId))
		{
			if(getAAttemptedTaskByID(taskId) == null)
			{
				return true;
			}
		}
		return false;
	}
  
	/**
	 * Is this tip currently running any tasks?
	 * @return true if any tasks are running
	 */
	public boolean isRunning()
	{
		if(getCurrentTask().getState() == TaskStatus.State.RUNNING)
		{
			return true;
		}
		return false;
	}

	/**
	 * Is this attempt currently running ?
	 * @param  taskId task attempt id.
	 * @return true if attempt taskId is running
	 */
	boolean isAttemptRunning(TaskID taskId) 
	{
		Task task = this.getAAttemptedTaskByID(taskId);
		if(task != null)
		{
			if(task.getState() == TaskStatus.State.RUNNING)
			{
				return true;
			}
		}
		return false;
	}
    
	/**
	 * Is this tip complete?
	 * 
	 * @return <code>true</code> if the tip is complete, else <code>false</code>
	 */
	public synchronized boolean isComplete() 
	{
		Task currentTask = getCurrentTask(); 
		if(currentTask.getState() != TaskStatus.State.SUCCEEDED)
		{
			return false;
		}
		List<Task> tasks = this.getAttemptTasks();
		Task task;
		int i=0;
		while(i < tasks.size())
		{
			if((task = tasks.get(i)).equals(currentTask) == false)
			{			
				if(task.getState() == TaskStatus.State.RUNNING)
				{
					return false;
				}
			}
			i++;
		}
		return true;
	}	

	/**
	 * Is the given taskid the one that took this tip to completion?
	 * 
	 * @param taskid taskid of attempt to check for completion
	 * @return <code>true</code> if taskid is complete, else <code>false</code>
	 */
	public boolean isComplete(TaskID taskid) 
	{
		Task task = this.getAAttemptedTaskByID(taskid);
		if(task!=null && task.getState()==TaskStatus.State.SUCCEEDED)
		{
			return true;
		}
		return false;    
	}

	/**
	 * Is the tip a failure?
	 * 
	 * @return <code>true</code> if tip has failed, else <code>false</code>
	 */
	public boolean isFailed() 
	{	  
		List<Task> tasks = this.getAttemptTasks();
		int i=0;
		while(i < tasks.size())
		{
			if(tasks.get(i).getState() != TaskStatus.State.FAILED)
			{
				return false;
			}
			i++;
		}
		return true;
	}

	/**
	 * Number of times the TaskInProgress has failed.
	 */
	public int numTaskFailures() 
	{
		List<Task> tasks = this.getAttemptTasks();
		int i=0;
		int numTaskFailures = 0;
		while(i < tasks.size())
		{
			if(tasks.get(i).getState() == TaskStatus.State.FAILED)
			{
				numTaskFailures++;
			}
			i++;
		}
		return numTaskFailures;
	}

	/**
	 * Number of times the TaskInProgress has been killed by the framework.
	 */
	public int numKilledTasks() 
	{
		List<Task> tasks = this.getAttemptTasks();
		int i=0;
		int numKilledTasks = 0;
		while(i < tasks.size()){
			if(tasks.get(i).getState() == TaskStatus.State.KILLED){
				numKilledTasks++;
			}
			i++;
		}
		return numKilledTasks;
	}

	/**
	 * Get the overall progress (from 0 to 1.0) for this TIP
	 */
	public double getProgress()
	{
		return this.progress; 
	}
	
	/**
	 * Get the task's counters
	 */
	public Counters getCounters() 
	{
		return getCurrentTask().getCounters();
	}


	/**
	 * Commit this task attempt for the tip. 
	 * @param taskid
	 */
	public void doCommit(TaskID taskid) 
	{
		
	}

	/**
	 * Returns whether the task attempt should be committed or not 
	 */
	public boolean shouldCommit(TaskID taskid)
	{
		return false;	   
	}

	/**
	 * Creates a "status report" for this task.  Includes the
	 * task ID and overall status, plus reports for all the
	 * component task-threads that have ever been started.
	 */
	public synchronized TaskReport generateSingleReport() 
	{
		TaskID taskID = this.getTIPId();
		float progress = (float) this.getProgress();
		ArrayList<String> diagnostics = new ArrayList<String>();

		diagnostics.add("Essai1");
		
		Iterator<List<String>> infos = this.diagnosticInfos.values().iterator();
		while(infos.hasNext())
		{
			diagnostics.addAll(infos.next());
		}
		String[] diagnos = diagnostics.toArray(new String[0]);
		
		Counters counters = this.getCounters();
		
		TaskStatus taskStatus = this.getTaskStatus(taskID);
		
		TIPStatus tipStatus = new TIPStatus(taskStatus.getPhase().toString(), taskStatus.getRunState().toString());

		long finishTime = this.getExecFinishTime();

		long startTime = this.getExecStartTime();
		
		return new TaskReport(taskID, progress, tipStatus.toString(), diagnos, counters, tipStatus, finishTime, startTime);	    
	}

	/**
	 * Get the diagnostic messages for a given task within this tip.
	 * 
	 * @param taskId the id of the required task
	 * @return the list of diagnostics for that task
	 */
	synchronized List<String> getDiagnosticInfo(TaskID taskId) 
	{
		return diagnosticInfos.get(taskId);   
	}

	////////////////////////////////////////////////
	// Update methods, usually invoked by the owning
	// job.
	////////////////////////////////////////////////
  
	/**
	 * Save diagnostic information for a given task.
	 * 
	 * @param taskId id of the task 
	 * @param diagInfo diagnostic information for the task
	 */
	public void addDiagnosticInfo(TaskID taskId, String diagInfo) 
	{
		if(attemptTasks.contains(taskId) && diagInfo!=null)
		{
			List<String> diagnostics = diagnosticInfos.get(taskId);
			if(diagnostics == null)
			{
				diagnostics = new ArrayList<String>();
			}
			diagnostics.add(diagInfo);
			diagnosticInfos.put(taskId, diagnostics);
		}
	}

	/**
	 * A status message from a client has arrived.
	 * It updates the status of a single component-thread-task,
	 * which might result in an overall TaskInProgress status update.
	 * @return has the task changed its state noticably?
	 */
	synchronized boolean updateStatus(TaskStatus status) 
	{
		return false;
	}

	/**
	 * Finalize the <b>completed</b> task; note that this might not be the first 
	 * task-attempt of the {@link TaskInProgress} and hence might be declared 
	 * {@link TaskStatus.State.SUCCEEDED} or {@link TaskStatus.State.KILLED}
	 * 
	 * @param taskId id of the completed task-attempt
	 * @param finalTaskState final {@link TaskStatus.State} of the task-attempt
	 * @throws Exception
	 */
	private void completedTask(TaskID taskId, TaskStatus.State finalTaskState) throws Exception 
	{
		Task task;
		TaskStatus taskStatus = this.getTaskStatus(taskId);
		this.getJob().completedTask(this, taskStatus);
		
	}

	/**
	 * Indicate that one of the taskids in this TaskInProgress
	 * has successfully completed!
	 */
	public void completed(TaskID taskid) 
	{
		Task task = getAAttemptedTaskByID(taskid);
		if(task != null)
		{
			task.setState(TaskStatus.State.SUCCEEDED);    
		}
	}

	/**
	 * Get the Status of the tasks managed by this TIP
	 */
	public TaskStatus[] getTaskStatuses() 
	{
		List<TaskStatus> taskStatuses = new ArrayList<TaskStatus>();
		
		List<Task> tasks = this.getAttemptTasks();
		Iterator<Task> it =  tasks.iterator();
		while(it.hasNext())
		{
			taskStatuses.add(it.next().taskStatus);
		}
		
		return taskStatuses.toArray(new TaskStatus[taskStatuses.size()]);
		
	}

	/**
	 * Get all the {@link TaskAttemptID}s in this {@link TaskInProgress}
	 */
	/* TaskID[] getAllTaskAttemptIDs() 
	{
		//MongoOperations mongoOperations = null;
		//List<TaskID> taskIDs = mongoOperations.findAll(TaskID.class);
		//return taskIDs.toArray(new TaskID[taskIDs.size()]);
	}*/

	/**
	 * Get the status of the specified task
	 * @param taskid
	 * @return
	 */
	public TaskStatus getTaskStatus(TaskID taskid)
	{
		Task task = this.getAAttemptedTaskByID(taskid);
		if(task != null)
		{
			return task.taskStatus;
		}
		return null;
	}

	/**
	 * The TIP's been ordered kill()ed.
	 */
	public void kill() 
	{
		
	}

	/**
	 * Was the task killed?
	 * @return true if the task killed
	 */
	public boolean wasKilled() 
	{
		return (getCurrentTask().getState() == TaskStatus.State.KILLED);
	}
	
	/**
	 * Kill the given task
	 */
	boolean killTask(TaskID taskId, boolean shouldFail) 
	{
		return false;
	}

	/**
	 * Return whether this TIP still needs to run
	 */
	boolean isRunnable() 
	{
		/*if(!(getCurrentTask().getState() == TaskStatus.State.RUNNING))
		{
			return false;
		}
		List<Task> runningTasks = mongoOperations.find(Query.query(Criteria.where("taskStatus.currentState").is(TaskStatus.State.RUNNING)), Task.class);
		
		if(runningTasks.size() == 0)		 
		{		  		  
			return false;		  
		}*/
		return true;
	}

	public Task addRunningTask(TaskID taskID) 
	{
		Task task;
		
		if(attemptTasks.contains(taskID) == false)
		{
			task = new Task(taskID);
			attemptTasks.add(taskID);
		}
		else
		{
			task = this.getAAttemptedTaskByID(taskID);
		}
		return task;
	}
	
	String getUser() 
	{
		return this.user;    
	}
	
	void setUser(String user) 
	{
		this.user = user;
	}

	/**
	 * Adds a previously running task to this tip. This is used in case of 
	 * jobtracker restarts.
	 */
	public Task addRunningTask(TaskID taskid, String taskTracker, boolean taskCleanup) 
	{
		return null;  
	}

	public boolean isRunningTask(TaskID taskid) 
	{
		Task task = getAAttemptedTaskByID(taskid);
		
		if(task!=null && task.getState()==TaskStatus.State.RUNNING)
		{
			return true;
		}
		return false;
	}

	boolean wasKilled(TaskID taskid) 
	{
		Task task = getAAttemptedTaskByID(taskid);
		if(task!=null && task.getState()==TaskStatus.State.KILLED)
		{
			return true;
		}
		return false;
	}

	/**
	 * Get the id of this map or reduce task.
	 * @return The index of this tip in the maps/reduces lists.
	 */
	public int getIdWithinJob() 
	{
		return 0;
	}

	TreeMap<TaskID, String> getActiveTasks() 
	{
		return null;	    
	}

	public void updateTask(TaskID taskID, State newTaskStatus) throws Exception
	{	
		if(newTaskStatus == State.FAILED)
		{
			//compute again

			if(this.numTaskFailures() > 10) /*too many attempts*/
			{
				//abort taskInProgress
				mongoOperations.updateFirst(Query.query(Criteria.where("id").is(taskID.getId())), Update.update("taskStatus.currentState", newTaskStatus), Task.class);
			}

		}
		if(newTaskStatus == State.KILLED)
		{
			//stopping all running tasks
			this.kill();
			mongoOperations.updateFirst(Query.query(Criteria.where("id").is(taskID.getId())), Update.update("taskStatus.currentState", newTaskStatus), Task.class);
		}
		if(newTaskStatus == State.SUCCEEDED)
		{
			mongoOperations.updateFirst(Query.query(Criteria.where("id").is(taskID.getId())), Update.update("taskStatus.currentState", newTaskStatus), Task.class);
		}
		if(this.isComplete())
		{
			this.getJob().completedTask(this, this.getTaskStatus(taskID));
		}	   
	}

	public boolean isReduceTask() 
	{	
		return reduceTask;
	}

	public boolean isSplitTask() 
	{	
		return splitTask;
	}

	public boolean isCombineTask()
	{
		return combineTask;
	} 
}
