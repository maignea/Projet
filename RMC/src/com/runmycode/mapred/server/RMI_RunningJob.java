package com.runmycode.mapred.server;

import java.io.IOException;
import java.util.Set;

import com.runmycode.mapred.Counters;
import com.runmycode.mapred.JobID;
import com.runmycode.mapred.JobStatus;
import com.runmycode.mapred.TaskCompletionEvent;
import com.runmycode.mapred.TaskID;
import com.runmycode.mapred.TaskReport;

public interface RMI_RunningJob
{ 
	/**
	 * Get the name of the job.
	 * 
	 * @return the name of the job.
	 */
	public String getJobName(JobID jobID);

	public Set<TaskReport> getMapTaskReports(JobID jobID) throws IOException;

	/**
	 * Gets the diagnostic messages for a given task attempt.
	 * @param taskid
	 * @return the list of diagnostic messages for the task
	 * @throws IOException
	 */
	public Set<String> getTaskDiagnostics(JobID jobID, TaskID taskid) throws IOException;

	/**
	 * Check if the job is finished or not. 
	 * This is a non-blocking call.
	 * 
	 * @return <code>true</code> if the job is complete, else <code>false</code>.
	 * @throws IOException
	 */
	public boolean isComplete(JobID jobID) throws IOException;

	public Set<TaskReport> getReduceTaskReports(JobID jobID);
	
	/**
	 * Get the <i>progress</i> of the job's map-tasks, as a float between 0.0 
	 * and 1.0.  When all map tasks have completed, the function returns 1.0.
	 * 
	 * @return the progress of the job's map-tasks.
	 * @throws IOException
	 */
	public float getMapProgress(JobID jobID) throws IOException;

	/**
	 * Get the <i>progress</i> of the job's reduce-tasks, as a float between 0.0 
	 * and 1.0.  When all reduce tasks have completed, the function returns 1.0.
	 * 
	 * @return the progress of the job's reduce-tasks.
	 * @throws IOException
	 */
	public float getReduceProgress(JobID jobID) throws IOException;

	/**
	 * Get events indicating completion (success/failure) of component tasks.
	 *  
	 * @param startFrom index to start fetching events from
	 * @return an array of {@link TaskCompletionEvent}s
	 * @throws IOException
	 */
	public Set<TaskCompletionEvent> getTaskCompletionEvents(JobID jobID, int startFrom) throws IOException;

	public long getStartTime(JobID jobID);
	  
	public long getLaunchTime(JobID jobID);
	  
	public long getFinishTime(JobID jobID);
	  
	/**
	 * Check if the job completed successfully. 
	 * 
	 * @return <code>true</code> if the job succeeded, else <code>false</code>.
	 * @throws IOException
	 */
	public boolean isSuccessful(JobID jobID) throws IOException;
	
	/**
	 * Returns the current state of the Job.
	 * {@link JobStatus}
	 * 
	 * @throws IOException
	 */
	  public JobStatus getJobState(JobID jobID) throws IOException;

	/**
	 * Kill the running job.  Blocks until all job tasks have been
	 * killed as well.  If the job is no longer running, it simply returns.
	 * 
	 * @throws IOException
	 */
	public void killJob(JobID jobID) throws IOException;

	/**
	 * Set the priority of a running job.
	 * @param priority the new priority for the job.
	 * @throws IOException
	 */
	public void setJobPriority(JobID jobID, String priority) throws IOException;

	/**
	 * Kill indicated task attempt.
	 * 
	 * @param taskId the id of the task to be terminated.
	 * @param shouldFail if true the task is failed and added to failed tasks 
	 *                   list, otherwise it is just killed, w/o affecting 
	 *                   job failure status.  
	 * @throws IOException
	 */
	public void killTask(JobID jobID, TaskID taskId, boolean shouldFail) throws IOException;

	/**
	 * Gets the counters for this job.
	 * 
	 * @return the counters for this job.
	 * @throws IOException
	 */
	public Counters getCounters(JobID jobID) throws IOException;

	/**
	 * Get failure info for the job.
	 * @return the failure info for the job.
	 * @throws IOException
	 */
	public String getFailureInfo(JobID jobID) throws IOException;

	public Set<TaskReport> getSplitTaskReports(JobID jobID);
}
