package com.runmycode.mapred.client;

import java.io.IOException;
import java.util.Set;
import com.runmycode.mapred.Counters;
import com.runmycode.mapred.JobID;
import com.runmycode.mapred.JobStatus;
import com.runmycode.mapred.RunningJob;
import com.runmycode.mapred.TaskCompletionEvent;
import com.runmycode.mapred.TaskID;
import com.runmycode.mapred.TaskReport;
import com.runmycode.mapred.server.RMI_RunningJob;

public class RMI_RunningJobClient implements RunningJob
{	
	private JobID jobID;
	
	private String jobFile;
	private String trackingURL;
	private JobStatus jobState;
	
	private RMI_RunningJob rmiRunningJob;

	public RMI_RunningJob getRmiRunningJob() 
	{
		return rmiRunningJob;
	}

	public void setRmiRunningJob(RMI_RunningJob rmiRunningJob) 
	{
		this.rmiRunningJob = rmiRunningJob;
	}

	public void setJobID(JobID jobID) 
	{
		this.jobID = jobID;
	}

	@Override
	public JobID getID()
	{
		return jobID;
	}

	@Override
	public synchronized String getJobName()
	{		
		return rmiRunningJob.getJobName(jobID);
	}

	@Override
	public float getMapProgress() throws IOException 
	{
		return rmiRunningJob.getMapProgress(jobID);
	}

	@Override
	public float getReduceProgress() throws IOException 
	{
		return rmiRunningJob.getReduceProgress(jobID);
	}

	@Override
	public Set<TaskCompletionEvent> getTaskCompletionEvents(int startFrom) throws IOException 
	{
		return null;
	}

	@Override
	public boolean isComplete() throws IOException 
	{
		return rmiRunningJob.isComplete(jobID);
	}

	@Override
	public boolean isSuccessful() throws IOException 
	{
		return rmiRunningJob.isSuccessful(jobID);
	}

	@Override
	public void waitForCompletion() throws IOException 
	{
			
	}

	@Override
	public JobStatus getJobState() throws IOException 
	{
		return rmiRunningJob.getJobState(jobID);
	}
	
	public void setJobState(JobStatus jobState)
	{
		System.out.println("setJobState dans RunningJobClient " + this);
		this.jobState = jobState;
	}
	
	@Override
	public void killJob() throws IOException 
	{
				
	}

	@Override
	public void setJobPriority(String priority) throws IOException 
	{
		
		
	}

	@Override
	public void killTask(TaskID taskId, boolean shouldFail) throws IOException 
	{
		
	}

	@Override
	public Counters getCounters() throws IOException 
	{
		return rmiRunningJob.getCounters(jobID);
	}

	@Override
	public String getFailureInfo() throws IOException 
	{
		return rmiRunningJob.getFailureInfo(jobID);
	}

	@Override
	public Set<String> getTaskDiagnostics(TaskID taskid) throws IOException 
	{
		return rmiRunningJob.getTaskDiagnostics(jobID, taskid);
	}

	@Override
	public Set<TaskReport> getMapTaskReports() throws IOException 
	{
		return rmiRunningJob.getMapTaskReports(jobID);
	}

	@Override
	public Set<TaskReport> getReduceTaskReports() 
	{
		return rmiRunningJob.getReduceTaskReports(jobID);
	}

	@Override
	public Set<TaskReport> getSplitTaskReports() 
	{
		return rmiRunningJob.getSplitTaskReports(jobID);
	}

	@Override
	public long getStartTime() {
		return rmiRunningJob.getStartTime(jobID);
	}

	@Override
	public long getLaunchTime() {
		return rmiRunningJob.getLaunchTime(jobID);
	}

	@Override
	public long getFinishTime() {
		return rmiRunningJob.getFinishTime(jobID);
	}
}

