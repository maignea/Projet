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

import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * <code>JobClient</code> is the primary interface for the user-job to interact
 * with the {@link JobTracker}.
 * 
 * <code>JobClient</code> provides facilities to submit jobs, track their 
 * progress, access component-tasks' reports/logs, get the Map-Reduce cluster
 * status information etc.
 * 
 * <p>The job submission process involves:
 * <ol>
 *   <li>
 *   Checking the input and output specifications of the job.
 *   </li>
 *   <li>
 *   Computing the {@link InputSplit}s for the job.
 *   </li>
 *   <li>
 *   Setup the requisite accounting information for the {@link DistributedCache} 
 *   of the job, if necessary.
 *   </li>
 *   <li>
 *   Copying the job's jar and configuration to the map-reduce system directory 
 *   on the distributed file-system. 
 *   </li>
 *   <li>
 *   Submitting the job to the <code>JobTracker</code> and optionally monitoring
 *   it's status.
 *   </li>
 * </ol></p>
 *  
 * Normally the user creates the application, describes various facets of the
 * job via {@link JobConf} and then uses the <code>JobClient</code> to submit 
 * the job and monitor its progress.
 * 
 * <p>Here is an example on how to use <code>JobClient</code>:</p>
 * <p><blockquote><pre>
 *     // Create a new JobConf
 *     JobConf job = new JobConf(new Configuration(), MyJob.class);
 *     
 *     // Specify various job-specific parameters     
 *     job.setJobName("myjob");
 *     
 *     job.setInputPath(new Path("in"));
 *     job.setOutputPath(new Path("out"));
 *     
 *     job.setMapperClass(MyJob.MyMapper.class);
 *     job.setReducerClass(MyJob.MyReducer.class);
 *
 *     // Submit the job, then poll for progress until the job is complete
 *     JobClient.runJob(job);
 * </pre></blockquote></p>
 * 
 * <h4 id="JobControl">Job Control</h4>
 * 
 * <p>At times clients would chain map-reduce jobs to accomplish complex tasks 
 * which cannot be done via a single map-reduce job. This is fairly easy since 
 * the output of the job, typically, goes to distributed file-system and that 
 * can be used as the input for the next job.</p>
 * 
 * <p>However, this also means that the onus on ensuring jobs are complete 
 * (success/failure) lies squarely on the clients. In such situations the 
 * various job-control options are:
 * <ol>
 *   <li>
 *   {@link #runJob(JobConf)} : submits the job and returns only after 
 *   the job has completed.
 *   </li>
 *   <li>
 *   {@link #submitJob(JobConf)} : only submits the job, then poll the 
 *   returned handle to the {@link RunningJob} to query status and make 
 *   scheduling decisions.
 *   </li>
 *   <li>
 *   {@link JobConf#setJobEndNotificationURI(String)} : setup a notification
 *   on job-completion, thus avoiding polling.
 *   </li>
 * </ol></p>
 * 
 * @see JobConf
 * @see ClusterStatus
 * @see Tool
 * @see DistributedCache
 */
public interface JobClient 
{	
	/**
	 * Submit a job to the MR system.
	 * 
	 * This returns a handle to the {@link RunningJob} which can be used to track
	 * the running-job.
	 * 
	 * @param job the job configuration.
	 * @return a handle to the {@link RunningJob} which can be used to track the
	 *         running-job.
	 * @throws FileNotFoundException
	 * @throws IOException
	 * @throws InterruptedException 
	 */
	public RunningJob submitJob(final JobConf job) throws Exception;

	/**
	 * Get an {@link RunningJob} object to track an ongoing job.  Returns
	 * null if the id does not correspond to any known job.
	 * 
	 * @param jobid the jobid of the job.
	 * @return the {@link RunningJob} handle to track the job, null if the 
	 *         <code>jobid</code> doesn't correspond to any known job.
	 * @throws IOException
	 */
	public RunningJob getJob(JobID jobid) throws IOException;

	/**
	 * Get the information of the current state of the map tasks of a job.
	 * 
	 * @param jobId the job to query.
	 * @return the list of all of the map tips.
	 * @throws IOException
	 */
	//public TaskReport[] getMapTaskReports(JobID jobId) throws IOException;
  
	/**
	 * Get the information of the current state of the reduce tasks of a job.
	 * 
	 * @param jobId the job to query.
	 * @return the list of all of the reduce tips.
	 * @throws IOException
	 */    
	/*public TaskReport[] getReduceTaskReports(JobID jobId) throws IOException 
	{
		return jobSubmitClient.getReduceTaskReports(jobId);
  	}*/

	/**
	 * Get the information of the current state of the cleanup tasks of a job.
	 * 
	 * @param jobId the job to query.
	 * @return the list of all of the cleanup tips.
	 * @throws IOException
	 */    
	/*public TaskReport[] getCleanupTaskReports(JobID jobId) throws IOException 
	{
    	return jobSubmitClient.getCleanupTaskReports(jobId);
  	}*/

	/**
	 * Get the information of the current state of the setup tasks of a job.
	 * 
	 * @param jobId the job to query.
	 * @return the list of all of the setup tips.
	 * @throws IOException
	 */    
	/*public TaskReport[] getSetupTaskReports(JobID jobId) throws IOException 
	{
    	return jobSubmitClient.getSetupTaskReports(jobId);
  	}*/
  
	/** 
	 * Get the jobs that are not completed and not failed.
	 * 
	 * @return array of {@link JobStatus} for the running/to-be-run jobs.
	 * @throws IOException
	 */
	/*public JobStatus[] jobsToComplete() throws IOException 
	{
    	return jobSubmitClient.jobsToComplete();
  	}*/

	/** 
	 * Get the jobs that are submitted.
	 * 
	 * @return array of {@link JobStatus} for the submitted jobs.
	 * @throws IOException
	 */
	/*public JobStatus[] getAllJobs() throws IOException 
	{
		return jobSubmitClient.getAllJobs();
  	}*/
  
	/** 
	 * Utility that submits a job, then polls for progress until the job is
	 * complete.
	 * 
	 * @param job the job configuration.
	 * @throws IOException if the job fails
	 * @throws InterruptedException 
	 */
	/*public static RunningJob runJob(JobConf job) throws IOException, InterruptedException 
	{
    	JobClient jc = new JobClient(job);
    	RunningJob rj = jc.submitJob(job);
    	try 
    	{
      		if (!jc.monitorAndPrintJob(job, rj)) 
      		{
        		throw new IOException("Job failed!");
      		}
    	}
    	catch (InterruptedException ie) 
    	{
      		Thread.currentThread().interrupt();
    	}
    	return rj;
  	}*/
  


  /**
   * Get status information about the max available Maps in the cluster.
   *  
   * @return the max available Maps in the cluster
   * @throws IOException
   */
  /*public int getDefaultMaps() throws IOException {
	return 0;

  }*/

  /**
   * Get status information about the max available Reduces in the cluster.
   *  
   * @return the max available Reduces in the cluster
   * @throws IOException
   */
  /*public int getDefaultReduces() throws IOException {
	return 0;

  }*/
  
  /**
   * Gets all the jobs which were added to particular Job Queue
   * 
   * @param queueName name of the Job Queue
   * @return Array of jobs present in the job queue
   * @throws IOException
   */
  
  /*public JobStatus[] getJobsFromQueue(String queueName) throws IOException {
    return jobSubmitClient.getJobsFromQueue(queueName);
  }*/
}

