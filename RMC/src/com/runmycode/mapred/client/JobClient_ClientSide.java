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
package com.runmycode.mapred.client;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.TreeMap;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.QueueReceiver;
import javax.jms.QueueSender;
import javax.jms.QueueSession;
import javax.jms.Session;

import org.apache.log4j.Logger;

import com.runmycode.mapred.JobClient;
import com.runmycode.mapred.JobConf;
import com.runmycode.mapred.JobID;
import com.runmycode.mapred.RunningJob;
import com.runmycode.mapred.TaskReport;
import com.runmycode.mapred.server.RunningJobMessageClientToServer;


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
public class JobClient_ClientSide implements JobClient, MessageListener
{
    private transient Logger logger = Logger.getLogger(com.runmycode.mapred.client.JobClient_ClientSide.class);

    public void onMessage( Message message )
    { 	
		if (message instanceof ObjectMessage){
			try {
				
				ObjectMessage objectMessage = (ObjectMessage)message;
				RunningJobMessageClientToServer runningJobMessage = (RunningJobMessageClientToServer)objectMessage.getObject();
				
				logger.debug("message recu dans JobClient_ClientSide : " + this + " = " + runningJobMessage);
				
				JobID jobID = runningJobMessage.getJobID();
				
				RunningJobClient runningJob = (RunningJobClient)jobs.get(jobID);
				
				Class runningJobClass = runningJob.getClass();
				
				String methodName = runningJobMessage.getMethodName();
				Object[] methodParameters = runningJobMessage.getMethodParameters();
				Method[] methods = runningJobClass.getDeclaredMethods();
				int i = 0;
				while(i < methods.length && methods[i].getName().equals(methodName) == false)
				{
					i++;
				}
				Method method;
				if(i < methods.length)
				{
					method = methods[i];
					Object returnValue = method.invoke(runningJob, methodParameters);
					
				}
			} 
			catch (Exception e) 
			{
				e.printStackTrace();
			} 						
		}
	}
	
	private TreeMap<JobID, RunningJob> jobs = new TreeMap<JobID, RunningJob>();
	
	private QueueConnectionFactory connectionFactory;
	private Queue clientToServerQueue;
	private QueueSession jobSubmissionQueueSession;
	private QueueReceiver queueReceiver;
	private QueueSender jobSubmissionQueueSender;
	
	public JobClient_ClientSide(QueueConnectionFactory connectionFactory, Queue jobSubmissionQueue, Queue clientToServerQueue, Queue serverToClientQueue) throws JMSException 
	{
		this.connectionFactory = connectionFactory;
		this.clientToServerQueue = clientToServerQueue;
		QueueConnection connection = connectionFactory.createQueueConnection();
		QueueSession session = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
		queueReceiver = session.createReceiver(serverToClientQueue);
		queueReceiver.setMessageListener(this);
		connection.start();
		
		logger.debug("JobClient_ClientSide : " + connection + " " + queueReceiver);
		
		connection = connectionFactory.createQueueConnection();
		jobSubmissionQueueSession = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
		jobSubmissionQueueSender = session.createSender(jobSubmissionQueue);
	}
      
  /**
   * Submit a job to the MR system.
   * 
   * This returns a handle to the {@link RunningJob} which can be used to track
   * the running-job.
   * 
   * @param job the job configuration.
   * @return a handle to the {@link RunningJob} which can be used to track the running-job.
   * @throws FileNotFoundException
   * @throws IOException
   * @throws InterruptedException 
   * @throws JMSException 
   */
  public synchronized RunningJob submitJob(final JobConf job) throws Exception 
  {
	  if(job.getJobID() == null)
	  {
		  throw new Exception("You should call setJobName and setUser before submitting a job !");
	  }
	  RunningJob runningJob = new RunningJobClient(job, connectionFactory, clientToServerQueue);
	  
	  jobs.put(job.getJobID(), runningJob);
	  
	  logger.debug("submitJob dans JobClient_ClientSide + envoi message");
	  
	  Message message = jobSubmissionQueueSession.createObjectMessage(job);
	  jobSubmissionQueueSender.send(message);
	
	  return runningJob;
  }
  
  /**
   * Get an {@link RunningJob} object to track an ongoing job.  Returns
   * null if the id does not correspond to any known job.
   * 
   * @param jobid the jobid of the job.
   * @return the {@link RunningJob} handle to track the job, null if the 
   *         <code>jobid</code> doesn't correspond to any known job.
   * @throws IOException
   */
  public RunningJob getJob(JobID jobid) throws IOException 
  {	
	  return jobs.get(jobid);
  }

  /**
   * Get the information of the current state of the map tasks of a job.
   * 
   * @param jobId the job to query.
   * @return the list of all of the map tips.
   * @throws IOException
   */
  public TaskReport[] getMapTaskReports(JobID jobId) throws IOException 
  {
	  return null;
  }
  
  /**
   * Get the information of the current state of the reduce tasks of a job.
   * 
   * @param jobId the job to query.
   * @return the list of all of the reduce tips.
   * @throws IOException
   */    
  /*public TaskReport[] getReduceTaskReports(JobID jobId) throws IOException {
	return jobSubmitClient.getReduceTaskReports(jobId);
  }*/
  
  /** 
   * Get the jobs that are not completed and not failed.
   * 
   * @return array of {@link JobStatus} for the running/to-be-run jobs.
   * @throws IOException
   */
  /*public JobStatus[] jobsToComplete() throws IOException {
    return jobSubmitClient.jobsToComplete();
  }*/

  /** 
   * Get the jobs that are submitted.
   * 
   * @return array of {@link JobStatus} for the submitted jobs.
   * @throws IOException
   */
  /*public JobStatus[] getAllJobs() throws IOException {
	return jobSubmitClient.getAllJobs();
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
}

