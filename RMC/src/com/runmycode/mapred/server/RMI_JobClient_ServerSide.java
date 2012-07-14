package com.runmycode.mapred.server;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.QueueReceiver;
import javax.jms.QueueSession;
import javax.jms.Session;

import org.apache.log4j.Logger;
import org.springframework.data.mongodb.core.MongoOperations;

import com.runmycode.mapred.Counters;
import com.runmycode.mapred.JobClient;
import com.runmycode.mapred.JobConf;
import com.runmycode.mapred.JobID;
import com.runmycode.mapred.JobInProgress;
import com.runmycode.mapred.JobInProgressFactory;
import com.runmycode.mapred.JobStatus;
import com.runmycode.mapred.RunningJob;
import com.runmycode.mapred.TaskCompletionEvent;
import com.runmycode.mapred.TaskID;
import com.runmycode.mapred.TaskReport;
import com.runmycode.mapred.impl.LocalSplitterTest;
import com.runmycode.mapred.impl.Splitter;
import com.runmycode.mapred.mongoDB.MongoDBFactory;


public class RMI_JobClient_ServerSide implements JobClient, MessageListener, RMI_RunningJob
{
	private transient Logger logger = Logger.getLogger(com.runmycode.mapred.server.RMI_JobClient_ServerSide.class);
	private transient MongoOperations mongoOperations = MongoDBFactory.getMongoOperations();
	
    //Listener on jobSubmissionQueue
	public void onMessage( Message message )
	{ 		
		if (message instanceof ObjectMessage)
		{
			try 
			{			
				ObjectMessage objectMessage = (ObjectMessage)message;
				JobConf jobConf = (JobConf)objectMessage.getObject();
				
				logger.debug("On Message submitJob dans JobClient_ServerSide");
				
				this.submitJob(jobConf);				
			} 
			catch (Exception e)
			{
				e.printStackTrace();
			} 				
		}
	}
	
	class LaunchingJobTask implements Callable<Integer>
	{
		private JobConf jobConf;

		public LaunchingJobTask(JobConf jobConf)
		{
			this.jobConf = jobConf;
		}

		@Override
		public Integer call() throws Exception 
		{
			try{

				logger.debug("Job launching ....");
				//Creating a JobInProgress to manage the progression of the job
				JobID jobID = jobConf.getJobID();
				JobInProgress jobInProgress = jobInProgressFactory.getJobInProgress(jobID);
				jobInProgress.initTasks();
				
				//DB storage
				mongoOperations.insert(jobInProgress, "jobInProgress");
				
				Splitter splitter = new LocalSplitterTest();
				splitter.runSplit(jobInProgressFactory, jobID);
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}			
			return null;
		}		
	}
	
	private TreeMap<JobID, RunningJob> jobs = new TreeMap<JobID, RunningJob>();
	
	private QueueConnectionFactory connectionFactory;
	private QueueReceiver queueReceiver;
	private int numberOfThreads;
	private ExecutorService executorService;

	private JobInProgressFactory jobInProgressFactory;
	
	//Constructor
	public RMI_JobClient_ServerSide(JobInProgressFactory jobInProgressFactory, QueueConnectionFactory connectionFactory, Queue jobSubmissionQueue, int numberOfThreads) throws JMSException 
	{
		this.jobInProgressFactory = jobInProgressFactory;
		this.connectionFactory = connectionFactory;
		
		QueueConnection connection = connectionFactory.createQueueConnection();
		QueueSession session = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
		
		queueReceiver = session.createReceiver(jobSubmissionQueue);
		queueReceiver.setMessageListener(this);
		connection.start();
		
		this.numberOfThreads = numberOfThreads;		
		executorService = Executors.newFixedThreadPool(numberOfThreads);
	}
	
	@Override
	public RunningJob submitJob(JobConf job) throws Exception 
	{
		logger.debug("submitJob dans JobClient_ServerSide");
		
		/*String inputpath = job.getInputPath();
		
		//writing treatments + data in DB ...
		File folder = new File(inputpath);
		if (folder.mkdirs()) 
		{		
			System.out.println("Ajout des dossiers : " + folder.getPath());
			
			File file = new File(inputpath+"\\config.xml");
			BufferedWriter writer = new BufferedWriter(new FileWriter(file));
			writer.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
			writer.newLine();
			writer.write("<InstanceSet N=\"0\">");
			writer.newLine();
			writer.write("\t<Param paramName=\"n\" paramDim=\"SCALAR\" paramType=\"INTEGER\" paramConfigString=\"0:1:10\"/>");
			writer.newLine();
			writer.write("</InstanceSet>");
			writer.close();
		} 
		else 
		{
			logger.error("Echec des dossiers : " + folder.getPath());
		}*/
		
		mongoOperations.insert(job, "jobConf");
		
		RunningJob runningJob = new RunningJobServer(job.getJobID());
		jobs.put(job.getJobID(), runningJob);
		
		executorService.submit(new LaunchingJobTask(job));
		
		return null;
	}

	@Override
	public RunningJob getJob(JobID jobid) throws IOException
	{
		return null;
	}

	@Override
	public String getJobName(JobID jobID) 
	{
		RunningJob runningJob = jobs.get(jobID);
		return runningJob.getJobName();
	}

	@Override
	public Set<TaskReport> getMapTaskReports(JobID jobID) throws IOException 
	{
		RunningJob runningJob = jobs.get(jobID);
		return runningJob.getMapTaskReports();
	}

	@Override
	public Set<String> getTaskDiagnostics(JobID jobID, TaskID taskid) throws IOException 
	{
		RunningJob runningJob = jobs.get(jobID);
		return runningJob.getTaskDiagnostics(taskid);
	}

	@Override
	public boolean isComplete(JobID jobID) throws IOException 
	{
		RunningJob runningJob = jobs.get(jobID);
		return runningJob.isComplete();
	}

	@Override
	public Set<TaskReport> getReduceTaskReports(JobID jobID) 
	{
		RunningJob runningJob = jobs.get(jobID);
		return runningJob.getReduceTaskReports();
	}

	@Override
	public float getMapProgress(JobID jobID) throws IOException
	{
		RunningJob runningJob = jobs.get(jobID);	
		return runningJob.getMapProgress();
	}

	@Override
	public float getReduceProgress(JobID jobID) throws IOException 
	{
		RunningJob runningJob = jobs.get(jobID);	
		return runningJob.getReduceProgress();
	}

	@Override
	public Set<TaskCompletionEvent> getTaskCompletionEvents(JobID jobID, int startFrom) throws IOException
	{
		RunningJob runningJob = jobs.get(jobID);	
		return runningJob.getTaskCompletionEvents(startFrom);
	}

	@Override
	public boolean isSuccessful(JobID jobID) throws IOException
	{
		RunningJob runningJob = jobs.get(jobID);	
		return runningJob.isSuccessful();
	}

	@Override
	public JobStatus getJobState(JobID jobID) throws IOException 
	{
		RunningJob runningJob = jobs.get(jobID);	
		return runningJob.getJobState();
	}

	@Override
	public void killJob(JobID jobID) throws IOException 
	{
		RunningJob runningJob = jobs.get(jobID);	
		runningJob.killJob();
	}

	@Override
	public void setJobPriority(JobID jobID, String priority) throws IOException 
	{
		RunningJob runningJob = jobs.get(jobID);	
		runningJob.setJobPriority(priority);
	}

	@Override
	public void killTask(JobID jobID, TaskID taskId, boolean shouldFail) throws IOException 
	{
		RunningJob runningJob = jobs.get(jobID);	
		runningJob.killTask(taskId, shouldFail);
		
	}

	@Override
	public Counters getCounters(JobID jobID) throws IOException 
	{
		RunningJob runningJob = jobs.get(jobID);	
		return runningJob.getCounters();
	}

	@Override
	public String getFailureInfo(JobID jobID) throws IOException
	{
		RunningJob runningJob = jobs.get(jobID);	
		return runningJob.getFailureInfo();
	}

	@Override
	public Set<TaskReport> getSplitTaskReports(JobID jobID) 
	{
		RunningJob runningJob = jobs.get(jobID);
		return runningJob.getSplitTaskReports();
	}

	@Override
	public long getStartTime(JobID jobID) {
		RunningJob runningJob = jobs.get(jobID);
		return runningJob.getStartTime();
	}

	@Override
	public long getLaunchTime(JobID jobID) {
		RunningJob runningJob = jobs.get(jobID);
		return runningJob.getLaunchTime();
	}

	@Override
	public long getFinishTime(JobID jobID) {
		RunningJob runningJob = jobs.get(jobID);
		return runningJob.getFinishTime();
	}
}
