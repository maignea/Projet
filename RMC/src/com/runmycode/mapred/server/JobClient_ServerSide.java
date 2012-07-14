package com.runmycode.mapred.server;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Method;
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
import javax.jms.QueueSender;
import javax.jms.QueueSession;
import javax.jms.Session;

import org.apache.log4j.Logger;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.data.mongodb.core.MongoOperations;


import com.runmycode.mapred.JobClient;
import com.runmycode.mapred.JobConf;
import com.runmycode.mapred.JobID;
import com.runmycode.mapred.JobInProgress;
import com.runmycode.mapred.JobInProgressFactory;
import com.runmycode.mapred.RunningJob;
import com.runmycode.mapred.impl.LocalSplitterTest;
import com.runmycode.mapred.impl.Splitter;
import com.runmycode.mapred.mongoDB.MongoDBFactory;

public class JobClient_ServerSide implements JobClient, MessageListener
{
	private transient MongoOperations mongoOperations = MongoDBFactory.getMongoOperations();
	private transient ApplicationContext context = new ClassPathXmlApplicationContext("applicationContextReporter.xml");
    private transient Logger logger = Logger.getLogger(com.runmycode.mapred.server.JobClient_ServerSide.class);
    
	//Listener on jobSubmissionQueue
	public void onMessage(Message message)
	{ 		
		if (message instanceof ObjectMessage)
		{
			try 
			{			
				ObjectMessage objectMessage = (ObjectMessage)message;
				JobConf jobConf = (JobConf)objectMessage.getObject();
				
				logger.debug("on Message submitJob dans JobClient_ServerSide");
				
				this.submitJob(jobConf);				
			} 
			catch (Exception e)
			{
				e.printStackTrace();
			} 				
		}
	}
	
    //Listener on clientToServerQueue
	class RunningJobsMessageListener implements MessageListener
	{
		public void onMessage(Message message)
		{ 		
			if (message instanceof ObjectMessage)
			{
				try
				{				
					ObjectMessage objectMessage = (ObjectMessage)message;
					RunningJobMessageClientToServer runningJobMessage = (RunningJobMessageClientToServer)objectMessage.getObject();
					
					logger.debug("on Message RunningJobMessageClientToServer dans JobClient_ServerSide : " + runningJobMessage);
					
					JobID jobID = runningJobMessage.getJobID();
					RunningJob runningJob = jobs.get(jobID);
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
						
						methodParameters = new Object[1];
						methodParameters[0] = returnValue;
						RunningJobMessageClientToServer respons = new RunningJobMessageClientToServer(jobID, runningJobMessage.getResponsMethodName(), null, methodParameters);
						
						logger.debug("envoi message reponse dans JobClient_ServerSide : " + respons);
						
						message = sessionServerToClient.createObjectMessage(respons);
						queueServerToClientSender.send(message);
						
						logger.debug("message reponse envoye dans JobClient_ServerSide : " + respons);
					} 										
				} 
				catch (Exception e)
				{				
					e.printStackTrace();
				} 								
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
			logger.debug("Job launching ....");
			
			//Creating a JobInProgress to manage the progression of the job
			JobID jobID = jobConf.getJobID();
			JobInProgress jobInProgress = jobInProgressFactory.getJobInProgress(jobID);
			jobInProgress.initTasks();
			logger.debug("jobInProgress = " + jobInProgress);
			
			//DB storage
			mongoOperations.insert(jobInProgress, "jobInProgress");
			
			//Split
			LocalSplitterTest splitter = new LocalSplitterTest();
			splitter.runSplit(jobInProgressFactory, jobID);
			
			return null;
		}
	}
	
	private TreeMap<JobID, RunningJob> jobs = new TreeMap<JobID, RunningJob>();
	
	private QueueConnectionFactory connectionFactory;
	private Queue serverToClientQueue;
	private QueueSession sessionServerToClient;
	private QueueSender queueServerToClientSender;
	private QueueReceiver queueReceiver;
	private int numberOfThreads;
	private ExecutorService executorService;
	private JobInProgressFactory jobInProgressFactory;
	
	public JobClient_ServerSide(JobInProgressFactory jobInProgressFactory, QueueConnectionFactory connectionFactory, Queue jobSubmissionQueue, Queue serverToClientQueue, Queue clientToServerQueue, int numberOfThreads) throws JMSException 
	{
		this.connectionFactory = connectionFactory;
		this.serverToClientQueue = clientToServerQueue;
		QueueConnection connection = connectionFactory.createQueueConnection();
		QueueSession session = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
		queueReceiver = session.createReceiver(jobSubmissionQueue);
		queueReceiver.setMessageListener(this);
		connection.start();
		
		connection = connectionFactory.createQueueConnection();
		session = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
		queueReceiver = session.createReceiver(clientToServerQueue);
		queueReceiver.setMessageListener(new RunningJobsMessageListener());
		connection.start();
		
		
		connection = connectionFactory.createQueueConnection();
		sessionServerToClient = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
		queueServerToClientSender = sessionServerToClient.createSender(serverToClientQueue);
		
		this.numberOfThreads = numberOfThreads;		
		executorService = Executors.newFixedThreadPool(numberOfThreads);
		
		this.jobInProgressFactory = jobInProgressFactory;
	}
	
	@Override
	public RunningJob submitJob(JobConf job) throws Exception 
	{
		logger.debug("submitJob dans JobClient_ServerSide");
		
		/*String inputpath = job.getInputPath();
		
		//writing treatments + data in DB
		File folder = new File(inputpath);
		if (folder.mkdirs()) 
		{		
			logger.debug("Ajout des dossiers : " + folder.getPath());
			
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
}
