package com.runmycode.mapred.server;

import java.util.Calendar;
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
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import com.runmycode.mapred.JobInProgressFactory;
import com.runmycode.mapred.TaskID;
import com.runmycode.mapred.TaskInProgress;
import com.runmycode.mapred.TaskInProgressID;
import com.runmycode.mapred.TaskStatus;
import com.runmycode.mapred.mongoDB.MongoDBFactory;

public class ReporterServerSide implements MessageListener
{
	private transient MongoOperations mongoOperations = MongoDBFactory.getMongoOperations();
	private transient Logger logger = Logger.getLogger(com.runmycode.mapred.server.ReporterServerSide.class);
	
    //Listener on computingReportationQueue
	public void onMessage( Message message )
	{
		if (message instanceof ObjectMessage)
		{
			try 
			{			
				ObjectMessage objectMessage = (ObjectMessage)message;
				ComputingReportationMessage  computingReportationMessage = (ComputingReportationMessage)objectMessage.getObject();
				
				logger .debug("on Message ComputingReportationMessage dans ReporterServerSide : " + computingReportationMessage.getTaskStatus().getPhase());
				
				executorService.submit(new ReportingTask(computingReportationMessage));
			} 
			catch (Exception e)
			{
				e.printStackTrace();
			} 				
		}
	}
	
    class ReportingTask implements Callable<Integer>
	{
		private ComputingReportationMessage computingReportationMessage;

		public ReportingTask(ComputingReportationMessage computingReportationMessage)
		{
			this.computingReportationMessage = computingReportationMessage;
		}

		@Override
		public Integer call() throws Exception 
		{
			try
			{
				TaskID taskID = computingReportationMessage.getTaskID();
				TaskInProgressID taskInProgressID = computingReportationMessage.getTaskInProgressID();
				TaskStatus taskStatus = computingReportationMessage.getTaskStatus();
				TaskStatus.State state = taskStatus.getRunState();
				
				TaskInProgress taskInProgress = mongoOperations.findById(taskInProgressID.getId(), TaskInProgress.class);
				taskInProgress.setJobInProgressFactory(jobInProgressFactory);
				mongoOperations.updateFirst(Query.query(Criteria.where("id").is(taskInProgressID.getId())), Update.update("progress", 1), TaskInProgress.class);
				mongoOperations.updateFirst(Query.query(Criteria.where("id").is(taskInProgressID.getId())), Update.update("execFinishTime", Calendar.getInstance().getTimeInMillis()), TaskInProgress.class);
				taskInProgress.updateTask(taskID, state);
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}
			return 0;
		}		
	}
		
	private QueueConnectionFactory connectionFactory;
	private Queue computingReportationQueue;
	private QueueSession sessionComputingReportation;
	private QueueReceiver queueReceiver;
	private int numberOfThreads;
	private ExecutorService executorService;
	private JobInProgressFactory jobInProgressFactory;
	
	//Constructeur
	public ReporterServerSide(JobInProgressFactory jobInProgressFactory, QueueConnectionFactory connectionFactory, Queue computingReportationQueue, int numberOfThreads) throws JMSException 
	{
		this.jobInProgressFactory = jobInProgressFactory;
		this.connectionFactory = connectionFactory;
		this.computingReportationQueue = computingReportationQueue;
		QueueConnection connection = connectionFactory.createQueueConnection();
		QueueSession session = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
		queueReceiver = session.createReceiver(computingReportationQueue);
		queueReceiver.setMessageListener(this);
		connection.start();
				
		this.numberOfThreads = numberOfThreads;		
		executorService = Executors.newCachedThreadPool();
	}
}
