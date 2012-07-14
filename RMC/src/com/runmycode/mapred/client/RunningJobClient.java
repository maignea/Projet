package com.runmycode.mapred.client;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.QueueSender;
import javax.jms.QueueSession;
import javax.jms.Session;

import org.apache.log4j.Logger;

import com.runmycode.mapred.Counters;
import com.runmycode.mapred.JobConf;
import com.runmycode.mapred.JobID;
import com.runmycode.mapred.JobStatus;
import com.runmycode.mapred.RunningJob;
import com.runmycode.mapred.TaskCompletionEvent;
import com.runmycode.mapred.TaskID;
import com.runmycode.mapred.TaskReport;
import com.runmycode.mapred.server.RunningJobMessageClientToServer;

public class RunningJobClient implements RunningJob
{
	private transient Logger logger = Logger.getLogger(com.runmycode.mapred.client.RunningJobClient.class);
	
	private JobID jobID;
	private Queue clientToServerQueue;
	private QueueSession session;
	private QueueSender queueSender;
	private String jobName;
	
	private CountDownLatch countDownLatch;
	
	private JobStatus jobState;
	private Set<TaskReport> mapTaskReports;
	private Set<String> taskDiagnostics;
	private Set<TaskReport> reduceTaskReports;
	private Set<TaskReport> splitTaskReports;
	

	public RunningJobClient(JobConf job, QueueConnectionFactory connectionFactory, Queue clientToServerQueue) throws JMSException 
	{
		this.jobID = job.getJobID();
		QueueConnection connection = connectionFactory.createQueueConnection();
		session = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
		this.clientToServerQueue = clientToServerQueue;
		queueSender = session.createSender(clientToServerQueue);
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
		try 
		{		
			logger.debug("debut getJobName dans RunningJobClient + envoi message " + this);
			
			RunningJobMessageClientToServer runningJobMessage = new RunningJobMessageClientToServer(jobID, "getJobName", "setJobName", null);
			Message message = session.createObjectMessage(runningJobMessage);
			queueSender.send(message);
			
			logger.debug("attente message retour getJobName dans RunningJobClient" + this);
			
			countDownLatch = new CountDownLatch(1);
			countDownLatch.await();
			
			logger.debug("fin attente message retour getJobName dans RunningJobClient");
			
			return jobName;			
		} 
		catch (JMSException e) 
		{
			e.printStackTrace();
		}
		catch (InterruptedException e)
		{		
			e.printStackTrace();
		}
		return null;
	}
	
	public void setJobName(String jobName)
	{
		logger.debug("setJobName dans RunningJobClient " + this);
		this.jobName = jobName;
		countDownLatch.countDown();
	}


	@Override
	public float getMapProgress() throws IOException 
	{
		return 0;
	}

	@Override
	public float getReduceProgress() throws IOException
	{
		return 0;
	}

	@Override
	public Set<TaskCompletionEvent> getTaskCompletionEvents(int startFrom) throws IOException
	{
		return null;
	}

	@Override
	public boolean isComplete() throws IOException 
	{
		return false;
	}

	@Override
	public boolean isSuccessful() throws IOException 
	{
		return false;
	}

	@Override
	public void waitForCompletion() throws IOException 
	{
		
	}

	@Override
	public JobStatus getJobState() throws IOException 
	{
		try 
		{		
			logger.debug("debut getJobState dans RunningJobClient + envoi message " + this);
			
			RunningJobMessageClientToServer runningJobMessage = new RunningJobMessageClientToServer(jobID, "getJobFile", "setJobFile", null);
			Message message = session.createObjectMessage(runningJobMessage);
			queueSender.send(message);
			
			System.out.println("attente message retour getJobState dans RunningJobClient");
			
			countDownLatch = new CountDownLatch(1);
			countDownLatch.await();
			
			logger.debug("fin attente message retour getJobState dans RunningJobClient");
			
			return jobState;			
		} 
		catch (JMSException e) 
		{
			e.printStackTrace();
		}
		catch (InterruptedException e)
		{		
			e.printStackTrace();
		}
		return null;
	}
	
	public void setJobState(JobStatus jobState)
	{
		logger.debug("setJobState dans RunningJobClient " + this);
		this.jobState = jobState;
		countDownLatch.countDown();
	}
	
	public void setMapTaskReports(Set<TaskReport> mapTaskReports)
	{
		logger.debug("setMapTaskReports dans RunningJobClient " + this);
		this.mapTaskReports = mapTaskReports;
		countDownLatch.countDown();
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
		return null;
	}

	@Override
	public String getFailureInfo() throws IOException 
	{
		return null;
	}

	@Override
	public Set<String> getTaskDiagnostics(TaskID taskid) throws IOException 
	{	
		try 
		{	
			logger.debug("debut getTaskDiagnostics dans RunningJobClient + envoi message " + this);
			
			Object[] methodParameters = new Object[1];
			methodParameters[0] = taskid;
			RunningJobMessageClientToServer runningJobMessage = new RunningJobMessageClientToServer(jobID, "getTaskDiagnostics", "setTaskDiagnostics", methodParameters);
			Message message = session.createObjectMessage(runningJobMessage);
			queueSender.send(message);
			
			logger.debug("attente message retour getTaskDiagnostics dans RunningJobClient");
			
			countDownLatch = new CountDownLatch(1);
			countDownLatch.await();
			
			return taskDiagnostics;			
		} 
		catch (JMSException e) 
		{
			e.printStackTrace();
		}
		catch (InterruptedException e)
		{		
			e.printStackTrace();
		}
		return null;
	}
	
	public void setTaskDiagnostics(Set<String> taskDiagnostics)
	{
		this.taskDiagnostics = taskDiagnostics;
		countDownLatch.countDown();
	}

	@Override
	public Set<TaskReport> getMapTaskReports() throws IOException 
	{
		try 
		{	

			logger.debug("debut getMapTaskReports dans RunningJobClient + envoi message " + this);
			
			RunningJobMessageClientToServer runningJobMessage = new RunningJobMessageClientToServer(jobID, "getMapTaskReports", "setMapTaskReports", null);
			Message message = session.createObjectMessage(runningJobMessage);
			queueSender.send(message);
			
			logger.debug("attente message retour getMapTaskReports dans RunningJobClient");
			
			countDownLatch = new CountDownLatch(1);
			countDownLatch.await();
			
			return mapTaskReports;			
		} 
		catch (JMSException e) 
		{
			e.printStackTrace();
		}
		catch (InterruptedException e)
		{		
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public Set<TaskReport> getReduceTaskReports() 
	{
		try 
		{	
			logger.debug("debut getReduceTaskReports dans RunningJobClient + envoi message " + this);
			
			RunningJobMessageClientToServer runningJobMessage = new RunningJobMessageClientToServer(jobID, "getReduceTaskReports", "setReduceTaskReports", null);
			Message message = session.createObjectMessage(runningJobMessage);
			queueSender.send(message);
			
			logger.debug("attente message retour getReduceTaskReports dans RunningJobClient");
			
			countDownLatch = new CountDownLatch(1);
			countDownLatch.await();
			
			return reduceTaskReports;			
		} 
		catch (JMSException e) 
		{
			e.printStackTrace();
		}
		catch (InterruptedException e)
		{		
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public Set<TaskReport> getSplitTaskReports() 
	{
		try 
		{	
			logger.debug("debut getSplitTaskReports dans RunningJobClient + envoi message " + this);
			
			RunningJobMessageClientToServer runningJobMessage = new RunningJobMessageClientToServer(jobID, "getReduceTaskReports", "setReduceTaskReports", null);
			Message message = session.createObjectMessage(runningJobMessage);
			queueSender.send(message);
			
			logger.debug("attente message retour getSplitTaskReports dans RunningJobClient");
			
			countDownLatch = new CountDownLatch(1);
			countDownLatch.await();
			
			return splitTaskReports;			
		} 
		catch (JMSException e) 
		{
			e.printStackTrace();
		}
		catch (InterruptedException e)
		{		
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public long getStartTime() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long getLaunchTime() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long getFinishTime() {
		// TODO Auto-generated method stub
		return 0;
	}

}
