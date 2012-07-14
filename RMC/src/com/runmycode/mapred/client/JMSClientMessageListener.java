package com.runmycode.mapred.client;

import java.lang.reflect.Method;

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

import com.runmycode.mapred.JobID;
import com.runmycode.mapred.RunningJob;
import com.runmycode.mapred.server.RunningJobServer;

public class JMSClientMessageListener implements MessageListener
{
	private transient Logger logger = Logger.getLogger(com.runmycode.mapred.client.JMSClientMessageListener.class);
	
	private JobClient_ClientSide jobClient;
	
	private JobID jobID;
	private Queue queue;
	private QueueConnection connection;
	private QueueSession session;
	private QueueSender queueSender;
	private QueueReceiver queueReceiver;
	
	public void setJobID(JobID jobID)
	{
		this.jobID = jobID;
	}

	public void setQueue(Queue queue) throws JMSException
	{
		if(session != null)
		{
			queueSender = session.createSender(queue);
			queueReceiver = session.createReceiver(queue);
			queueReceiver.setMessageListener(this);
			connection.start();
		}
		this.queue = queue;
	}

	public void setConnectionFactory(QueueConnectionFactory connectionFactory) throws JMSException 
	{
		connection = connectionFactory.createQueueConnection();
		session = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
		if(queue != null)
		{
			queueSender = session.createSender(queue);
			queueReceiver = session.createReceiver(queue);
			queueReceiver.setMessageListener(this);
			connection.start();
		}
	}
	
    public void setJobClient(JobClient_ClientSide jobClient)
    {
		this.jobClient = jobClient;
	}

	public void onMessage( Message message )
	{ 	
		logger.debug("on Message : " + message);
		
    	if (message instanceof ObjectMessage)
    	{
    		try 
    		{	
    			ObjectMessage objectMessage = (ObjectMessage)message;
    			RunningJobMessageServerToClient runningJobMessage = (RunningJobMessageServerToClient)objectMessage.getObject();
    			
    			logger.debug("on Message : " + runningJobMessage);
    		
    			RunningJob runningJob = new RunningJobServer(null);
				Class runningJobClass = runningJob.getClass();
				
				String methodName = runningJobMessage.getMethodName();
				Object[] methodParameters = runningJobMessage.getMethodParameters();
				Method[] methods = runningJobClass.getDeclaredMethods();
				int i = 0;
				while(i < methods.length && methods[i].getName().equals(methodName)==false){
					i++;
				}
				Method method;
				if(i < methods.length)
				{
					method = methods[i];
					method.invoke(runningJob, methodParameters);
				}
			} 
    		catch (Exception e) 
    		{
				e.printStackTrace();
			}     		    		
    	}
    }    
}
