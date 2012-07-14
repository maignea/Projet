package com.runmycode.mapred.client;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicConnectionFactory;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;

import com.runmycode.mapred.AsynchronousRunningJob;
import com.runmycode.mapred.JobID;

public abstract class AbstractAsynchronousRunningJobClient implements AsynchronousRunningJob, MessageListener
{	
	private transient Logger logger = Logger.getLogger(com.runmycode.mapred.client.AbstractAsynchronousRunningJobClient.class);
	
	class ATask implements Callable<Integer>
	{
		private AsynchronousRunningJob runningJob;
		private Serializable serializable;
		
		public ATask(Serializable serializable, AsynchronousRunningJob runningJob)
		{
			this.serializable = serializable;
			this.runningJob = runningJob;
		}
		
		@Override
		public Integer call() throws Exception 
		{
			logger.debug("message recu : dans le thread");
				
			if(serializable instanceof AsynchronousRunningJobMessage)
			{			
				AsynchronousRunningJobMessage runningJobMessage = (AsynchronousRunningJobMessage)serializable;
				JobID jobID = runningJobMessage.getJobID();
				
				String methodName = runningJobMessage.getMethodName();
				Object[] methodParameters = runningJobMessage.getMethodParameters();
				Method[] methods = runningJob.getClass().getDeclaredMethods();
				int i = 0;
				while(i < methods.length && methods[i].getName().equals(methodName)== false)
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
			return null;
		}		
	}
	
	private ExecutorService executorService;
	
	private TopicConnectionFactory connectionFactory;
	private TopicConnection topicConnection;
	private Topic topic;
	private int numberOfThreads;
	
	
	@Autowired
	public void setConnectionFactory(TopicConnectionFactory connectionFactory) throws JMSException 
	{
		this.connectionFactory = connectionFactory;
		topicConnection = connectionFactory.createTopicConnection();
		
		if(this.topic != null)
		{
			TopicSession topicSession = topicConnection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
			
			TopicSubscriber topicSubscriber = topicSession.createSubscriber(topic);
	        topicSubscriber.setMessageListener(this);
	        System.out.println("start setConnectionFactory " + this);
	        topicConnection.start();
		}
	}

	@Autowired
	public void setTopic(Topic topic) throws JMSException 
	{
		this.topic = topic;
		if(topicConnection != null && topic != null)
		{
			topicConnection = connectionFactory.createTopicConnection();
			TopicSession topicSession = topicConnection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
			
			TopicSubscriber topicSubscriber = topicSession.createSubscriber(topic);
	        topicSubscriber.setMessageListener( this );
	        System.out.println("start setTopic " + this);
	        topicConnection.start();
		}
	}

	@Autowired
	public void setNumberOfThreads(int numberOfThreads) 
	{
		this.numberOfThreads = numberOfThreads;		
		this.executorService = Executors.newFixedThreadPool(numberOfThreads);
	}
	
    public void onMessage( Message message )
    {    
    	logger.debug("message recu");
        
		if (message instanceof ObjectMessage)
		{
			try 
			{			
				ObjectMessage objectMessage = (ObjectMessage)message;
				Serializable serializable = objectMessage.getObject();
				
				executorService.submit( new ATask(serializable, this) );			
			}
			catch (Exception e) 
			{
				e.printStackTrace();
			} 	
		}    
    }
}
