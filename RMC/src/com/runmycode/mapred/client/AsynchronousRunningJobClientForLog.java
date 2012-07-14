package com.runmycode.mapred.client;

import javax.jms.JMSException;
import javax.jms.Topic;
import javax.jms.TopicConnectionFactory;

import org.apache.log4j.Logger;

import com.runmycode.mapred.JobID;

public class AsynchronousRunningJobClientForLog extends AbstractAsynchronousRunningJobClient
{
	private transient Logger logger = Logger.getLogger(com.runmycode.mapred.client.AsynchronousRunningJobClientForLog.class);
	
	@Override
	public void setMapProgress(float progress, JobID jobID) 
	{
		logger.debug("message recu dans setMapProgress de AsynchronousRunningJobClientForLog");
	}
	
	public AsynchronousRunningJobClientForLog()
	{
		logger.debug(super.hashCode());
	}
}
