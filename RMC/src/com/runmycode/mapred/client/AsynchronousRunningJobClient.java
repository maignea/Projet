package com.runmycode.mapred.client;

import org.apache.log4j.Logger;

import com.runmycode.mapred.JobID;

public class AsynchronousRunningJobClient extends AbstractAsynchronousRunningJobClient
{
	private transient Logger logger = Logger.getLogger(com.runmycode.mapred.client.AsynchronousRunningJobClient.class);
	
	@Override
	public void setMapProgress(float progress, JobID jobID) 
	{
		logger.debug("message recu dans setMapProgress de AsynchronousRunningJobClient");
	}

	public AsynchronousRunningJobClient()
	{
		logger.debug(super.hashCode());
	}
}
