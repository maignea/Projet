package com.runmycode.mapred;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

import org.springframework.data.mongodb.core.MongoOperations;

import com.runmycode.mapred.mongoDB.MongoDBFactory;

public class JobInProgressFactory
{	
	private TreeMap<JobID, JobInProgress> jobs = new TreeMap<JobID, JobInProgress>();
	private List<JobID> pool = new ArrayList<JobID>();
	private int poolSize = 10;
	
	public JobInProgress getJobInProgress(JobID jobID) throws Exception
	{
		if(jobID == null)
		{
			throw new Exception("jodID should not be null");
		}
		JobInProgress job;
		if(pool.contains(jobID))
		{
			job = jobs.get(jobID);
			return job;
		}
		else if(pool.size() == poolSize)
		{
			pool.remove(0);
			jobs.remove(0);
		}
		MongoOperations mongoOperations = MongoDBFactory.getMongoOperations();
		job = mongoOperations.findById(jobID.getId(), JobInProgress.class);
		if(job == null)
		{
			job = new JobInProgress(jobID);
			job.setJobInProgressFactory(this);
		}
		pool.add(jobID);
		jobs.put(jobID, job);
		return job;
	}
}
