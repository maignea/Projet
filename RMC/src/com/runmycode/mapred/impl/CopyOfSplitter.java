package com.runmycode.mapred.impl;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Calendar;

import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import com.runmycode.mapred.DataIndex;
import com.runmycode.mapred.JobConf;
import com.runmycode.mapred.JobID;
import com.runmycode.mapred.JobInProgress;
import com.runmycode.mapred.JobInProgressFactory;
import com.runmycode.mapred.JobStatus;
import com.runmycode.mapred.Reporter;
import com.runmycode.mapred.TIPStatus;
import com.runmycode.mapred.Task;
import com.runmycode.mapred.TaskID;
import com.runmycode.mapred.TaskInProgress;
import com.runmycode.mapred.TaskInProgressID;
import com.runmycode.mapred.TaskStatus;
import com.runmycode.mapred.mongoDB.MongoDBFactory;
import com.runmycode.ro.InstanceSet;

public abstract class CopyOfSplitter 
{
	private transient MongoOperations mongoOperations = MongoDBFactory.getMongoOperations();
	protected InstanceSet instanceSet;
	protected TaskID taskid;
	protected TaskInProgressID taskInProgressID;
	private JobInProgressFactory jobInProgressFactory;
	
	public CopyOfSplitter(JobInProgressFactory jobInProgressFactory)
	{
		this.jobInProgressFactory = jobInProgressFactory;
	}
	
	/*public void runSplit(JobID jobID) throws Exception
	{
		setup(jobID);
		
		split(jobID);
	}*/

	
	
	//sending the treatment to a worker
	//protected abstract void split(JobID jobID) throws Exception;
	protected abstract void split(String configPath, String indexPath, JobID jobID, Reporter reportMessage) throws Exception;
	
	//public abstract void cleanup(JobID jobID) throws IOException, URISyntaxException;
	
	public void cleanup(JobID jobID) throws IOException, URISyntaxException 
	{	
		//Read output from IRODS
		File indexFile = new File(jobID.getId()+"\\index.txt");
		
		//Write output in MongoDB
		BufferedReader reader = new BufferedReader(new FileReader(indexFile));
		String line;
		while((line = reader.readLine()) != null)
		{
			String[] parts = line.split(" ");
			DataIndex index = new DataIndex(jobID);
			index.setMap(true);
			index.setOffset(parts[0].substring(parts[0].indexOf("=")+1));
			index.setRow(Integer.parseInt(parts[1].substring(parts[1].indexOf("=")+1)));
			index.setColumn(Integer.parseInt(parts[2].substring(parts[2].indexOf("=")+1)));
			index.setLength(Integer.parseInt(parts[3].substring(parts[3].indexOf("=")+1)));
			mongoOperations.insert(index);
		}
	}
}
