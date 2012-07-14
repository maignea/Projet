package com.runmycode.mapred.impl;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Calendar;

import javax.jms.JMSException;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import com.runmycode.mapred.DataIndex;
import com.runmycode.mapred.InputFormat;
import com.runmycode.mapred.InputSplit;
import com.runmycode.mapred.JobConf;
import com.runmycode.mapred.JobID;
import com.runmycode.mapred.JobInProgressFactory;
import com.runmycode.mapred.Key;
import com.runmycode.mapred.Mapper;
import com.runmycode.mapred.RecordReader;
import com.runmycode.mapred.Reporter;
import com.runmycode.mapred.TIPStatus;
import com.runmycode.mapred.Task;
import com.runmycode.mapred.TaskID;
import com.runmycode.mapred.TaskInProgress;
import com.runmycode.mapred.TaskInProgressID;
import com.runmycode.mapred.TaskStatus;
import com.runmycode.mapred.mongoDB.MongoDBFactory;
import com.runmycode.mapred.server.ComputingReportationMessage;

public class RunMapper 
{
	private transient MongoOperations mongoOperations = MongoDBFactory.getMongoOperations();
	
	private Mapper mapper;
	private InputSplit[] splits;
	private InputFormat inputFormat;
	private JobConf jobConf;
	protected TaskInProgressID tipID;
	private TaskID taskid;

	private JobInProgressFactory jobInProgressFactory;
	
	public RunMapper(JobInProgressFactory jobInProgressFactory) throws JMSException 
	{
		this.jobInProgressFactory = jobInProgressFactory;
	}
	
	public void runMap(JobID jobID) throws Exception
	{
		setup(jobID);
		
		int numMapTasks = 0;
		for(InputSplit split : splits)
		{						
			Reporter reporter = null;
			RecordReader<Key, DataIndex> input = inputFormat.getRecordReader(split, jobConf, reporter);
		
			Key key = input.createKey();
			DataIndex value = input.createValue();
		
			while(input.next(key, value) == true)
			{
				numMapTasks++;
			}
		}
		
		mongoOperations.updateFirst(Query.query(Criteria.where("jobID._id").is(jobID.getId())), Update.update("numMapTasks", numMapTasks), JobConf.class);

		for(InputSplit split : splits)
		{			
			Reporter reporter = null;
			
			RecordReader<Key, DataIndex> input = inputFormat.getRecordReader(split, jobConf, reporter);
		
			Key key = input.createKey();
			DataIndex value = input.createValue();
			
			int nbFile = 10;
		
			while(input.next(key, value) == true)
			{
				//value = (DataIndex) value.clone();
				tipID = new TaskInProgressID(jobID, Double.toString(Math.random()).substring(2));
				TaskInProgress taskInProgress = new TaskInProgress(tipID, TIPStatus.Phase.MAP);
				taskInProgress.setJobInProgressFactory(jobInProgressFactory);
				Calendar calendar = Calendar.getInstance();
				taskInProgress.setExecStartTime(calendar.getTimeInMillis());
				
				taskid = new TaskID(jobID, jobID.getId()+Double.toString(Math.random()).substring(2));
				
				Task task = taskInProgress.addRunningTask(taskid);
				task.setPhase(TaskStatus.Phase.MAP);
				
				mongoOperations.insert(taskInProgress, "taskInProgress");
				mongoOperations.insert(task, "task");		
				
				TaskStatus taskStatus = new TaskStatus();
				taskStatus.setRunState(TaskStatus.State.COMMIT_PENDING);
				taskStatus.setRunPhase(TaskStatus.Phase.MAP);
				ComputingReportationMessage reportMessage = new ComputingReportationMessage(tipID, taskid, taskStatus);
				
				//output.open();
				
				//mapper.map(key, value, output, reportMessage);
				
				JobConf job = mongoOperations.findById(jobID.getId(), JobConf.class);
				String outputPath = job.getOutputPath(); 
				
				outputPath += "/MapResults";
				outputPath +="/mapResult_"+nbFile;
				
				//outputPath += "\\MapResults";
				//outputPath +="\\mapResult_"+nbFile;
				
				mapper.map(key, value, outputPath, reportMessage);
				
				nbFile++;
				//output.close();
				
				key = input.createKey();
				value = input.createValue();
			}
		}			
	}
	
	private void setup(JobID jobID) throws InstantiationException, IllegalAccessException, IOException, IllegalArgumentException, InvocationTargetException, SecurityException, NoSuchMethodException 
	{
		jobConf = mongoOperations.findById(jobID.getId(), JobConf.class, "jobConf");
		
		//Dowloading treatment
		Class mapperClass = jobConf.getMapperClass();
		this.mapper = (Mapper) mapperClass.newInstance();
		
		//Dowloading input data
		Class inputFormatClass = jobConf.getInputFormat();
		inputFormat = (InputFormat) inputFormatClass.newInstance();
		
		int numSplits = 0;
		splits = inputFormat.getSplits(jobConf, numSplits);
	}
	
	public void cleanup(JobID jobID) throws Exception 
	{
		//Read output from IRODS
		JobConf jobConf = mongoOperations.findById(jobID.getId(), JobConf.class, "jobConf");
		String outputPath = jobConf.getOutputPath();
		outputPath+="/MapResults";
		URI uri = new URI(outputPath);
		ResultsFolder mapResults = new ResultsFolder(jobID, uri);
		//ResultsFolder mapResults = new ResultsFolder(jobID, jobID.getId()+"\\Resultats\\MapResults");

		//Write output in MongoDB
		mongoOperations.insert(mapResults);
	}
}
