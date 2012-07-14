package com.runmycode.mapred.impl;

import java.io.IOException;
import java.net.URI;
import java.util.Calendar;

import org.apache.log4j.Logger;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import com.runmycode.mapred.JobConf;
import com.runmycode.mapred.JobID;
import com.runmycode.mapred.JobInProgress;
import com.runmycode.mapred.JobInProgressFactory;
import com.runmycode.mapred.JobStatus;
import com.runmycode.mapred.PropertiesUtil;
import com.runmycode.mapred.Reporter;
import com.runmycode.mapred.TIPStatus;
import com.runmycode.mapred.Task;
import com.runmycode.mapred.TaskID;
import com.runmycode.mapred.TaskInProgress;
import com.runmycode.mapred.TaskInProgressID;
import com.runmycode.mapred.TaskStatus;
import com.runmycode.mapred.mongoDB.MongoDBFactory;
import com.runmycode.mapred.server.ComputingReportationMessage;
import com.runmycode.ro.InstanceSet;


public abstract class Splitter 
{
	private transient Logger logger = Logger.getLogger(com.runmycode.mapred.impl.LocalSplitterTest.class);
	protected transient MongoOperations mongoOperations = MongoDBFactory.getMongoOperations();
	String fileSystemRoot;
	
	public Splitter(){
		ApplicationContext context = new ClassPathXmlApplicationContext("applicationContextLocalFileSystem.xml");
		PropertiesUtil propertiesUtil = (PropertiesUtil)context.getBean("applicationProperties");
		fileSystemRoot = propertiesUtil.getProperty("fileSystem.root");
	}
	public void runSplit(JobInProgressFactory jobInProgressFactory, JobID jobID) throws Exception 
	{
		logger.debug("Split starting ...");
		
		//TaskID taskid = setup(jobInProgressFactory, jobConf);
		mongoOperations.updateFirst(Query.query(Criteria.where("id").is(jobID.getId())), Update.update("jobStatus.runState", JobStatus.State.RUNNING), JobInProgress.class);
		
		//Dowloading input data		
		JobConf jobConf = mongoOperations.findById(jobID.getId(), JobConf.class);
		//instanceSet = jobConf.getInputParamaters();
		
		TaskInProgressID taskInProgressID = new TaskInProgressID(jobID, Double.toString(Math.random()).substring(2));
		TaskInProgress taskInProgress = new TaskInProgress(taskInProgressID, TIPStatus.Phase.SPLIT);
		taskInProgress.setJobInProgressFactory(jobInProgressFactory);
		Calendar calendar = Calendar.getInstance();
		taskInProgress.setExecStartTime(calendar.getTimeInMillis());
		
		TaskID taskid = new TaskID(jobID, jobID.getId()+Double.toString(Math.random()).substring(2));//TaskID.forName("idMembre_idJob_idSite_idTask");
		
		Task task = taskInProgress.addRunningTask(taskid);
		task.setPhase(TaskStatus.Phase.SPLIT);
		
		mongoOperations.insert(taskInProgress, "taskInProgress");
		mongoOperations.insert(task, "task");

		
		TaskStatus taskStatus = new TaskStatus();
		taskStatus.setRunState(TaskStatus.State.SUCCEEDED);
		taskStatus.setRunPhase(TaskStatus.Phase.SPLIT);
		ComputingReportationMessage reportMessage = new ComputingReportationMessage(taskInProgressID, taskid, taskStatus);
		
		String configPath = fileSystemRoot + "/" + jobID.getId() + "/Demandes/config.xml";
		String indexPath = fileSystemRoot + "/" + jobID.getId();
		//String configPath= jobID.getId() +"\\Demandes\\config.xml";
		//String indexPath = jobID.getId();
		
		InstanceSet instanceSet = jobConf.getInputParamaters();
		split(instanceSet, configPath, indexPath, jobID, reportMessage);
	}
	
	public abstract void split(InstanceSet instanceSet, String configPath, String indexPath, JobID jobID, Reporter reportMessage) throws Exception;
	
	public abstract void cleanup(JobID jobID) throws Exception;
}
