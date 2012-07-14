package com.runmycode.mapred.impl;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;


import org.apache.log4j.Logger;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import com.runmycode.mapred.DataIndex;
import com.runmycode.mapred.JobConf;
import com.runmycode.mapred.JobID;
import com.runmycode.mapred.JobInProgressFactory;
import com.runmycode.mapred.RecordReader;
import com.runmycode.mapred.Reducer;
import com.runmycode.mapred.Reporter;
import com.runmycode.mapred.TIPStatus;
import com.runmycode.mapred.Task;
import com.runmycode.mapred.TaskID;
import com.runmycode.mapred.TaskInProgress;
import com.runmycode.mapred.TaskInProgressID;
import com.runmycode.mapred.TaskStatus;
import com.runmycode.mapred.mongoDB.MongoDBFactory;
import com.runmycode.mapred.server.ComputingReportationMessage;
import com.runmycode.ro.Param;
import com.runmycode.ro.Param.ParamDim;
import com.runmycode.ro.Param.ParamType;


public abstract class Combiner
{
	protected transient MongoOperations mongoOperations = MongoDBFactory.getMongoOperations();
	private transient Logger logger = Logger.getLogger(com.runmycode.mapred.impl.Combiner.class);
	

	public void runCombiner(JobInProgressFactory jobInProgressFactory, JobID jobID) throws Exception
	{
		JobConf jobConf = mongoOperations.findById(jobID.getId(), JobConf.class);
		
		//Dowloading input data
		ResultsFolder mapResultsFolder = mongoOperations.findOne(Query.query(Criteria.where("jobID._id").is(jobID.getId())), ResultsFolder.class);		

		//shuffle(jobID, mapResultsFolder);
		
		TaskInProgressID tipID = new TaskInProgressID(jobID, Double.toString(Math.random()).substring(2));
		TaskInProgress taskInProgress = new TaskInProgress(tipID, TIPStatus.Phase.COMBINE);
		taskInProgress.setJobInProgressFactory(jobInProgressFactory);
		Calendar calendar = Calendar.getInstance();
		taskInProgress.setExecStartTime(calendar.getTimeInMillis());

		TaskID taskid = new TaskID(jobID, jobID.getId()+Double.toString(Math.random()).substring(2));

		
		Task task = taskInProgress.addRunningTask(taskid);
		task.setPhase(TaskStatus.Phase.COMBINE);
		
		mongoOperations.insert(taskInProgress, "taskInProgress");
		mongoOperations.insert(task, "task");
		
		TaskStatus taskStatus = new TaskStatus();
		taskStatus.setRunState(TaskStatus.State.COMMIT_PENDING);
		taskStatus.setRunPhase(TaskStatus.Phase.COMBINE);
		ComputingReportationMessage reportMessage = new ComputingReportationMessage(tipID, taskid, taskStatus);
		
		combine(jobID, mapResultsFolder, reportMessage);
		
	}

	public abstract void combine(JobID jobID, ResultsFolder mapResultsFolder, Reporter reportMessage) throws Exception;
	
	public abstract void cleanup(JobID jobID) throws Exception;
	
	
/*	private void shuffle(JobID jobID, ResultsFolder mapResultsFolder) throws NumberFormatException, IOException
	{
		File[] files = mapResultsFolder.listFiles();

		Map<Key, List<Integer>> valuesForAKey = new HashMap<Key, List<Integer>>();

		for(File aFile : files)
		{
			BufferedReader reader = new BufferedReader(new FileReader(aFile));

			String line;
			while((line = reader.readLine()) != null)
			{
				String keyString = line.substring(line.indexOf("Key=")+4, line.indexOf(" "));
				Param param = new Param("result", ParamDim.SCALAR, ParamType.INTEGER, keyString, null);
				Key key = new Key();
				key.setParam(param);
				Integer e = Integer.parseInt(line.substring(line.indexOf("Value=")+6));

				if(valuesForAKey.containsKey(key) == true)
				{
					valuesForAKey.get(key).add(e);
				}
				else
				{		
					List<Integer> list = new ArrayList<Integer>();
					list.add(e);
					valuesForAKey.put(key, list);
				}
			}
			reader.close();
		}

		BufferedWriter writer = new BufferedWriter(new FileWriter(jobID.getId()+"\\reduceinput.txt"));

		Set<Key> keySet = valuesForAKey.keySet();

		DataIndex dataIndex = null;
		int nbLine = 0;
		for(Key key : keySet)
		{
			writer.write("Key="+key.toString()+" Values= ");

			List<Integer> listOfValue = valuesForAKey.get(key);

			int nbValue = 0;
			for(Integer i : listOfValue)				
			{
				dataIndex = new DataIndex(jobID);
				dataIndex.setMap(false);
				dataIndex.setOffset(jobID.getId()+"\\reduceinput.txt");

				writer.write(i+" ");

				dataIndex.setLength(String.valueOf(i).length());				
				dataIndex.setColumn(nbValue);

				nbValue++;

				dataIndex.setRow(nbLine);

				mongoOperations.insert(dataIndex);
			}			
			writer.newLine();
			nbLine++;
		}
		writer.close();
	}*/
}
