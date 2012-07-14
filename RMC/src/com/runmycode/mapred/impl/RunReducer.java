package com.runmycode.mapred.impl;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;


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


public class RunReducer
{
	private transient MongoOperations mongoOperations = MongoDBFactory.getMongoOperations();

	private Reducer reducer;
	private RecordReader input;
	
	private TaskInProgressID tipID;
	private TaskID taskid;

	private JobInProgressFactory jobInProgressFactory;
	
	public RunReducer(JobInProgressFactory jobInProgressFactory)
	{
		this.jobInProgressFactory = jobInProgressFactory;
	}

	public void runReduce(JobID jobID) throws Exception
	{
		setup(jobID);

		Map<Integer, List<DataIndex>> dataIndexTable = getDataIndexes(jobID);
		Set<Integer> keys = dataIndexTable.keySet();
		Iterator<Integer> it = keys.iterator();

		List<DataIndex> dataIndexes = new ArrayList<DataIndex>();

		//ReducerCollector output = new ReducerCollector(jobID);

		int numReduceTasks = 0;

		while(it.hasNext())
		{		
			it.next();
			numReduceTasks++;
		}

		mongoOperations.updateFirst(Query.query(Criteria.where("jobID._id").is(jobID.getId())), Update.update("numReduceTasks", numReduceTasks), JobConf.class);

		it = keys.iterator();

		int nbFile = 0;
		
		while(it.hasNext())
		{
			Integer key = it.next();

			dataIndexes = dataIndexTable.get(key);

			Iterator<DataIndex> values = dataIndexes.iterator();

			tipID = new TaskInProgressID(jobID, Double.toString(Math.random()).substring(2));
			TaskInProgress taskInProgress = new TaskInProgress(tipID, TIPStatus.Phase.REDUCE);
			taskInProgress.setJobInProgressFactory(jobInProgressFactory);
			Calendar calendar = Calendar.getInstance();
			taskInProgress.setExecStartTime(calendar.getTimeInMillis());

			taskid = new TaskID(jobID, jobID.getId()+Double.toString(Math.random()).substring(2));

			
			Task task = taskInProgress.addRunningTask(taskid);
			task.setPhase(TaskStatus.Phase.REDUCE);
			
			mongoOperations.insert(taskInProgress, "taskInProgress");
			mongoOperations.insert(task, "task");
			
			TaskStatus taskStatus = new TaskStatus();
			taskStatus.setRunState(TaskStatus.State.COMMIT_PENDING);
			taskStatus.setRunPhase(TaskStatus.Phase.REDUCE);
			ComputingReportationMessage reportMessage = new ComputingReportationMessage(tipID, taskid, taskStatus);
			
			//output.open();
			
			//reducer.reduce(key, values, output, reportMessage);
			
			JobConf job = mongoOperations.findById(jobID.getId(), JobConf.class);
			String outputPath = job.getOutputPath(); 
			
			outputPath += "/ReduceResults";
			outputPath +="/reduceResult_"+nbFile;
			
			//outputPath += "\\ReduceResults";
			//outputPath +="\\reduceResult_"+nbFile;
			
			reducer.reduce(key, values, outputPath, reportMessage);
			
			nbFile++;
			
			//output.close();
		}
	}

	private void setup(JobID jobID) throws Exception 
	{
		JobConf jobConf = mongoOperations.findById(jobID.getId(), JobConf.class);

		//Dowloading treatment
		Class reducerClass = jobConf.getReducerClass();

		this.reducer = (Reducer)reducerClass.newInstance();
		
		//Dowloading input data
		ResultsFolder mapResultsFolder = mongoOperations.findOne(Query.query(Criteria.where("jobID._id").is(jobID.getId())), ResultsFolder.class);		

		//shuffle(jobID, mapResultsFolder);
		
		/*TaskInProgressID tipID = new TaskInProgressID(jobID, Double.toString(Math.random()).substring(2));
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
		ComputingReportationMessage reportMessage = new ComputingReportationMessage(tipID, taskid, taskStatus);
		
		
		CombinerThread combinerThread = new CombinerThread(jobID, mapResultsFolder, reportMessage);
		combinerThread.start();*/
			
	}

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

	public void cleanup(JobID jobID) throws Exception 
	{
		JobConf jobConf = mongoOperations.findById(jobID.getId(), JobConf.class, "jobConf");
		String outputPath = jobConf.getOutputPath();
		outputPath+="/ReduceResults";
		URI uri = new URI(outputPath);
		ResultsFolder reduceResults = new ResultsFolder(jobID, uri);
		
		
		//Read output from IRODS
		//ResultsFolder reduceResults = new ResultsFolder(jobID, jobID.getId()+"\\Resultats\\ReduceResults");
		//Write output in MongoDB
		mongoOperations.insert(reduceResults);
	}

	public Map<Integer, List<DataIndex>> getDataIndexes(JobID jobID) throws IOException 
	{
		List<DataIndex> dataIndexes = mongoOperations.find(Query.query(Criteria.where("jobID._id").is(jobID.getId()).and("isMap").is(false)), DataIndex.class);

		Map<Integer, List<DataIndex>> dataIndexTable = new HashMap<Integer, List<DataIndex>>();

		for(DataIndex dataIndex : dataIndexes)
		{
			if(dataIndexTable.isEmpty() == true)
			{
				List<DataIndex> indexes = mongoOperations.find(Query.query(Criteria.where("jobID._id").is(jobID.getId()).and("row").is(dataIndex.getRow()).and("isMap").is(false)), DataIndex.class);

				dataIndexTable.put(dataIndex.getRow(), indexes);
			}
			else
			{
				if(dataIndexTable.containsKey((dataIndex.getRow())) == false)
				{
					List<DataIndex> indexes = mongoOperations.find(Query.query(Criteria.where("jobID._id").is(jobID.getId()).and("row").is(dataIndex.getRow()).and("isMap").is(false)), DataIndex.class);

					dataIndexTable.put(dataIndex.getRow(), indexes);
				}
			}
		}
		return dataIndexTable;
	}
}
