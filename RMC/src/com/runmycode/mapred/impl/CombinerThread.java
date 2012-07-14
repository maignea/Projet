package com.runmycode.mapred.impl;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.QueueConnection;
import javax.jms.QueueSender;
import javax.jms.QueueSession;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQQueue;

import com.runmycode.mapred.JobID;
import com.runmycode.mapred.Reporter;
import com.runmycode.mapred.TaskStatus;
import com.runmycode.ro.Param;
import com.runmycode.ro.Param.ParamDim;
import com.runmycode.ro.Param.ParamType;

public class CombinerThread extends Thread{
	
	private QueueSession computingReportationQueueSession;
	private QueueSender computingReportationQueueSender;
	
	JobID jobID;
	ResultsFolder mapResultsFolder;
	Reporter reporter;
	
	public CombinerThread(JobID jobID, ResultsFolder mapResultsFolder, Reporter reporter) throws JMSException {
	
		this.jobID = jobID;
		this.mapResultsFolder = mapResultsFolder;
		this.reporter = reporter;
		
		ActiveMQConnectionFactory activeMQConnectionFactory = new ActiveMQConnectionFactory();
		activeMQConnectionFactory.setBrokerURL("tcp://localhost:61616");
		QueueConnection connection = activeMQConnectionFactory.createQueueConnection();
		computingReportationQueueSession = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
		
		ActiveMQQueue activeMQQueue = new ActiveMQQueue("computingReportationQueue");
		
		computingReportationQueueSender = computingReportationQueueSession.createSender(activeMQQueue);
	}
	
	private void shuffle(JobID jobID, ResultsFolder mapResultsFolder) throws Exception
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

		String path = mapResultsFolder.getUri().toString();
		path = path.substring(0, path.indexOf("/Resultats"));
		path+="/reduceinput.txt";
		URI uri = new URI(path);
		BufferedWriter writer = new BufferedWriter(new FileWriter(new File(uri)));
		//BufferedWriter writer = new BufferedWriter(new FileWriter(jobID.getId()+"\\reduceinput.txt"));

		Set<Key> keySet = valuesForAKey.keySet();

		for(Key key : keySet)
		{
			writer.write("Key="+key.toString()+" Values=");

			List<Integer> listOfValue = valuesForAKey.get(key);
			
			for(Integer i : listOfValue)				
			{
				writer.write(i+" ");
			}			
			writer.newLine();
		}
		writer.close();
		
		reporter.getTaskStatus().setRunState(TaskStatus.State.SUCCEEDED);
	
		Message message = computingReportationQueueSession.createObjectMessage(reporter);
		computingReportationQueueSender.send(message);	
	}

	public void run()
	{
		
			try {
				shuffle(jobID, mapResultsFolder);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		
	}
}