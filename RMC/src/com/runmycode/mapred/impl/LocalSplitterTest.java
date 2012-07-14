package com.runmycode.mapred.impl;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.QueueConnection;
import javax.jms.QueueSender;
import javax.jms.QueueSession;
import javax.jms.Session;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.log4j.Logger;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
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
import com.runmycode.ro.Param;

public class LocalSplitterTest extends Splitter
{	
	
	Logger logger = Logger.getLogger(com.runmycode.mapred.impl.LocalSplitterTest.class);
	
	public void split(InstanceSet instanceSet, String configPath, String indexPath, JobID jobID, Reporter reportMessage) throws Exception
	{
		SplitterThread splitter = new SplitterThread(instanceSet, configPath, indexPath, reportMessage, jobID);
		splitter.start();
	}
	
	public void cleanup(JobID jobID) throws Exception 
	{	
		
		/*URI uri;
		try {
			uri = new URI(fileSystemRoot);
			logger.debug(uri.toString());
			File f = new File(uri);
			f.mkdir();
			
			uri = new URI(fileSystemRoot + "/" + jobID.getId() + "/index.txt");
			logger.debug(uri.toString());
			
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
		
		
		
		
		//Read output from IRODS
		//File indexFile = new File(jobID.getId()+"\\index.txt");
		URI uri = new URI(fileSystemRoot + "/" + jobID.getId()+ "/index.txt");
		File indexFile = new File(uri);
		
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
		reader.close();
	}
}
