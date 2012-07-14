package com.runmycode.mapred.impl;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.runmycode.mapred.DataIndex;
import com.runmycode.mapred.JobID;
import com.runmycode.mapred.PropertiesUtil;
import com.runmycode.mapred.Reporter;

public class LocalCombinerTest extends Combiner
{
	@Override
	public void combine(JobID jobID, ResultsFolder mapResultsFolder, Reporter reportMessage) throws Exception
	{
		CombinerThread combinerThread = new CombinerThread(jobID, mapResultsFolder, reportMessage);
		combinerThread.start();
		
	}

	@Override
	public void cleanup(JobID jobID) throws Exception
	{
		ApplicationContext context = new ClassPathXmlApplicationContext("applicationContextLocalFileSystem.xml");
		PropertiesUtil propertiesUtil = (PropertiesUtil)context.getBean("applicationProperties");
		String fileSystemRoot = propertiesUtil.getProperty("fileSystem.root");
		URI uri = new URI(fileSystemRoot + "/" + jobID.getId()+ "/reduceinput.txt");
		
		System.out.println("----------------------------" + uri.toString());
		
		File indexFile = new File(uri);
		
		
		//Read output from IRODS
		//File indexFile = new File(jobID.getId()+"\\reduceinput.txt");
				
		//Write output in MongoDB
		BufferedReader reader = new BufferedReader(new FileReader(indexFile));
		String line;
		DataIndex dataIndex = null;
		int nbLine = 0;
		while((line = reader.readLine()) != null)
		{
			int nbValue = 0;
			String values = line.substring(line.indexOf("Values=")+("Values=").length());
			String[] listOfValue = values.split(" ");
			for(String s : listOfValue)				
			{
				dataIndex = new DataIndex(jobID);
				dataIndex.setMap(false);
				
				//System.out.println("--------------------" + fileSystemRoot + "/" + jobID.getId()+ "/reduceinput.txt");
				dataIndex.setOffset(fileSystemRoot + "/" + jobID.getId()+ "/reduceinput.txt");
				
				//dataIndex.setOffset(jobID.getId()+"\\reduceinput.txt");
				dataIndex.setLength(s.length());				
				dataIndex.setColumn(nbValue);
				dataIndex.setRow(nbLine);
				nbValue++;
				mongoOperations.insert(dataIndex);
			}			
			nbLine++;
		}
		reader.close();
	}
}
