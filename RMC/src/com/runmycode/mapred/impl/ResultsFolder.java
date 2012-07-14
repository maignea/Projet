package com.runmycode.mapred.impl;

import java.io.File;
import java.io.Serializable;
import java.net.URI;

import org.springframework.data.annotation.Id;

import com.runmycode.mapred.JobID;

public class ResultsFolder extends File implements Serializable
{
	private JobID jobID;
	private URI uri;
	
	public ResultsFolder(){
		super("");
	}
	
	public ResultsFolder(JobID jobID, URI uri) 
	{
		super(uri);
		this.jobID = jobID;
		this.uri = uri;
	}

	public URI getUri() {
		return uri;
	}
	
	
	
	/*public ResultsFolder(JobID jobID, String path) 
	{
		super(path);
		this.jobID = jobID;
	}*/
}
