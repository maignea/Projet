/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.runmycode.mapred;

import java.io.Serializable;

import org.springframework.data.annotation.Id;
import com.runmycode.ro.InstanceSet;

/** 
 * A map/reduce job configuration.
 * 
 * <p><code>JobConf</code> is the primary interface for a user to describe a 
 * map-reduce job to the Hadoop framework for execution. The framework tries to
 * faithfully execute the job as-is described by <code>JobConf</code>, however:
 * <ol>
 *   <li>
 *   Some configuration parameters might have been marked as 
 *   <a href="{@docRoot}/org/apache/hadoop/conf/Configuration.html#FinalParams">
 *   final</a> by administrators and hence cannot be altered.
 *   </li>
 *   <li>
 *   While some job parameters are straight-forward to set 
 *   (e.g. {@link #setNumReduceTasks(int)}), some parameters interact subtly 
 *   rest of the framework and/or job-configuration and is relatively more 
 *   complex for the user to control finely (e.g. {@link #setNumMapTasks(int)}).
 *   </li>
 * </ol></p>
 * 
 * <p><code>JobConf</code> typically specifies the {@link Mapper}, combiner 
 * (if any), {@link Partitioner}, {@link Reducer}, {@link InputFormat} and 
 * {@link OutputFormat} implementations to be used etc.
 *
 * <p>Optionally <code>JobConf</code> is used to specify other advanced facets 
 * of the job such as <code>Comparator</code>s to be used, files to be put in  
 * the {@link DistributedCache}, whether or not intermediate and/or job outputs 
 * are to be compressed (and how), debugability via user-provided scripts 
 * ( {@link #setMapDebugScript(String)}/{@link #setReduceDebugScript(String)}),
 * for doing post-processing on task logs, task's stdout, stderr, syslog. 
 * and etc.</p>
 * 
 * <p>Here is an example on how to configure a job via <code>JobConf</code>:</p>
 * <p><blockquote><pre>
 *     // Create a new JobConf
 *     JobConf job = new JobConf(new Configuration(), MyJob.class);
 *     
 *     // Specify various job-specific parameters     
 *     job.setJobName("myjob");
 *     
 *     FileInputFormat.setInputPaths(job, new Path("in"));
 *     FileOutputFormat.setOutputPath(job, new Path("out"));
 *     
 *     job.setMapperClass(MyJob.MyMapper.class);
 *     job.setCombinerClass(MyJob.MyReducer.class);
 *     job.setReducerClass(MyJob.MyReducer.class);
 *     
 *     job.setInputFormat(SequenceFileInputFormat.class);
 *     job.setOutputFormat(SequenceFileOutputFormat.class);
 * </pre></blockquote></p>
 * 
 * @see JobClient
 * @see ClusterStatus
 * @see Tool
 * @see DistributedCache
 */
public class JobConf implements Serializable 
{
	private String jobName;
	private JobID jobID; 
	private JobPriority jobPriority = JobPriority.NORMAL;
	private String user;
	
	private Mapper mapperObject;
	private Reducer reducerObject;
	
	private InputFormat inputFormatObject;
	private OutputFormat outputFormatObject;
	
	private String inputPath;
	private String outputPath;
	
	// Id pour MongoBD
	@Id
	private String id;
	private InstanceSet instanceSet;
	private int numMapTasks;
	private int numReduceTasks;
		
	/**   
	 * Construct a map/reduce job configuration.
	 */  
	public JobConf() 
	{
  
	}
 
	/**
	 * Get the reported username for this job.
	 * 
	 * @return the username
	 */
	public String getUser()
	{
		return user;
	}

	public JobID getJobID() 
	{
		return jobID;
	}

	/**
	 * Set the reported username for this job.
	 * 
	 * @param user the username for this job.
	 */
	public void setUser(String user) 
	{
		if(user != null)
		{
			this.user = user;
		}
	}

	/**
	 * Get the {@link InputFormat} implementation for the map-reduce job,
	 * defaults to {@link TextInputFormat} if not specified explicity.
	 * 
	 * @return the {@link InputFormat} implementation for the map-reduce job.
	 * @throws IllegalAccessException 
	 * @throws InstantiationException 
	 */
	

	public Class<? extends InputFormat> getInputFormat()
	{
		return inputFormatObject.getClass();
	}

	/**
	 * Set the {@link InputFormat} implementation for the map-reduce job.
	 * 
	 * @param theClass the {@link InputFormat} implementation for the map-reduce 
	 *                 job.
	 */
	public void setInputFormat(Class<? extends InputFormat> inputFormatClass) throws Exception
	{
		inputFormatObject = inputFormatClass.newInstance();
	}

	/**
	 * Get the {@link OutputFormat} implementation for the map-reduce job,
	 * defaults to {@link TextOutputFormat} if not specified explicity.
	 * 
	 * @return the {@link OutputFormat} implementation for the map-reduce job.
	 */
	public Class<? extends OutputFormat> getOutputFormat()
	{	
		return outputFormatObject.getClass(); 
	}

	/**
	 * Set the {@link OutputFormat} implementation for the map-reduce job.
	 * 
	 * @param theClass the {@link OutputFormat} implementation for the map-reduce 
	 *                 job.
	 */
	public void setOutputFormat(Class<? extends OutputFormat> outputFormatClass) throws Exception
	{	  
		outputFormatObject = outputFormatClass.newInstance();
	}
	
	/**
	 * Get the {@link Mapper} class for the job.
	 * The default class is MainJobMap
	 * 
	 * @return the {@link Mapper} class for the job.
	 */  
	
	public Class<? extends Mapper> getMapperClass() 
	{	
		return mapperObject.getClass();    
	}
  
	/**
	 * Set the {@link Mapper} class for the job.
	 * The default class is MainJobMap
	 * 
	 * @param theClass the {@link Mapper} class for the job.
	 * @throws IllegalAccessException 
	 * @throws InstantiationException 
	 */
	public void setMapperClass(Class<? extends Mapper> mapperClass) throws InstantiationException, IllegalAccessException
	{
		mapperObject = mapperClass.newInstance();
	}

	/**
	 * Get the {@link Reducer} class for the job.
	 * 
	 * @return the {@link Reducer} class for the job.
	 */
	public Class<? extends Reducer> getReducerClass() 
	{	
		return reducerObject.getClass();
	}
  
	/**
	 * Set the {@link Reducer} class for the job.
	 * 
	 * @param theClass the {@link Reducer} class for the job.
	 * @throws IllegalAccessException 
	 * @throws InstantiationException 
	 */
	
	public void setReducerClass(Class<? extends Reducer> reducerClass) throws InstantiationException, IllegalAccessException 
	{
		reducerObject = reducerClass.newInstance();
	}

	/**
	 * Get configured the number of reduce tasks for this job.
	 * Defaults to <code>1</code>.
	 * 
	 * @return the number of reduce tasks for this job.
	 */
	public int getNumMapTasks() 
	{
		return numMapTasks;  
	}

	/**
	 * Get configured the number of reduce tasks for this job. Defaults to 
	 * <code>1</code>.
	 * 
	 * @return the number of reduce tasks for this job.
	 */
	public int getNumReduceTasks() 
	{
		return numReduceTasks;  
	}

	/**
	 * Get the user-specified job name. This is only used to identify the 
	 * job to the user.
	 * 
	 * @return the job's name, defaulting to "".
	 */
	
	public String getJobName()
	{	
		return jobName;    
	}

	/**
	 * Set the user-specified job name.
	 * 
	 * @param name the job's new name.
	 */
	public void setJobName(String name)
	{
		if(name != null)
		{
			jobName = name;
			this.jobID = new JobID(user + "_" +jobName + "_" + Double.toString(Math.random()).substring(2));
			this.id = jobID.getId();
		}
	}

	/**
	 * Set {@link JobPriority} for this job.
	 * 
	 * @param prio the {@link JobPriority} for this job.
	 */
	public void setJobPriority(JobPriority prio) 
	{
		jobPriority = prio;
	}
	
	/**
	 * Get the {@link JobPriority} for this job.
	 * 
	 * @return the {@link JobPriority} for this job.
	 */
	public JobPriority getJobPriority()
	{
		return jobPriority;
	}

	public String getInputPath() 
	{	
		return inputPath;
	}

	public String getOutputPath() 
	{
		return outputPath;	 
	}

	public void setInputPath(String inputPath) 
	{
		this.inputPath = inputPath;
	}

	public void setOutputPath(String outputPath) 
	{
		this.outputPath = outputPath;	 
	}

	public InstanceSet getInputParamaters() 
	{
		return this.instanceSet;
	}

	public void setInputParameters(InstanceSet instanceSet)
	{
		this.instanceSet = instanceSet;
		
	}
}