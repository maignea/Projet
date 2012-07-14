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

/** A base class for {@link OutputFormat}. */
public abstract class FileOutputFormat<K, V> implements OutputFormat<K, V> 
{ 
	/**
	 * Set the {@link Path} of the output directory for the map-reduce job.
	 *
	 * @param conf The configuration of the job.
	 * @param outputDir the {@link Path} of the output directory for 
	 * the map-reduce job.
	 */
	public static void setOutputPath(JobConf conf, String outputDir) 
	{
		conf.setOutputPath(outputDir);   
	}
  
	/**
	 * Get the {@link Path} to the output directory for the map-reduce job.
	 * 
	 * @return the {@link Path} to the output directory for the map-reduce job.
	 * @see FileOutputFormat#getWorkOutputPath(JobConf)
	 */
	public static String getOutputPath(JobConf conf) 
	{
		return null;    
	}
}

