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

import java.io.IOException;


/** 
 * A base class for file-based {@link InputFormat}.
 * 
 * <p><code>FileInputFormat</code> is the base class for all file-based 
 * <code>InputFormat</code>s. This provides a generic implementation of
 * {@link #getSplits(JobConf, int)}.
 * Subclasses of <code>FileInputFormat</code> can also override the 
 * {@link #isSplitable(FileSystem, Path)} method to ensure input-files are
 * not split-up and are processed as a whole by {@link Mapper}s.
 */
public abstract class FileInputFormat<K, V> implements InputFormat<K, V> 
{ 
	public abstract RecordReader<K, V> getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException;

	/** Splits files returned by {@link #listStatus(JobConf)} when
	 * they're too big.
	 */ 
	@SuppressWarnings("deprecation")
	public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException
	{
		return null;
	}
  
	/**
	 * Sets the given comma separated paths as the list of inputs 
	 * for the map-reduce job.
	 * 
	 * @param conf Configuration of the job
	 * @param commaSeparatedPaths Comma separated paths to be set as 
	 *        the list of inputs for the map-reduce job.
	 */
	public static void setInputPaths(JobConf conf, String commaSeparatedPaths)
	{
		conf.setInputPath(commaSeparatedPaths);
	}  
}
