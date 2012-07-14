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
import java.text.ParseException;
import java.util.Collection;

/**
 * A set of named counters.
 * 
 * <p><code>Counters</code> represent global counters, defined either by the 
 * Map-Reduce framework or applications. Each <code>Counter</code> can be of
 * any {@link Enum} type.</p>
 * 
 * <p><code>Counters</code> are bunched into {@link Group}s, each comprising of
 * counters from a particular <code>Enum</code> class. 
 */
public class Counters implements Serializable
{	
	/**
   	 * Returns the names of all counter classes.
   	 * @return Set of counter names.
   	 */
	public synchronized Collection<String> getGroupNames() 
  	{
		return null;	 
  	}

	/**
	 * Find the counter for the given enum. The same enum will always return the
	 * same counter.
	 * @param key the counter key
	 * @return the matching counter object
	 */
	public synchronized Counter findCounter(Enum key) 
	{
		return null;	  
	}

	/**
	 * Find a counter given the group and the name.
	 * @param group the name of the group
	 * @param name the internal name of the counter
	 * @return the counter for that name
	 */
	public synchronized Counter findCounter(String group, String name) 
	{
		return null;
	}

	/**
	 * Increments the specified counter by the specified amount, creating it if
	 * it didn't already exist.
	 * @param key identifies a counter
	 * @param amount amount by which counter is to be incremented
	 */
	public synchronized void incrCounter(Enum key, long amount) 
	{
 
	}
  
	/**
	 * Increments the specified counter by the specified amount, creating it if
	 * it didn't already exist.
	 * @param group the name of the group
	 * @param counter the internal name of the counter
	 * @param amount amount by which counter is to be incremented
	 */
	public synchronized void incrCounter(String group, String counter, long amount) 
	{
		
	}
  
	/**
	 * Returns current value of the specified counter, or 0 if the counter
	 * does not exist.
	 */
	public synchronized long getCounter(Enum key) 
	{
		return 0;    
	}
  
	/**
	 * Increments multiple counters by their amounts in another Counters 
	 * instance.
	 * @param other the other Counters instance
	 */
	public synchronized void incrAllCounters(Counters other) 
	{
    
	}

	/**
	 * Convenience method for computing the sum of two sets of counters.
	 */
	public static Counters sum(Counters a, Counters b) 
	{
		return null;
    
	}
  
	/**
	 * Returns the total number of counters, by summing the number of counters
	 * in each group.
	 */
	public synchronized  int size() 
	{
		return 0;
    
	}
  
	/**
	 * return the short name of a counter/group name
	 * truncates from beginning.
	 * @param name the name of a group or counter
	 * @param limit the limit of characters
	 * @return the short name
	 */
	static String getShortName(String name, int limit) 
	{
		return null;
	}

	/**
	 * Convert a stringified counter representation into a counter object. Note 
	 * that the counter can be recovered if its stringified using 
	 * {@link #makeEscapedCompactString()}. 
	 * @return a Counter
	 */
	public static Counters fromEscapedCompactString(String compactString) throws ParseException 
	{
		return null;
	}

	/**
	 * Counter exception thrown when the number of counters exceed 
	 * the limit
	 */
	public static class CountersExceededException extends RuntimeException 
	{
		private static final long serialVersionUID = 1L;

		public CountersExceededException(String msg) 
		{
			super(msg);
		}
	}
}
