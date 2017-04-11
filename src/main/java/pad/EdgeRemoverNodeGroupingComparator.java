/**
 *	@file NodeGroupingComparator.java
 *	@brief the reducer bundles together records with the same
 *		   NodeID while it is streaming the mapper output records from local disk.
 *  @author Devendra Dahiphale
 *  
 *	Copyright 2017 Devendra Dahiphale
 *	https://github.com/DevendraDahiphale
 * 
 *	Licensed under the Apache License, Version 2.0 (the "License"); 
 *	you may not use this file except in compliance with the License. 
 *	You may obtain a copy of the License at 
 * 
 *	http://www.apache.org/licenses/LICENSE-2.0 
 *  
 *	Unless required by applicable law or agreed to in writing, software 
 *	distributed under the License is distributed on an "AS IS" BASIS, 
 *	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
 *	See the License for the specific language governing permissions and 
 *	limitations under the License. 
 */

package pad;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Thanks to this class, the reducer bundles together records with the same
 * NodeID while it is streaming the mapper output records from local disk.
 */
public class EdgeRemoverNodeGroupingComparator extends WritableComparator
{
	/** Initializes a new instance of the NodeGroupingComparator class. */
	protected EdgeRemoverNodeGroupingComparator()
	{
		super( IntWritable.class, true );
	}
	
	/**
	* Compare two keys read from the mapper output records,
	* only looking  to the first component, i.e NodeID.
	* @param key1	first key.
	* @param key2	second key.
	* @return 		<c>0</c> if the NodeID is the same,
	* 				<c>-1</c> if key1 is smaller than key2
	* 				<c>1</c> if key1 is greater than key2.
	*/
	@SuppressWarnings("rawtypes")
	public int compare( WritableComparable key1, WritableComparable key2 )
	{
		IntWritable k1 = (IntWritable)key1;
		IntWritable k2 = (IntWritable)key2;
		
		return k1.get() - k2.get();
	}
}
