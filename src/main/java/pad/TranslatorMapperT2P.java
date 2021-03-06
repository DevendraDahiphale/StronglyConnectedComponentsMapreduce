/**
 *	@file TranslatorMapperT2P.java
 *	@brief Mapper of \see TranslatorDriver.
 *  @author Federico Conte (draxent)
 *  
 *	Copyright 2015 Federico Conte
 *	https://github.com/Draxent/ConnectedComponents
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

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/** Mapper of \see TranslatorDriver. */
public class TranslatorMapperT2P extends Mapper<LongWritable, Text, IntWritable, IntWritable> 
{
	private IntWritable nodeID = new IntWritable();
	private IntWritable neighbourID = new IntWritable();
	
	/**
	* Map method of the this TranslatorMapperT2P class.
	* Extract the <nodeID, NeighbourID> from the line and emit it.
	* @param _			offset of the line read, not used in this method.
	* @param value		text of the line read.
	* @param context	context of this Job.
	* @throws IOException, InterruptedException
	*/
	public void map( LongWritable _, Text value, Context context ) throws IOException, InterruptedException
	{
		// Read line.
		String line = value.toString();
		
		// Split the line on the tab character.
		String userID_neighbourID[] = line.split( "\t" );
		
		// Extract the nodeID and neighbourID.
		nodeID.set( Integer.parseInt( userID_neighbourID[0] ) );
		neighbourID.set( Integer.parseInt( userID_neighbourID[1] ) );
		
		// Emit the pair
		context.write( nodeID, neighbourID );
	}
}
