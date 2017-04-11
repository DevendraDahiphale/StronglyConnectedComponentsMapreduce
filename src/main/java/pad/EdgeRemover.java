/**
 *	@file EdgeRemover.java
 *	@brief Input handler job for removing an edge and regenerating new edge list
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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;

/**	Driver of the Job responsible for executing the Small-Star or Large-Star operation on the input graph. */
public class EdgeRemover extends Configured implements Tool
{
	/** The StarDriver can be of type Large-StarDriver or Small-StarDriver */
//	public enum StarDriverType { LARGE, SMALL };
	
	private final String title;
//	private final StarDriverType type;
	private final Path input, output;
	private final boolean verbose;
	private final long edgeNumber;
//	private long numChanges;
	
	/**
	* Initializes a new instance of the StarDriver class.
	* @param type		identify which kind of job execute: Small-Star or Large-Star.
	* @param input		path of the result folder of \see InitializationDriver or \see StarDriver Job.
	* @param output		path of the output folder.
	* @param iteration	used to build the title of this Job.
	* @param verbose	if <c>true</c> shows on screen the messages of the Job execution.
	*/
	public EdgeRemover( Path input, Path output, long edgeNumber, boolean verbose )
	{
	//	this.type = type;
		this.title = "EdgeRemover_"  + Long.toString(edgeNumber);
		this.input = input;
		this.output = output;
		this.edgeNumber = edgeNumber;
		this.verbose = verbose;
	}
	
	/**
	 * Execute the StarDriver Job.
	 * @param args		array of external arguments, not used in this method
	 * @return 			<c>1</c> if the StarDriver Job failed its execution; <c>0</c> if everything is ok. 
	 * @throws Exception 
	 */
	public int run( String[] args ) throws Exception
	{
		Configuration conf = new Configuration();
		// GenericOptionsParser invocation in order to suppress the hadoop warning.
		new GenericOptionsParser( conf, args );
		//conf.set( "type", this.type.toString() );
		conf.set("edgeID", Long.toString(this.edgeNumber));
		Job job = new Job( conf, this.title );
		job.setJarByClass( EdgeRemover.class );
	
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass( IntWritable.class );
		job.setOutputKeyClass( IntWritable.class );
		job.setOutputValueClass( IntWritable.class );
		
	
		job.setMapperClass(EdgeRemoverMapper.class );
//		job.setNumReduceTasks(0);
		job.setCombinerClass( EdgeRemoverCombiner.class );
	//	job.setPartitionerClass( NodePartitioner.class );
		job.setGroupingComparatorClass( EdgeRemoverNodeGroupingComparator.class );
		job.setReducerClass( EdgeRemoverReducer.class );
	
		job.setInputFormatClass( SequenceFileInputFormat.class );
		job.setOutputFormatClass( SequenceFileOutputFormat.class );
	
		FileInputFormat.addInputPath( job, this.input );
		FileOutputFormat.setOutputPath( job, this.output );

		if ( !job.waitForCompletion( verbose ) )
			return 1;
		
		// Set up the private variable looking to the counter value
	//	this.numChanges = job.getCounters().findCounter( UtilCounters.NUM_CHANGES ).getValue();
		System.out.println("edge counter " + job.getCounters().findCounter( UtilCounters.NUM_EDGE_COUNTER ).getValue());
		return 0;
	}
	
	/**
	 * Return the number of changes occurred during the operation Small-Star or Large-Star.
	 * @return 	number of changes.
	 */
	//public long getNumChanges()
//	{
		//return this.numChanges;
//	}
	
	/**
	 * Main of the \see StarDriver class.
	 * @param args	array of external arguments,
	 * @throws Exception
	 */
	public static void main( String[] args ) throws Exception 
	{	
		if ( args.length != 3 )
		{
			System.out.println( "Usage: StarDriver <type> <input> <output>" );
			System.exit(1);
		}
		
		// Check what Job we need to execute: Small-Star or Large-Star
	///	StarDriverType type = args[0].toLowerCase().equals("small") ? StarDriverType.SMALL : StarDriverType.LARGE;
	//	String name = ( type == StarDriverType.SMALL ) ? "Small" : "Large";
		
		// Execute the Small-Star or Large-Star Job
		Path input = new Path( args[1] );
		Path output = new Path( args[2] );
		System.out.println( "removing 0 edge" );
		EdgeRemover star = new EdgeRemover( input, output, Integer.parseInt(args[0]), true );
		if ( star.run( null ) != 0 )
		{
			FileSystem.get( new Configuration() ).delete( output, true  );
			System.exit( 1 );
		}
		System.out.println( "remved 0 edge");
		
		System.exit( 0 );
	}
}
