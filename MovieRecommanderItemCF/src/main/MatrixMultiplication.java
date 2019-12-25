import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/*
bench mark
makotoBot98
*/
//job4: perform matrix multiplication between cooccurance matrix and rating matrix built from previous jobs
/**
 *  data input format of job1: (user_id,movie_id,rating) --> this is in fact the rating matrix with each record to be a cell
 * 	with rowID = movie_id, columnID = user_id														
 * 
 *  job2 --> output cooccurance matrix based on the output of job1
 *  mapper1: generate cooccurance matrix cells
 *  mapper2: generate rating matrix cells
 * 	reducer: perform matrix cell multiplication between cooccurance and rating matrix
 */
public class MatrixMultiplication {
	public static class CooccurrenceMapper extends Mapper<LongWritable, Text, Text, Text> {

		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// input = output from job2, movieID1 \t movieID2 = sum_coOccuranceCount, movieID1 = columnID, movieID2 = rowID
			// output key: movieID1; note movieID1 = columnID of co-occurance matrix
			// output value: movieID2 = sum_coOccuranceCount; note movieID2 = rowID of co-occurance matrix

			String[] line = value.toString().trim().split("\t");
				//line[0] = movieID1, line[1] = "movieID2 = sum_coOccuranceCOunt"
			context.write(new Text(line[0]), new Text(line[1]));

		}
	}

	public static class RatingMapper extends Mapper<LongWritable, Text, Text, Text> {

		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// input: (user_id,movie_id,rating) from input data of mapper in job1
			// output key: movie_id; the rowID of rating matrix
			// output value: user_id : rating
			String[] line = value.toString().split(",");
			//line = {user_id, movie_id, rating}
			String userID = line[0];
			String movieID = line[1];
			String rating = line[2];
			context.write(new Text(userID), new Text(movieID + ":" + rating));
		}
	}

	public static class MultiplicationReducer extends Reducer<Text, Text, Text, DoubleWritable> {
		// reduce method
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			/**
			 * 
			 * data value format of input value from mapper1(coccurance mapper) = "="
			 * data value format of input value from mapper2(rating matrix mapper) = ":"
			 * 
			 * input key: movieID1(columnID in mapper1, rowID id mapper2)
			 * input value: Iterable{movieID2=coCount2, movieID3=coCount3, ...., user_id1:rating1, user_id2:rating2}
			 * output key: movieID2:user_id1, movieID3:user_id2
			 * output value: coCount*rating
			 * 
			 * datastructure: use two hashmap, map1 for coOccurance cells, and map2 for rating 
			 */
			Map<String, Double> coCountMap = new HashMap<>();
			Map<String, Double> ratingMap = new HashMap<>();
			while (values.iterator().hasNext()) {
				String val = values.iterator().next().toString();
				if (val.contains("=")) {
					//coOccurance pair
					String[] splits = val.split("=");
					String movieID = splits[0];
					double coCount = Double.parseDouble(splits[1]);
					coCountMap.put(movieID, coCount);
				} else if (val.contains(",")) {
					//rating pair
					String[] splits = val.split(",");
					String userID = splits[0];
					double rating = Double.parseDouble(splits[1]);
					ratingMap.put(userID, rating);
				}
			}

			//perform cell multiplication (every cooccurance pair and rating pair)
			//output the results for each multiplication to user_id:movieID
			//output key: user_id:movieID, note the movieID = rowID of cooccurance matrix
			//output value: coCount * rating
			for (Map.Entry<String, Double> coOccuranceEntry : coCountMap.entrySet()) {
				String movieID = coOccuranceEntry.getKey();
				double coCount = coOccuranceEntry.getValue();
				for (Map.Entry<String, Double> ratingEntry : ratingMap.entrySet()) {
					String userID = ratingEntry.getKey();
					double rating = ratingEntry.getValue();
					String outputKey = userID + ":" + movieID;
					double outputVal = coCount * rating;
					context.write(new Text(outputKey), new DoubleWritable(outputVal));
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);
		job.setJarByClass(MatrixMultiplication.class);

		ChainMapper.addMapper(job, CooccurrenceMapper.class, LongWritable.class, Text.class, Text.class, Text.class, conf);
		ChainMapper.addMapper(job, RatingMapper.class, Text.class, Text.class, Text.class, Text.class, conf);

		job.setMapperClass(CooccurrenceMapper.class);
		job.setMapperClass(RatingMapper.class);

		job.setReducerClass(MultiplicationReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CooccurrenceMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RatingMapper.class);

		TextOutputFormat.setOutputPath(job, new Path(args[2]));

		job.waitForCompletion(true);
	}
}
