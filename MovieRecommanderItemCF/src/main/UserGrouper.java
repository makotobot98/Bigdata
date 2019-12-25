import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/*
bench mark
makotoBot98
*/
//job1: group all movie:rating pair by the same user
public class UserGrouper {
	public static class DataDividerMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

		// map method
		@Override
		public void map(LongWritable key, javax.xml.soap.Text value, Context context)
				throws IOException, InterruptedException {

			// value: user_id,movie_id,rating
			// output key: user_id
			// output value: movie_id : rating
			String[] splits = value.toString().trim().split(",");
			int userdID = Integer.parseInt(splits[0]);
			String movieID = splits[1];
			String rating = splits[2];
			context.write(new IntWritable(userID), new Text(movieID + ":" + rating));
		}
	}

	public static class DataDividerReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
		// reduce method
		@Override
		public void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			// key-value pairs: (user_id = 1, movie_id = 2 : rating = 3), (1, 4:5), (1, 3:6) ...
			// output key: user_id = 1
			// output value: movie_id1:rating1, movie_id2:rating2, .... --> 1:3,2:4,5:6
			StringBuilder output = new StringBuilder();
			while (values.iterator().hasNext()) {
				output.append("," + values.iterator().next());
			}
			//remove the trailing ","
			output.deleteCharAt(output.length() - 1);
			context.write(key, new Text(output.toString());
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);
		job.setMapperClass(DataDividerMapper.class);
		job.setReducerClass(DataDividerReducer.class);

		job.setJarByClass(UserGrouper.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		TextInputFormat.setInputPaths(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}

}
