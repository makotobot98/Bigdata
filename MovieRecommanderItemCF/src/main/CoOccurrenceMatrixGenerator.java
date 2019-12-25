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
//job2, generate the co-occurance matrix given input from job1 with the adjacency list of user(row), movie(column), rating(cell)
public class CoOccurrenceMatrixGenerator {
	public static class MatrixGeneratorMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		// map method
		@Override

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// input of job2 comes from reducer of job1
			// value = userID1 \t movie1: rating, movie2: rating...
			// output: we are essentially performing a cartesion product (rows be movieID,
			// cols be ratings), and outputing all the (movieID1, movieID2) pairs as key,
			// value be 1 to count up in the reducer.Thus use two for loop to loop all cross
			// product between all movie pairs, rated by the same userID

			String[] splits = value.toString().trim().split("\t"); // splits[0] = userID, splits[1] = "movie1:rating1,
																															// movie2:rating2, ..."
			if (splits.length < 2) {
				return;
			}

			String[] movieRatingPairs = splits[1].trim().split(",");

			// generating cross product
			for (int i = 0; i < movieRatingPairs.length; i++) {
				String movieID1 = movieRatingPairs[i].trim().split(":")[0]; // ["movieID1", "rating1"]
				for (int j = 0; j < movieRatingPairs.length; j++) {
					String movieID2 = movieRatingPairs[j].trim().split(":")[0];
					context.write(new Text(movieID1 + ":" + movieID2, new IntWritable(1)));
				}
			}

		}
	}

	public static class MatrixGeneratorReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		// reduce method
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			// key movie1:movie2
			// value = iterable<1, 1, 1>
			// output key: movie1:movie2
			// output value: sum(1's) = count of total co-occurance of movie1 and movie2
			// rated by same user
			int sum = 0;
			while (values.iterator().hasNext()) {
				sum += values.iterator().next().get();
			}

			context.write(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);
		job.setMapperClass(MatrixGeneratorMapper.class);
		job.setReducerClass(MatrixGeneratorReducer.class);

		job.setJarByClass(CoOccurrenceMatrixGenerator.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		TextInputFormat.setInputPaths(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);

	}
}
