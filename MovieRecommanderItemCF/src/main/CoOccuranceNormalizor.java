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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/*
bench mark
makotoBot98
*/
//job3: normalize the co-occurance matrix
/**
 * steps: from job2, we have key-value pairs of (movieID1:movieID2,
 * co-occuranceCount)
 * 
 * mapper: map all movieID1 to the same reducer. 
 * 
 * reducer: output the normalized key-value pair. 
 * First collect the sum of (movieID1:movieID2, 2), (movieID1:movieID3, 4)... 
 * collect the sum of all counts from movieID1 to movieID?,
 * 
 * 
 * and devide each key-value pair by the sum to normalize output the normalized co-occurance matrix in columnar representation 
 * 
 * 
 * (so matrix cells are output with key = columnID and value = rowID:normalized_co-occurance_count)
 */
public class CoOccuranceNormalizor {

    public static class NormalizeMapper extends Mapper<LongWritable, Text, Text, Text> {

        // map method
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            // value = movieID1:movieID2 \t cooccuranceCount
            //output key: movieID1 = rowID of co-occurance matrix
            //output value: movieID2:cooccuranceCount

            String[] splits = value.toString().trim().split("\t");
            String coCount = splits[1];
            String[] moviePair = splits[0].split(":");

            context.write(new Text(moviePair[0]), new Text(moviePair[1] + ":" + splits[1]));
        }
    }

    public static class NormalizeReducer extends Reducer<Text, Text, Text, Text> {
        // reduce method
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            // key = movieA(fromMovie, the rowID), value=<toMovie:coCount, movieB:coCount1, movieC:coCount2...>
            //output key: toMovieID(movie ID != movieA, so it's the columnar ID for cooccurance matrix)
            //output value: coCount_toMovieID / total_count

            int sum = 0;
            Map<String, Integer> map = new HashMap<>();
            while (values.iterator().hasNext()) {
                String s = values.iterator().next().toSring();
                String[] pair = s.split(":");
                String movieID = pair[0];
                int coCount = Integer.parseInt(pair[1]);
                map.put(movieID, coCount);
                sum += coCount;
            }
            //iterate map again to devide all coCount by sum and then write to output
            String fromMovie = key.toString();
            for (Map.Entry<String, Integer> entry : map.entrySet()) {
                String movieID = entry.getKey();
                int coCount = entry.getValue();
                double normalizedCount = (double) coCount / sum;
                //output key = columnID = toMovie, outputvalue = fromMovie + "=" + normalizedCount
                context.write(new Text(movieID), new Text(fromMovie + "=" + normalizedCount));
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setMapperClass(NormalizeMapper.class);
        job.setReducerClass(NormalizeReducer.class);

        job.setJarByClass(CoOccuranceNormalizor.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        TextInputFormat.setInputPaths(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
