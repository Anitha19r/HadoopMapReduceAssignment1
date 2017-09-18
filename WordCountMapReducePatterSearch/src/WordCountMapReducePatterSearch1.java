import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordCountMapReducePatterSearch1 {
	public static class MapForWordCount extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		private HashMap<String,Integer> search_file_word_hash = new HashMap<>();

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			URI[] search_file = context.getCacheFiles();
			if (search_file.length != 1) {
				System.out.println("Invalid number of search files");
			}
			if (search_file != null && search_file.length > 0) {
				URI search_file_word_hashFile = search_file[0];
				BufferedReader bufferedReader = new BufferedReader(
						new FileReader(search_file_word_hashFile.toString()));
				String search_file_word_hashWord = null;
				StringBuilder sb = new StringBuilder();
				while ((search_file_word_hashWord = bufferedReader.readLine()) != null) {
					sb.append(search_file_word_hashWord);
				}
				
				String[] inputWords = sb.toString().split("\\s");
				for(String s : inputWords){
					search_file_word_hash.put(s,1);
					System.out.println(s);
				}
				bufferedReader.close();

			}

		}

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
            String[] words=line.split("\\s");
            for(String word: words )
            {
            	if(search_file_word_hash.containsKey(word.toString().toLowerCase())){
            		Text outputKey = new Text(word.toUpperCase().trim());
            		IntWritable outputValue = new IntWritable(1);
            		context.write(outputKey, outputValue);
            	}
            }
		}
	}

	public static class ReduceForWordCount extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable value = new IntWritable(0);

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable value : values)
				sum += value.get();
			value.set(sum);
			context.write(key, value);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(WordCountMapReducePatterSearch1.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapperClass(MapForWordCount.class);
		job.setReducerClass(ReduceForWordCount.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setNumReduceTasks(1);
		job.addCacheFile(new URI("/anitha/input/pattern_1.txt")); //so that all the clusters can share the search file
		FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
		boolean success = job.waitForCompletion(true);
		if(success)
        	System.out.println("The job has terminated succesfully! ");
        else 
        	System.out.println("The job has terminated with errors! ");
	}
}
