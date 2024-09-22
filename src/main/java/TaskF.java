import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/*Report all owners of a LinkBookPage who are more popular than an average user,
namely, those who have more relationships than the average number of relationships
across all owners LinkBookPages.*/
public class TaskF {

    public static int average;
    public static int total = 0;
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text Associates = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            //StringTokenizer itr = new StringTokenizer(value.toString());
            String dataset = value.toString();
            String[] datapoint = dataset.split("/n");
            String[][] columns = new String[datapoint.length][];
            for(int i = 0; i < columns.length; i++){
                columns[i] = datapoint[i].split(",");
            }


            for (String[] column : columns) {
                Associates.set(column[1]);
                context.write(Associates, one);
                Associates.set(column[2]);
                context.write(Associates, one);
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            total+=sum;
            average = total/100;
            System.out.println("average is "+average);
            result.set(sum);
            context.write(key, result);
        }
    }

    public void debug(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Education Count");
        job.setJarByClass(TaskA.class);
        job.setMapperClass(TaskA.TokenizerMapper.class);
        job.setCombinerClass(TaskA.IntSumReducer.class);
        job.setReducerClass(TaskA.IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Popular Count");
        job.setJarByClass(TaskF.class);
        job.setMapperClass(TaskF.TokenizerMapper.class);
        job.setCombinerClass(TaskF.IntSumReducer.class);
        job.setReducerClass(TaskF.IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path("TestAssociates.csv"));
        FileOutputFormat.setOutputPath(job, new Path("TaskFOutput"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
