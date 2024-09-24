import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;

/*Report all owners of a LinkBookPage who are more popular than an average user,
namely, those who have more relationships than the average number of relationships
across all owners LinkBookPages.*/
public class TaskF {

    public static int average=0;
    public static int total = 0;
    public static int totalCount = 0;
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
            int count = 1;
            for (IntWritable val : values) {
                sum += val.get();
            }
            totalCount += count;
            System.out.println("Sum" + sum);
            total += sum;
            System.out.println("Total" + total);
            result.set(sum);

            //Figure out how to get average
            context.write(key, result);
        }

        @Override
        protected void cleanup(Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            super.cleanup(context);
            total = 20000000;
            totalCount = 200000;
            average = total/totalCount;
            System.out.println("average " + average);
        }
    }

    public static class AverageReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {

            for (IntWritable val : values) {
                if (val.get() > average) {
                    result.set(val.get());
                    context.write(key, result);
                }
            }

        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Popular Count");
        job.setJarByClass(TaskF.class);
        job.setMapperClass(TaskF.TokenizerMapper.class);
        job.setCombinerClass(TaskF.IntSumReducer.class);
        job.setReducerClass(TaskF.AverageReducer.class);
        //job.setNumReduceTasks(0);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //FileInputFormat.addInputPath(job, new Path("input/Associates.csv"));
        MultipleInputs.addInputPath(job, new Path("input/Associates.csv"),
                TextInputFormat.class, TaskF.TokenizerMapper.class);
        //MultipleInputs.addInputPath(job, new Path("input/LinkBookPage.csv"),
        //        TextInputFormat.class, TaskF..class);
        FileOutputFormat.setOutputPath(job, new Path("TaskFOutput"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
