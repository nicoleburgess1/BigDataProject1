/*Determine which people have favorites. That is, for each LinkBookPage owner,
determine how many total accesses to LinkBookPage they have made (as reported in the
AccessLog) and how many distinct LinkBookPage they have accessed in total. As for the
identifier of each LinkBookPage owner, you don’t have to report name. IDs are enough.*/

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
import java.util.ArrayList;

public class TaskE {
    public static class TokenizerMapper
            extends Mapper<Object, Text, IntWritable, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private IntWritable accessID = new IntWritable();
        private IntWritable accessedID = new IntWritable();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            //StringTokenizer itr = new StringTokenizer(value.toString());
            String dataset = value.toString();
            String[] datapoint = dataset.split("\n");
            String[][] columns = new String[datapoint.length][];
            for(int i = 0; i < columns.length; i++){
                columns[i] = datapoint[i].split(",");
            }

            for (String[] column : columns) {
                accessID.set(Integer.parseInt(column[1]));
                accessedID.set(Integer.parseInt(column[2]));
                context.write(accessID, accessedID);
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<IntWritable,IntWritable,IntWritable,Text> {
        private Text result = new Text();

        public void reduce(IntWritable key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int accesses = 0; //sets num accesses for key to 0
            ArrayList<Integer> distinct = new ArrayList<>();
            for (IntWritable val : values) {
                accesses +=1; //total num of accesses/key increases
                int accessID = val.get(); //the ID accessed by key

                if(!distinct.contains(accessID)){
                    distinct.add(accessID); //adds to the list if key hasn't already accessed that ID
                }
            }
            result.set(accesses + "\t" + distinct.size());
            context.write(key, result); //ID, total accesses, total num distinct access

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "access Count");
        job.setJarByClass(TaskE.class);
        job.setMapperClass(TaskE.TokenizerMapper.class);
        job.setReducerClass(TaskE.IntSumReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path("input/accesslogs.csv"));
        FileOutputFormat.setOutputPath(job, new Path("TaskEOutput"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
