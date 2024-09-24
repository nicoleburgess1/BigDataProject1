import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
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
import java.util.Map;
import java.util.TreeMap;

/*Find the 10 most popular LinkBook pages, namely, those that got the most accesses
based on the AccessLog among all pages. Return Id, NickName, and Occupation.*/
public class TaskB {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text id = new Text();

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
                id.set(column[2]);
                context.write(id, one);
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
            result.set(sum);
            context.write(key, result);
        }
    }

    public static class LinkBookPageJoinMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text outKey = new Text();
        private Text outValue = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (fields.length >= 2) {
                outKey.set(fields[0].trim()); // Use ID as the key
                outValue.set("L" + fields[1].trim() + "," + fields[2]); // Prefix "L" to indicate LinkBookPage //nickname, occupation
                System.out.println("LinkBookPage Mapper: " + outKey.toString() + " -> " + outValue.toString());
                context.write(outKey, outValue);
            } else {
                System.err.println("LinkBookPage Mapper Error: Invalid input line - " + value.toString());
            }
        }
    }

    public static class AccessJoinMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text outKey = new Text();
        private Text outValue = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t"); // Split using tab, as this is the default output format
            if (fields.length == 2) {
                outKey.set(fields[0].trim()); // Use ID as the key
                outValue.set("A" + fields[1].trim()); // Prefix "A" to indicate Access count
                System.out.println("Access Mapper: " + outKey.toString() + " -> " + outValue.toString());
                context.write(outKey, outValue);

            } else {
                System.err.println("Access Mapper Error: Invalid input line - " + value.toString());
            }
        }
    }

    // Reducer for the second job: Joins the data from LinkBookPage and Associates counts
    public static class JoinReducer extends Reducer<Text, Text, Text, IntWritable> {
        private TreeMap<Integer, Text> maxTree = new TreeMap();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String nickname ="";
            String ocu = "";
            int count=0;
            int id = Integer.parseInt(key.toString());


            //System.out.println("Reducer Key: " + key.toString());

            // Separate LinkBookPage and Associates data
            for (Text val : values) {
                String value = val.toString();
                if (value.startsWith("L")) {
                    String[] fields = value.split(",");
                    nickname = fields[0].substring(1).trim();
                    ocu = fields[1].trim();
                    //System.out.println("Reducer LinkBookPage Value: " + value.substring(1));
                } else if (value.startsWith("A")) {
                    String[] fields = value.split("\t");
                    count = Integer.parseInt(fields[0].substring(1).trim());
                    //System.out.println("Reducer Associates Value: " + value.substring(1));
                } else {
                    System.err.println("Reducer Error: Invalid value - " + value.toString());
                }
            }

            // AccessReducer logic: For each linkbookpage name, associate it with the count or zero

            if (!nickname.equals("")) { //if it doens't have a nickname then it couldn't be a valid input
                String LBPInfo = id + "\t" + nickname + "\t"  + ocu; //returned LinkBookPage info
                maxTree.put(count, new Text(LBPInfo));
                if (maxTree.size() > 10) {
                    maxTree.remove(maxTree.firstKey()); // Remove the smallest count (in order smallest to largest)
                }
                //context.write(new Text(LBPInfo), new IntWritable(count));
                System.out.println("Reducer Output: " + count + ", " + LBPInfo );
            }
        }

        @Override
        protected void cleanup(Reducer<Text, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            for (Map.Entry<Integer, Text> entry : maxTree.entrySet()) {
                context.write(entry.getValue(), new IntWritable(entry.getKey()));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        //job1
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Page Access Count");
        job.setJarByClass(TaskB.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path("input/accessLogs.csv"));
        FileOutputFormat.setOutputPath(job, new Path("TaskB/TaskBOutput"));
        //System.exit(job.waitForCompletion(true) ? 0 : 1);
        job.waitForCompletion(true);

        //job2
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "Join with max 10");
        job2.setJarByClass(TaskB.class);

        // Adding Multiple Inputs
        MultipleInputs.addInputPath(job2, new Path("input/LinkBookPage.csv"),
                TextInputFormat.class, TaskB.LinkBookPageJoinMapper.class);
        MultipleInputs.addInputPath(job2, new Path("TaskB/TaskBOutput/part-r-00000"),
                TextInputFormat.class, TaskB.AccessJoinMapper.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setReducerClass(TaskB.JoinReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        FileOutputFormat.setOutputPath(job2, new Path("TaskB/TaskBJoinOutput"));
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}