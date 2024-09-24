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

/*Identify people that have a relationship with someone (Associates); yet never accessed
their respective friend’s LinkBookPage. Report IDs and nicknames.*/
public class TaskH {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, Text> {

        private Text values = new Text();
        private Text Associates = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            //StringTokenizer itr = new StringTokenizer(value.toString());
            String dataset = value.toString();
            String[] datapoint = dataset.split("/n");
            String[][] columns = new String[datapoint.length][];
            for (int i = 0; i < columns.length; i++) {
                columns[i] = datapoint[i].split(",");
            }

            for (String[] column : columns) {
                Associates.set(column[1] + " " + column[2]);
                values.set("A" + column[1] + " " + column[2]);
                context.write(Associates, values);
                Associates.set(column[2] + " " + column[1]);
                values.set("A" + column[2] + " " + column[1]);
                context.write(Associates, values);
            }
        }
    }

    public static class accessMapper
            extends Mapper<Object, Text, Text, Text> {

        private Text values = new Text();
        private Text Access = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            //StringTokenizer itr = new StringTokenizer(value.toString());
            String dataset = value.toString();
            String[] datapoint = dataset.split("/n");
            String[][] columns = new String[datapoint.length][];
            for (int i = 0; i < columns.length; i++) {
                columns[i] = datapoint[i].split(",");
            }

            for (String[] column : columns) {
                Access.set(column[1] + " " + column[2]);
                values.set("B" + column[1] + " " + column[2]);
                context.write(Access, values);
            }
        }
    }




    public static class JoinReducer
            extends Reducer<Text,Text,Text,IntWritable> {

        private ArrayList<String> associates = new ArrayList<>();
        private ArrayList<String> accessLog = new ArrayList<>();
        private IntWritable outputValue = new IntWritable();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Clear lists for new key
            associates.clear();
            accessLog.clear();

            System.out.println("Reducer Key: " + key.toString());

            // Separate LinkBookPage and Associates data
            for (Text val : values) {
                String value = val.toString();
                if (value.startsWith("A")) {
                    associates.add(value.substring(1)); // Remove "L" prefix
                    System.out.println("Reducer LinkBookPage Value: " + value.substring(1));
                } else if (value.startsWith("B")) {
                    accessLog.add(value.substring(1)); // Remove "A" prefix
                    System.out.println("Reducer Associates Value: " + value.substring(1));
                } else {
                    System.err.println("Reducer Error: Invalid value - " + value.toString());
                }
            }

            // JoinReducer logic: For each linkbookpage name, associate it with the count or zero
            for (String vals : associates) {
                if (accessLog.isEmpty()) {
                    // No associations found, output "name, 0"
                    String[] ID = vals.split(" ");
                    Text ID1 = new Text();
                    ID1.set(ID[1]);
                    IntWritable ID0 = new IntWritable(Integer.parseInt(ID[0]));
                    context.write(ID1, ID0);
                    System.out.println("Reducer Output: " + vals + " -> 0");
                } else {
                    // Associations found, output "name, count"
                    for (String count : accessLog) {
                        //outputValue.set(Integer.parseInt(count));
                        //context.write(new Text(name), outputValue);
                        System.out.println("Reducer Output: " + vals + " -> " + count);
                    }
                }
            }
        }
    }

    public static class LinkedBookMapper
            extends Mapper<Object, Text, Text, Text> {

        private Text Nickname = new Text();
        private Text ID = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String dataset = value.toString();
            String[] datapoint = dataset.split("/n");
            String[][] columns = new String[datapoint.length][];
            for(int i = 0; i < columns.length; i++){
                columns[i] = datapoint[i].split(",");
                System.out.println(2);
            }


            for (String[] column : columns) {
                System.out.println(3);
                ID.set(column[0]);
                Nickname.set("A" + column[0] + " " + column[1]);
                System.out.println(Nickname.toString());
                context.write(ID, Nickname);
            }
        }
    }


    public static class IDMapper
            extends Mapper<Object, Text, Text, Text> {


        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            Text keys = new Text();
            Text values=  new Text();
            keys.set(key.toString());
            values.set("B1");
            context.write(keys, values);

        }
    }

    public static class JoinReducer2 extends Reducer<Text, Text, Text, IntWritable> {
        private ArrayList<String> linkbookpages = new ArrayList<>();
        private ArrayList<String> IDs = new ArrayList<>();
        private IntWritable outputValue = new IntWritable();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Clear lists for new key
            linkbookpages.clear();
            IDs.clear();

            System.out.println("Reducer Key: " + key.toString());

            // Separate LinkBookPage and Associates data
            for (Text val : values) {
                String value = val.toString();
                if (value.startsWith("A")) {
                    linkbookpages.add(value.substring(1)); // Remove "L" prefix
                    System.out.println("Reducer LinkBookPage Value: " + value.substring(1));
                } else if (value.startsWith("B")) {
                    IDs.add(value.substring(1)); // Remove "A" prefix
                    System.out.println("Reducer Associates Value: " + value.substring(1));
                } else {
                    System.err.println("Reducer Error: Invalid value - " + value.toString());
                }
            }

            // AccessReducer logic: For each linkbookpage name, associate it with the count or zero
            for (String name : linkbookpages) {
                if (IDs.isEmpty()) {
                    // No associations found, output "name, 0"
                    outputValue.set(0);
                    context.write(new Text(name), outputValue);
                    System.out.println("Reducer Output: " + name + " -> 0");
                } else {
                    // Associations found, output "name, count"
                    for (String count : IDs) {
                        //outputValue.set(Integer.parseInt(count));
                        //context.write(new Text(name), outputValue);
                        System.out.println("Reducer Output: " + name + " -> " + count);
                    }
                }
            }
        }
    }





    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Popular Count");
        job.setJarByClass(TaskH.class);
        job.setMapperClass(TaskH.TokenizerMapper.class);
        //job.setCombinerClass(TaskF.IntSumReducer.class);
        job.setReducerClass(TaskH.JoinReducer.class);
        //job.setNumReduceTasks(1);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job, new Path("input/AccessLogs.csv"),
                TextInputFormat.class, TaskH.accessMapper.class);
        MultipleInputs.addInputPath(job, new Path("input/Associates.csv"),
                TextInputFormat.class, TaskH.TokenizerMapper.class);
        FileOutputFormat.setOutputPath(job, new Path("TaskHOutput"));

        job.waitForCompletion(true);


        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "Popular Count");
        job2.setJarByClass(TaskH.class);
        job2.setMapperClass(TaskH.TokenizerMapper.class);
        //job.setCombinerClass(TaskF.IntSumReducer.class);
        job2.setReducerClass(TaskH.JoinReducer2.class);
        //job.setNumReduceTasks(1);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job2, new Path("input/LinkBookPage.csv"),
                TextInputFormat.class, TaskH.LinkedBookMapper.class);
        MultipleInputs.addInputPath(job2, new Path("TaskHOutput/part-r-00000"),
                TextInputFormat.class, TaskH.IDMapper.class);
        FileOutputFormat.setOutputPath(job2, new Path("TaskHOutput2"));



        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}