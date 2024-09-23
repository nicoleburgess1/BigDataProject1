/*
Identify "outdated" LinkBookPage. Return IDs and nicknames of persons that have not
accessed LinkBook for 90 days (i.e., no entries in the AccessLog in the last 90 days)
*/


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;

public class TaskG {

    public static class LessThan90DaysMapper
            extends Mapper<Object, Text, Text, Text> {

        private Text ID = new Text();
        private Text one = new Text("A" + 1);
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
                Integer intCol = Integer.parseInt(column[4]);
                if (intCol<129600){
                    ID.set(column[1]);
                    context.write(ID, one);
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
            System.out.println(1);
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
                Nickname.set("L" + column[0] + " " + column[1]);
                System.out.println(Nickname.toString());
                context.write(ID, Nickname);
            }
            }
        }





    public static class JoinReducer
            extends Reducer<Text,Text,Text,IntWritable> {

        private ArrayList<String> linkbookpages = new ArrayList<>();
        private ArrayList<String> accessLog = new ArrayList<>();
        private IntWritable outputValue = new IntWritable();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Clear lists for new key
            linkbookpages.clear();
            accessLog.clear();

            System.out.println("Reducer Key: " + key.toString());

            // Separate LinkBookPage and Associates data
            for (Text val : values) {
                String value = val.toString();
                if (value.startsWith("L")) {
                    linkbookpages.add(value.substring(1)); // Remove "L" prefix
                    System.out.println("Reducer LinkBookPage Value: " + value.substring(1));
                } else if (value.startsWith("A")) {
                    accessLog.add(value.substring(1)); // Remove "A" prefix
                    System.out.println("Reducer Associates Value: " + value.substring(1));
                } else {
                    System.err.println("Reducer Error: Invalid value - " + value.toString());
                }
            }

            // JoinReducer logic: For each linkbookpage name, associate it with the count or zero
            for (String name : linkbookpages) {
                if (accessLog.isEmpty()) {
                    // No associations found, output "name, 0"
                    String[] nameID = name.split(" ");
                    Text Name = new Text();
                    Name.set(nameID[1]);
                    IntWritable ID = new IntWritable(Integer.parseInt(nameID[0]));
                    context.write(Name, ID);
                    System.out.println("Reducer Output: " + name + " -> 0");
                } else {
                    // Associations found, output "name, count"
                    for (String count : accessLog) {
                        outputValue.set(Integer.parseInt(count));
                        //context.write(new Text(name), outputValue);
                        System.out.println("Reducer Output: " + name + " -> " + count);
                    }
                }
            }
        }
        }


        public static void main(String[] args) throws Exception {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "ID Count");
            job.setJarByClass(TaskG.class);
            //job.setNumReduceTasks(0);
            job.setReducerClass(JoinReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);



            //FileInputFormat.addInputPath(job, new Path("TestaccessLogs.csv"));
            MultipleInputs.addInputPath(job, new Path("testAccessLogs.csv"),
                    TextInputFormat.class, TaskG.LessThan90DaysMapper.class);
            MultipleInputs.addInputPath(job, new Path("testLinkBookPage.csv"),
                    TextInputFormat.class, TaskG.LinkedBookMapper.class);
            FileOutputFormat.setOutputPath(job, new Path("TaskGOutput"));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }


    }


