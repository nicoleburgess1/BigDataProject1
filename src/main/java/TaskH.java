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
                values.set("A" + column[2] + " " + column[1]);
                Associates.set(column[2] + " " + column[1]);
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
                values.set("acc" + column[1] + " " + column[2]);
                context.write(Access, values);
            }
        }
    }

    public static class JoinReducer extends Reducer<Text, Text, Text, IntWritable> {
        private ArrayList<String> access = new ArrayList<>();
        private ArrayList<String> associates = new ArrayList<>();
        private IntWritable outputValue = new IntWritable();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Clear lists for new key
            access.clear();
            associates.clear();

            System.out.println("Reducer Key: " + key.toString());

            // Separate LinkBookPage and Associates data
            for (Text val : values) {
                String value = val.toString();
                if (value.startsWith("acc")) {
                    access.add(value.substring(1)); // Remove "L" prefix
                    System.out.println("Reducer LinkBookPage Value: " + value.substring(1));
                } else if (value.startsWith("A")) {
                    associates.add(value.substring(1)); // Remove "A" prefix
                    System.out.println("Reducer Associates Value: " + value.substring(1));
                } else {
                    System.err.println("Reducer Error: Invalid value - " + value.toString());
                }
            }

            // JoinReducer logic: For each linkbookpage name, associate it with the count or zero
            for (String name : access) {
                if (associates.isEmpty()) {
                    // No associations found, output "name, 0"
                    outputValue.set(0);
                    context.write(new Text(name), outputValue);
                    System.out.println("Reducer Output: " + name + " -> 0");
                } else {
                    // Associations found, output "name, count"
                    for (String count : associates) {
                        outputValue.set(Integer.parseInt(count));
                        context.write(new Text(name), outputValue);
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
        //job.setNumReduceTasks(0);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        MultipleInputs.addInputPath(job, new Path("testAccessLogs.csv"),
                TextInputFormat.class, TaskH.accessMapper.class);
        MultipleInputs.addInputPath(job, new Path("testAssociates.csv"),
                TextInputFormat.class, TaskH.TokenizerMapper.class);
        FileOutputFormat.setOutputPath(job, new Path("TaskHOutput"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}