import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;
import java.util.ArrayList;

/*
For each LinkBookPage, compute the “happiness factor” of its owner. That is, for each
LinkBookPage, report the owner’s nickname, and the number of relationships they have.
For page owners that aren't listed in Associates, return a score of zero. Please note that
we maintain a symmetric relationship, take that into account in your calculations.
*/
public class TaskD {

    // Mapper for the first job: Counts associations for each ID
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text id = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (fields.length >= 3) { // Ensures that there are at least two columns to process
                id.set(fields[1].trim()); // First associated ID
                context.write(id, one);

                id.set(fields[2].trim()); // Second associated ID
                context.write(id, one);
            }
        }
    }

    // Reducer for the first job: Sums up the counts for each ID
    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    // Mapper for LinkBookPage dataset in the second job
    public static class LinkBookPageJoinMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text outKey = new Text();
        private Text outValue = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (fields.length >= 2) {
                outKey.set(fields[0].trim()); // Use ID as the key
                outValue.set("L" + fields[1].trim()); // Prefix "L" to indicate LinkBookPage
                System.out.println("LinkBookPage Mapper: " + outKey.toString() + " -> " + outValue.toString());
                context.write(outKey, outValue);
            } else {
                System.err.println("LinkBookPage Mapper Error: Invalid input line - " + value.toString());
            }
        }
    }

    // Mapper for the first job's output in the second job
    public static class AssociatesJoinMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text outKey = new Text();
        private Text outValue = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t"); // Split using tab, as this is the default output format
            if (fields.length == 2) {
                outKey.set(fields[0].trim()); // Use ID as the key
                outValue.set("A" + fields[1].trim()); // Prefix "A" to indicate Associates count
                System.out.println("Associates Mapper: " + outKey.toString() + " -> " + outValue.toString());
                context.write(outKey, outValue);

            } else {
                System.err.println("Associates Mapper Error: Invalid input line - " + value.toString());
            }
        }
    }

    // Reducer for the second job: Joins the data from LinkBookPage and Associates counts
    public static class JoinReducer extends Reducer<Text, Text, Text, IntWritable> {
        private ArrayList<String> linkbookpages = new ArrayList<>();
        private ArrayList<String> associates = new ArrayList<>();
        private IntWritable outputValue = new IntWritable();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Clear lists for new key
            linkbookpages.clear();
            associates.clear();

            System.out.println("Reducer Key: " + key.toString());

            // Separate LinkBookPage and Associates data
            for (Text val : values) {
                String value = val.toString();
                if (value.startsWith("L")) {
                    linkbookpages.add(value.substring(1)); // Remove "L" prefix
                    System.out.println("Reducer LinkBookPage Value: " + value.substring(1));
                } else if (value.startsWith("A")) {
                    associates.add(value.substring(1)); // Remove "A" prefix
                    System.out.println("Reducer Associates Value: " + value.substring(1));
                } else {
                    System.err.println("Reducer Error: Invalid value - " + value.toString());
                }
            }

            // AccessReducer logic: For each linkbookpage name, associate it with the count or zero
            for (String name : linkbookpages) {
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
        // First MapReduce Job Configuration
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "Associates Count");
        job1.setJarByClass(TaskD.class);
        job1.setMapperClass(TokenizerMapper.class);

//        job1.setMapOutputKeyClass(Text.class);
//        job1.setMapOutputValueClass(IntWritable.class);

//        job1.setCombinerClass(AccessReducer.class);
        job1.setReducerClass(IntSumReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, new Path("input/Associates.csv"));
        FileOutputFormat.setOutputPath(job1, new Path("TaskDOutput"));
        job1.waitForCompletion(true);

        // Second MapReduce Job Configuration
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "AccessReducer LinkBookPage and Associates");
        job2.setJarByClass(TaskD.class);

        // Adding Multiple Inputs
        MultipleInputs.addInputPath(job2, new Path("input/LinkBookPage.csv"),
                TextInputFormat.class, LinkBookPageJoinMapper.class);
        MultipleInputs.addInputPath(job2, new Path("TaskDOutput/part-r-00000"),
                TextInputFormat.class, AssociatesJoinMapper.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);

        job2.setReducerClass(JoinReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        FileOutputFormat.setOutputPath(job2, new Path("TaskDJoinOutput"));
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}