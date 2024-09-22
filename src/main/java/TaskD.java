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
            result.set(sum);
            context.write(key, result);
        }
    }

    public static class LinkBookPageJoinMapper extends Mapper<LongWritable, Text, Text, Text> {

        private Text outkey = new Text();
        private Text outvalue = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // get a line of input from linkbookpage.csv
            String line = value.toString();
            String[] split = line.split(" ");
            // The foreign join key is the ID
            outkey.set(split[0]);
            // Flag this record for the reducer and then output
            outvalue.set("L" + split[1]);
            context.write(outkey, outvalue);
        }
    }

    public static class AssociatesJoinMapper extends Mapper<LongWritable, Text, Text, Text> {

        private Text outkey = new Text();
        private Text outvalue = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // get a line of input from job1output
            String line = value.toString();
            String[] split = line.split(" ");
            // The foreign join key is the ID
            outkey.set(split[0]);
            // Flag this record for the reducer and then output
            outvalue.set("A" + split[1]);
            context.write(outkey, outvalue);
        }
    }

    public static class JoinReducer extends Reducer<Text, Text, Text, Text> {

        private ArrayList<String> linkbookpages = new ArrayList<>();
        private ArrayList<String> associates = new ArrayList<>();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            // clear list

            linkbookpages.clear();
            associates.clear();

            // bin each record from both mapper based on the tag letter "L" or "A". Then, remove the tag.

            for(Text vals: values){
                if (vals.charAt(0) == 'L') {
                    linkbookpages.add(vals.toString().substring(1));
                } else if (vals.charAt(0) == 'A') {
                    associates.add(vals.toString().substring(1));
                }
            }
            // execute the join logic after both source lists are filled after iterating all values
            executeJoinLogic(context);
        }

        private void executeJoinLogic(Context context) throws IOException, InterruptedException {
            if (!linkbookpages.isEmpty() && !associates.isEmpty()) {
                for (String L : linkbookpages) {
                    boolean hasAssociates = false;
                    String[] splitLink = L.split(" ");
                    for (String A : associates) {
                        String[] splitAssociate = A.split(" ");
                        if(splitLink[0].equals(splitAssociate[0])) {
                            context.write(new Text(splitLink[1]), new Text(splitAssociate[1]));
                            hasAssociates = true;
                        }
                    }
                    if (!hasAssociates) {
                        context.write(new Text(splitLink[1]), new Text("0"));
                    }
                }

            }
        }
    }

    public void debug(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Associates count");
        job.setJarByClass(TaskD.class);
        job.setMapperClass(TaskD.TokenizerMapper.class);
        job.setCombinerClass(TaskD.IntSumReducer.class);
        job.setReducerClass(TaskD.IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Associates Count");
        job.setJarByClass(TaskD.class);
        job.setMapperClass(TaskD.TokenizerMapper.class);
        job.setCombinerClass(TaskD.IntSumReducer.class);
        job.setReducerClass(TaskD.IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path("TestAssociates.csv"));
        FileOutputFormat.setOutputPath(job, new Path("TaskDOutput"));
        job.waitForCompletion(true);

        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "Join");
        job2.setJarByClass(TaskD.class);
        //job2.setMapperClass(TaskD.LinkBookPageJoinMapper.class);
        //job2.setMapperClass(TaskD.AssociatesJoinMapper.class);
        MultipleInputs.addInputPath(job2, new Path("TestLinkBookPages.csv"), TextInputFormat.class, TaskD.LinkBookPageJoinMapper.class);
        MultipleInputs.addInputPath(job2, new Path("TaskDOutput"), TextInputFormat.class, TaskD.AssociatesJoinMapper.class);
        job2.setReducerClass(TaskD.JoinReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        //FileInputFormat.addInputPaths(job2, new Path("TestLinkBookPages.csv") + "," + new Path("TaskDOutput"));
        FileOutputFormat.setOutputPath(job2, new Path("TaskDJoinOutput"));
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
