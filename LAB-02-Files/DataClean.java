import java.io.IOException;
import java.util.*; 
import java.util.stream.Collectors;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DataClean {

    public static class DataCleanMapper extends
            Mapper<Object, Text, Text, Text> {
        
        public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
            
            context.write(new Text("temp"), value);
        }
    }

    public static class DataCleanReducer extends
            Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            List<String> list = new ArrayList<String>();
            
            int count = 0;
            for (Text val : values) {
                String str = val.toString();
                list.add(str);
                count++;
            }

            List<String> sortedList = list.stream()
                                          .sorted(Comparator.naturalOrder())
                                          .collect(Collectors.toList());
            
            int lastNum = 0;

            String lastLetter = "A";

            for (String p : sortedList) {
                int index = p.indexOf(',');
                int currentNum = Integer.parseInt(p.substring(0, index));
                if (currentNum == lastNum + 1) {
                    lastLetter = p.substring(index + 1);
                    lastNum++;
                    Text pair = new Text(currentNum + "," + p.substring(index + 1));
                    context.write(key, pair);
                } else {
                    int newNum = lastNum + 1;
                    Text pair = new Text( newNum + "," + lastLetter);
                    context.write(key, pair);
                    Text catchUp = new Text(currentNum + "," + p.substring(index + 1));
                    context.write(key, catchUp);
                }
            }
            // for (int i = 0; i < sortedList.size(); i++) {
            //     String str = sortedList.get(i);
            //     int index = str.indexOf(',');
            //     int currentNumber = Integer.parseInt(str.substring(0, index));
            //     context.write(key, pair);
            // }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "data clean");
        job.setJarByClass(DataClean.class);
        job.setMapperClass(DataCleanMapper.class);
        job.setReducerClass(DataCleanReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}