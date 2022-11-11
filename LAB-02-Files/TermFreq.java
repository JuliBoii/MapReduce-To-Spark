import java.io.IOException;
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

/* RESOURCES USED TO HELP ME
 * 
 * Goes through the wordcount example given by hadoop
 * https://ggbaker.ca/732/content/hadoop.html 
 * 
 * Helped me understand how to format strings,
 * I thought of it as something similar to how you 
 * can use the iomanip header in C++ to
 * manipulate how something is outputted
 * https://www.javatpoint.com/java-string-format
 * 
 * This forum post on StackOverflow helped me
 * understand how to break up the a line to separate the 
 * sentence and the document ID.
 * While also showing me that it is possible to combine
 * multiple variables into a key so that it could be
 * passed in Hadoop.
 * https://stackoverflow.com/questions/34263288/java-hadoop-mapreduce-multiple-value
 * 
 * This websites also helped with the sentence break-up:
 * https://www.scaler.com/topics/string-to-array-in-java/
 * https://www.geeksforgeeks.org/split-string-java-examples/
 */

public class TermFreq {

    public static class TermFreqMapper extends
            Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            // Convert the inputted sentence to a string data type
            String lineStr = value.toString();
            /*
             * Find the instance in the string where a comma appears
             * and save that index number
             */
            int index = lineStr.indexOf(',');

            // if there was a comma in the string continue
            if (index != -1) {
                /*
                 * Since we know the document ID is written prior to the comma
                 * Thus, we save everything prior to the comma index
                 * This is done so later on we can associate a word
                 * to its respective document ID.
                 */
                String documentID = lineStr.substring(0, index);
                /*
                 * Then we save the remainder of the string into a sentence variable.
                 * So we want everything after the comma index
                 * That way we do not keep the comma
                 * nor need to remove it at later stage
                 */
                String sentenceStr = lineStr.substring(index + 1);

                // Now we traverse the sentence and pass the tokens
                // as (key, value) pairs to the reducer function.
                StringTokenizer itr = new StringTokenizer(sentenceStr);
                while (itr.hasMoreTokens()) {
                    /*
                     * To ensure the keys are sorted based on the documentID
                     * We combine the documentID string in front and the 
                     * respective token together and make a new Text variable,
                     * which is what word.set() is doing.
                     */
                    word.set(documentID + " " + itr.nextToken());
                    context.write(word, one);
                }
            }
        }
    }

    public static class TermFreqReducer extends
            Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();
        private Text word = new Text();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            // Sums up the values which are the occurrence counts for each key
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            // Here we take the key that is paired with the given value
            // and convert the Text variable into a string variable.
            String idAndWord = key.toString();
            /*
             * From there we separate the string into an array.
             * Since we made the key be a combination of:
             * document ID + " " + token
             * we know there exist a space between the
             * document ID and the token, so we split
             * the string based on the space
             */
            String[] sent = idAndWord.split(" ");
            /*
             * Since the first element in the string array was the
             * document ID, we save that in a string variable
             * from the array.
             */
            String documentID = sent[0];

            // Since the second element in the string array was the
            // token, we save that in a string variable.
            String token = sent[1];
            /*
             * To ensure the spacing between the token and Document ID
             * is consistent, we format the strings with empty spacing
             * using the format() method.
             * the '-' in "%-10s" makes sure the token is left aligned
             * while the '10' signifies the number of total spaces to
             * have at all times, the same applies to the '3' for
             * the document ID
             * The 's' simply signifies that the output, after formatting,
             * will be a string variable.
             */
            documentID = String.format("%3s", documentID);
            token = String.format("%-10s", token);
            
            // we set the value to the sum
            result.set(sum);
            /*
             * Then once again, we combine the token and its
             * respective documentID followed by a ',' and
             * make a new Text variable, so it can be outputted
             * by the reducer function.
             */
            word.set(token + documentID + ",");
            context.write(word, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "term freq");
        job.setJarByClass(TermFreq.class);
        job.setMapperClass(TermFreqMapper.class);
        job.setReducerClass(TermFreqReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}