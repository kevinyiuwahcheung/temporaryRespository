import java.io.File;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import java.lang.*;



public class TermFreqency {
    public static class Map extends MapReduceBase implements Mapper<Object, Text, Text, Text>{
        private Text word = new Text();
        public void map(Object object, Text text, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {

            String lines[] = text.toString().split("\n");
            String url = lines[0];
            lines[0]="";

            StringBuilder content = new StringBuilder();
            for(String s: lines){
                content.append(s);
                content.append(" ");
            }

            StringTokenizer st = new StringTokenizer(content.toString());

            int numToken = st.countTokens();
            for(int i=0;i<numToken;i++){
                String word = st.nextToken();
                Text key = new Text(url+","+word);
                System.out.println("Mapper outputing " + "key = " + key.toString() + ", value = 1");
                outputCollector.collect(key, new Text(word));
            }

        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text>{
        public void reduce(Text key, Iterator<Text> iterator, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
            int count = 0 ;

            while(iterator.hasNext()){
                count++;
                iterator.next();
            }

            double tf = Math.log10(count+1);
            System.out.println("Reducer Key = " +key.toString()+", tf = " + tf);

            outputCollector.collect(key, new Text(""+tf));

        }
    }


    public static void main(String[] args) throws IOException {
        File f = new File("output");
        if(f.exists()){
            f.delete();
        }

        JobConf conf = new JobConf(TermFreqency.class);
        conf.setJobName("TermFreqency");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(TermFreqency.Map.class);
//        conf.setCombinerClass(TermFreqency.Reduce.class);
        conf.setReducerClass(TermFreqency.Reduce.class);

        conf.setNumMapTasks(3);
        conf.setNumReduceTasks(3);

        conf.setInputFormat(NLinesInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.addInputPath(conf, new Path("input_term_frequency"));
        FileOutputFormat.setOutputPath(conf, new Path("output"));

        JobClient.runJob(conf);
    }
}
