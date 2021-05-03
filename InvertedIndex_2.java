import java.io.File;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.NLineInputFormat;
import org.apache.hadoop.util.*;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import javax.print.Doc;


public class InvertedIndex_2 {


    public static class Map extends MapReduceBase implements Mapper<Object, Text, Text, Text>{
        private Text word = new Text();
        public void map(Object object, Text text, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException{
            String pages[] = text.toString().split("----KYCWNewLine----\\n");
            for(String page:pages){
                String lines[] = page.toString().split("\n");
                String url = lines[0];
                lines[0]="";

                StringBuilder content = new StringBuilder();
                for(String s: lines){
                    content.append(s);
                    content.append(" ");
                }
                Document doc = Jsoup.parse(content.toString());
                StringTokenizer st = new StringTokenizer(doc.text());
                Set<String> wordSet = new HashSet<String>();
                while(st.hasMoreTokens()){
                    wordSet.add(st.nextToken());
                }
                for(String word:wordSet){
                    outputCollector.collect(new Text(word), new Text(url));
                }
            }
        }


    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text>{
        public void reduce(Text word, Iterator<Text> urlIterator, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {

            ArrayList<String> arrUrl = new ArrayList<String>();
            while(urlIterator.hasNext()){
                Text url = urlIterator.next();
                arrUrl.add(url.toString());
            }
            outputCollector.collect(new Text(word), new Text(arrUrl.toString()));

        }
    }


    public static void main(String[] args) throws IOException {
        JobConf conf = new JobConf(InvertedIndex.class);
        conf.setJobName("invertedIndex");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(InvertedIndex_2.Map.class);
        conf.setReducerClass(InvertedIndex_2.Reduce.class);

        conf.setNumMapTasks(5);
        conf.setNumReduceTasks(5);


        conf.setInputFormat(NLinesInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.addInputPath(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
    }


}
