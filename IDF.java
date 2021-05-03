import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class IDF {

    public static class Map extends MapReduceBase implements Mapper<Object, Text, Text, Text> {
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

    public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
        public void reduce(Text word, Iterator<Text> urlIterator, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {

            int df = 0;
            while (urlIterator.hasNext()) {
                Text url = urlIterator.next();
                df++;
            }

            double idf = Math.log10((double) 1000000 / df);
            outputCollector.collect(word, new Text("" + idf));

        }
    }

    public static void main(String[] args) throws IOException {
        File f = new File("output");
        if(f.exists()){
            f.delete();
        }

        JobConf conf = new JobConf(IDF.class);
        conf.setJobName("IDF");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(IDF.Map.class);
        conf.setReducerClass(IDF.Reduce.class);

        conf.setNumMapTasks(3);
        conf.setNumReduceTasks(3);

        conf.setInputFormat(NLinesInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.addInputPath(conf, new Path("input_term_frequency"));
        FileOutputFormat.setOutputPath(conf, new Path("output"));

        JobClient.runJob(conf);
    }
}
