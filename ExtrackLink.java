import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class ExtrackLink {

    public static class URLInfo {
        private String hostName;
        private int portNo;
        private String filePath;
        private long lastAccessTime;
        private int crawlDelays;

        private boolean isSecure = false;

        /**
         * Constructor called with raw URL as input.
         */
        public URLInfo(String docURL) {
            if (docURL == null || docURL.equals(""))
                return;
            docURL = docURL.trim();

            if (docURL.startsWith("https://")) {
                isSecure = true;
                docURL = docURL.replaceFirst("https:", "http:");
            }

            if (!docURL.startsWith("http://") || docURL.length() < 8)
                return;
            // Stripping off 'http://'
            docURL = docURL.substring(7);

            int i = 0;
            while (i < docURL.length()) {
                char c = docURL.charAt(i);
                if (c == '/')
                    break;
                i++;
            }
            String address = docURL.substring(0, i);
            if (i == docURL.length())
                filePath = "/";
            else{
                filePath = docURL.substring(i);
                if(filePath.contains(".")==false && filePath.endsWith("/")==false && filePath.length()!=1){
                    filePath = filePath + "/";
                }
            }
            filePath = filePath.substring(0,filePath.length()-1);
//            filePath = docURL.substring(i); // starts with '/'
            if (address.equals("/") || address.equals(""))
                return;
            if (address.indexOf(':') != -1) {
                String[] comp = address.split(":", 2);
                hostName = comp[0].trim();
                try {
                    portNo = Integer.parseInt(comp[1].trim());
                } catch (NumberFormatException nfe) {
                    portNo = isSecure ? 443 : 80;
                }
            } else {
                hostName = address;
                portNo = isSecure ? 443 : 80;
            }
        }

        public URLInfo(String hostName, String filePath) {
            this.hostName = hostName;
            this.filePath = filePath;
            this.portNo = 80;
        }

        public URLInfo(String hostName, int portNo, String filePath) {
            this.hostName = hostName;
            this.portNo = portNo;
            this.filePath = filePath;
        }

        public String getHostName() {
            return hostName;
        }

        public void setHostName(String s) {
            hostName = s;
        }

        public int getPortNo() {
            return portNo;
        }

        public void setPortNo(int p) {
            portNo = p;
        }

        public String getFilePath() {
            return filePath;
        }

        public void setFilePath(String fp) {
            filePath = fp;
        }

        public boolean isSecure() {
            return isSecure;
        }

        public void setSecure(boolean sec) {
            isSecure = sec;
        }

        public void setLastAccessTime(long t) {
            this.lastAccessTime = t;
        }

        public long getLastAccessTime() {
            return this.lastAccessTime;
        }

        public void setCrawlDelays(int t) {
            this.crawlDelays = t;
        }

        public int getCrawlDelays() {
            return this.crawlDelays;
        }

        public String toString() {
            return (isSecure ? "https://" : "http://") + hostName + ":" + portNo + filePath;
        }

        public String getDomain(){
            return (isSecure ? "https://":"http://") + hostName + ":" + portNo;
        }
    }


    public static class Map extends MapReduceBase implements Mapper<Object, Text, Text, Text> {
        private Text word = new Text();
        public void map(Object object, Text text, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
            String pages[] = text.toString().split("----KYCWNewLine----\n");
            for(String page:pages){
                String lines[] = page.toString().split("\n");
                String url = lines[0];
                if(url.contains("http")==false){
                    continue;
                }

                StringBuilder content = new StringBuilder();
                for(String s: lines){
                    content.append(s);
                    content.append(" ");
                }

                Document doc = Jsoup.parse(content.toString());
                Elements links = doc.getElementsByTag("a");
                for(Element link : links){
                    String linkText = link.attr("href");
                    if(linkText.startsWith("http")){
                        outputCollector.collect(new Text(url), new Text(linkText));
                    }else{
                        URLInfo urlInfo = new URLInfo(url);
                        outputCollector.collect(new Text(url), new Text(urlInfo.toString()+linkText));
                    }
                }
                System.out.println("url = " + url);
            }
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterator<Text> urlIterator, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {

            ArrayList<String> arrUrl = new ArrayList<String>();
            while(urlIterator.hasNext()){
                Text url = urlIterator.next();
                arrUrl.add(url.toString());
            }
            outputCollector.collect(key, new Text(arrUrl.toString()));


        }
    }


    public static void main(String[] args) throws IOException {
        File f = new File("output");
        if(f.exists()){
            f.delete();
        }

        JobConf conf = new JobConf(ExtrackLink.class);
        conf.setJobName("ExtrackLink");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(ExtrackLink.Map.class);
        conf.setReducerClass(ExtrackLink.Reduce.class);

        conf.setNumMapTasks(10);
        conf.setNumReduceTasks(5);


        conf.setInputFormat(NLinesInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.addInputPath(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
    }


}
