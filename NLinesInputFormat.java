import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.TextInputFormat;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;


public class NLinesInputFormat extends TextInputFormat
{
    @Override
    public RecordReader<LongWritable, Text> getRecordReader(InputSplit split, JobConf conf, Reporter reporter)throws IOException     {
        reporter.setStatus(split.toString());
        return new FileRecordReader(conf, (FileSplit)split);
    }
}

