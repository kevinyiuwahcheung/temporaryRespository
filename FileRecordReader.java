import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.RecordReader;

import java.io.IOException;

public class FileRecordReader implements RecordReader<LongWritable, Text>
{
    private LineRecordReader lineRecord;
    private LongWritable lineKey;
    private Text lineValue;

    public FileRecordReader(JobConf conf, FileSplit split) throws IOException {
        lineRecord = new LineRecordReader(conf, split);
        lineKey = lineRecord.createKey();
        lineValue = lineRecord.createValue();
    }


    public boolean next(LongWritable longWritable, Text text) throws IOException {
        boolean appended, gotsomething;
        boolean retval;
        byte space[] = {' '};
        byte newline[] = {'\n'};
        text.clear();
        gotsomething = false;

        do{
            appended = false;
            retval = lineRecord.next(lineKey, lineValue);
            if(retval){
                byte[] rawline = lineValue.getBytes();
                int rawlinelen = lineValue.getLength();
                text.append(rawline,0,rawlinelen);
                text.append(newline,0,1);
                appended=true;
                gotsomething=true;
            }

        }while(appended);

        return gotsomething;
    }

    public LongWritable createKey() {
        return new LongWritable();
    }

    public Text createValue() {
        return new Text("");
    }

    public long getPos() throws IOException {
        return lineRecord.getPos();
    }

    public void close() throws IOException {

    }

    public float getProgress() throws IOException {
        return lineRecord.getPos();
    }
}