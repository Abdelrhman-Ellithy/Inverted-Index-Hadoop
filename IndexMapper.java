import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.StringTokenizer;
public class IndexMapper extends Mapper<LongWritable, Text, Text, Text> {
    private final Text wordAtFileNameKey = new Text();
    private final Text ONE_STRING = new Text("1");

    @Override
    protected void map(LongWritable key, Text value,
                       Context context) throws IOException, InterruptedException {
        FileSplit split = (FileSplit) context.getInputSplit();
        StringTokenizer tokenizer = new StringTokenizer(value.toString());
        while (tokenizer.hasMoreTokens()) {
            String fileName = split.getPath().getName().split("\\.")[0];
            wordAtFileNameKey.set(tokenizer.nextToken() + "@" + fileName);
            context.write(wordAtFileNameKey, ONE_STRING);
        }
    }
}
