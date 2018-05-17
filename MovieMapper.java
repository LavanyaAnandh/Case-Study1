
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class MovieMapper extends Mapper<Object, Text,Text,Text>{
            public void map(Object key, Text value, Context context) throws IOException, InterruptedException
            {
            	String record = value.toString();
            	String[] parts = record.split(",");
            	//The below code takes movie id and rating name as key and value and pass on to the reducer
            	context.write(new Text(parts[0]), new Text("movies\t" + parts[1]));
            }
}
