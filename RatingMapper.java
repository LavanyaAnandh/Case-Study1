import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class RatingMapper extends Mapper<Object, Text, Text, Text> {
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
    {
    	 String record = value.toString();
         String[] parts = record.split(",");
         //The below code takes movie id and movie name as key and value and pass on to the reducer
         context.write(new Text(parts[1]), new Text("rating\t" + parts[2]));  
    }
}
