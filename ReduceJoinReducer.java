import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReduceJoinReducer 
extends Reducer<Text, Text, Text, Text> {
  public void reduce(Text key, Iterable<Text> values, Context context) 
		  throws IOException, InterruptedException
     {
	     System.out.println("Reduce join reducer called"); //For logging purpose
	     String movieId = "";
    	 float AvgRating = 0;
    	 float total =0;
    	 int count = 0;
    	 System.out.println("Text Key    =>"+ key.toString());
    	 for(Text t : values)
    	 {
    		 String parts[] = t.toString().split("\t");
    		 System.out.println("Text Key   =>" + t.toString());
    	     if(parts[0].equals("rating"))
    	    		 {
    	    	       count++;  //code to check how many times a movie has been rated.
    	    	       total += Float.parseFloat(parts[1]); //code to get the overall rating for each movie
    	    	       
    	    		 }else if(parts[0].equals("movies"))
    	    		 {
    	    			 movieId = parts[1];
    	    		 }
    	     AvgRating = total/count;   //Code to find the average rating for each movie
    	 }
    	 String str = String.format("%d\t%f",count, AvgRating);
    	 context.write(new Text(movieId) , new Text(str));
     }
}
