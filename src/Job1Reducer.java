/***
 * Class Job1Reducer
 * Job1 Reducer class
 * @author sgarouachi
 */


import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Job1Reducer extends Reducer<Text, Text, Text, Text> {

	/**
	 * Job1 Reduce method (page, 1.0 \t outLinks)
	 * Remove redundant links & sort them Asc
	 */
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		// TODO if needed
		// Remove redundant outLinks
		// Sort outLinks by Asc
		// Append default page rank + outLinks
		

		
		ArrayList<Text> sortedList = new ArrayList<Text>();
		for (Text i : values) {
			if(! sortedList.contains(i)){
		    sortedList.add(i);
		}
		}
		Collections.sort(sortedList);
		
		
		
		
		
		String pagerank = "1.0\t";

        boolean first = true;
        for(Text val : sortedList){
            if(!first) pagerank += ",";
            
            pagerank += val.toString();
            first = false;
        }
        
        
        context.write(key, new Text(pagerank));
		
	}
}
