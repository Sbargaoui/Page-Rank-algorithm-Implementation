/***
 * Class Job2Reducer
 * Job2 Reducer class
 * @author sgarouachi
 */

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Job2Reducer extends Reducer<Text, Text, Text, Text> {
	// Init dumping factor to 0.85
	private static final float damping = 0.85F;

	/**
	 * Job2 Reduce method Calculate the new page rank
	 */
	@Override
	public void reduce(Text page, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		// For each otherPage: 
        // - check control characters
        // - calculate pageRank share <rank> / count(<links>)
        // - add the share to sumShareOtherPageRanks
		
		// Write to output
		// (page, rank \t outLinks)
		// context.write(page, new Text(String.format(java.util.Locale.US,
		// "%.4f", newRank) + links));

		// TODO if needed
		 boolean isExistingWikiPage = false;
	        String[] split;
	        float sumShareOtherPageRanks = 0;
	        String links = "";
	        String pageWithRank;
	        
	        // For each otherPage: 
	        // - check control characters
	        // - calculate pageRank share <rank> / count(<links>)
	        // - add the share to sumShareOtherPageRanks
	        for(Text val : values){
	            pageWithRank = val.toString();
	            
	            if(pageWithRank.equals("!")) {
	                isExistingWikiPage = true;
	                continue;
	            }
	            
	            if(pageWithRank.startsWith("|")){
	                links = "\t"+pageWithRank.substring(1);
	                continue;
	            }

	            split = pageWithRank.split("\\t");
	            
	            float pageRank = Float.valueOf(split[1]);
	            int countOutLinks = Integer.valueOf(split[2]);
	            
	            sumShareOtherPageRanks += (pageRank/countOutLinks);
	        }

	        if(!isExistingWikiPage) return;
	        float newRank = damping * sumShareOtherPageRanks + (1-damping);

	        context.write(page, new Text(String.format(java.util.Locale.US, "%.4f", newRank) + links));
	    }
	}

