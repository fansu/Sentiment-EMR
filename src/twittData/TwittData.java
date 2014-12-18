package twittData;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.SendMessageRequest;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;

public class TwittData {
	public static AmazonSQS sqs;
	public static AmazonS3 s3;
	public static String myQueueUrl;
	public static int data_count = 0;
	
	public static void main(String[] args) throws IOException {	
		System.out.println("--Reading credential information...");
		AWSCredentials credentials;
		try {
			credentials = new PropertiesCredentials(
					TwittData.class.getResourceAsStream("AwsCredentials.properties"));
	        sqs = new AmazonSQSClient(credentials);
	        s3 = new AmazonS3Client(credentials);
	        Region usWest2 = Region.getRegion(Regions.US_WEST_2);
	        sqs.setRegion(usWest2);
	        s3.setRegion(usWest2);
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}


		TwitterStream twitterStream = new TwitterStreamFactory().getInstance();	
		 StatusListener listener = new StatusListener(){
	        public void onStatus(Status t) {			 
				String text = t.getText().replace("'","");
				System.out.println(t.getCreatedAt().getTime()/60000 + "::" + text);
				File dir = new File(Long.toString(t.getCreatedAt().getTime()/600000));
				if (!dir.exists())
					dir.mkdir();
				try(PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(t.getCreatedAt().getTime()/600000 + "/" + t.getCreatedAt().getTime()/600000 + ".txt", true)))) {
				    out.println(text);
				}catch (IOException e) {
				    //exception handling left as an exercise for the reader
				}			        	
	        }
	        public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {}
	        public void onTrackLimitationNotice(int numberOfLimitedStatuses) {}
	        public void onException(Exception ex) {
//		            ex.printStackTrace();
	        }
			@Override
			public void onScrubGeo(long arg0, long arg1) {
				// TODO Auto-generated method stub
				
			}
			@Override
			public void onStallWarning(StallWarning arg0) {
				// TODO Auto-generated method stub
				
			}
	    }; 
	
		FilterQuery q = new FilterQuery();
		String[] keywordsArray = { "news"};
	    q.track(keywordsArray);		
	    twitterStream.addListener(listener);
	    System.out.println(q);
		twitterStream.filter(q);
		
	    // sample() method internally creates a thread which manipulates TwitterStream and calls these adequate listener methods continuously.
	    twitterStream.sample();    
	}
}