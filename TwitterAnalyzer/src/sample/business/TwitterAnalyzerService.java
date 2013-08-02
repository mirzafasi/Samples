/**
 * 
 */
package sample.business;

import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import sample.ITwitterAnalyzer;
import sample.ITwitterAnalyzerDao;
import sample.dao.TwitterAnalyzerDaoImpl;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.Twitter;
import twitter4j.TwitterFactory;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.auth.AccessToken;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.json.DataObjectFactory;

import com.mongodb.DBObject;

/**
 * @author mirza
 *
 */
public class TwitterAnalyzerService implements ITwitterAnalyzer{
	
	private ITwitterAnalyzerDao twitterAnalyzerDao;
	
	public TwitterAnalyzerService() {
		this.twitterAnalyzerDao = TwitterAnalyzerDaoImpl.createInstanceOfDao();
	}

	@Override
	public void feedData(List<DBObject> list) {
		twitterAnalyzerDao.feedData(list);
		
	}

	public AccessToken getTwitterAccessToken(){
		try{
			Twitter twitter = TwitterFactory.getSingleton();
			AccessToken accessToken = new AccessToken("1633691779-fTMz8n5gsgOcwNyQWE8nTtdxkLBEHPW8rYgOrZa",
					"vtArAhyzBdrCFFk4qa5NLOoMleI7w04B9iiLAWv8");
		    twitter.setOAuthConsumer("P3pxu4o3TQvpZ6ihQTYeIA", "t9MU0Fbcx9ztqCGv9Auvy5vqmdx5DBD7dIgfQMKbeBY");
		    twitter.setOAuthAccessToken(accessToken);
		    
		}catch(Exception e){
			e.printStackTrace();
		}
		return null;
		
	}
	
	public void getSampleStreams(int count){
		OutputStream outputStream = null;
		TwitterStream twitterStream = null;
		try{
			ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
			configurationBuilder.setJSONStoreEnabled(true);
			Configuration configuration = configurationBuilder.build();
			twitterStream = new TwitterStreamFactory(configuration).getInstance();
			AccessToken accessToken = new AccessToken("1633691779-fTMz8n5gsgOcwNyQWE8nTtdxkLBEHPW8rYgOrZa",
					"vtArAhyzBdrCFFk4qa5NLOoMleI7w04B9iiLAWv8");
			twitterStream.setOAuthConsumer("P3pxu4o3TQvpZ6ihQTYeIA", "t9MU0Fbcx9ztqCGv9Auvy5vqmdx5DBD7dIgfQMKbeBY");
			twitterStream.setOAuthAccessToken(accessToken);
			final List<DBObject> dbObjects = new ArrayList<DBObject>();
			outputStream = new FileOutputStream("sample.txt");
	        StatusListener listener = new StatusListenerImpl(outputStream);
	        twitterStream.addListener(listener);
	        twitterStream.sample();
	        int counter = 0;
	        while((counter = ((StatusListenerImpl)listener).getTweetCount()) < count){
	        	System.out.println("TwitterAnalyzerService.getSampleStreams() Getting Tweet #"+counter);
	        }
	        
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			if(twitterStream != null){
				twitterStream.shutdown();
			}
			
    		if(outputStream != null){
    			try{
    				outputStream.close();
    			}catch(IOException exception){}
    		}
    	}
		
	}
	
	private static class StatusListenerImpl implements StatusListener{
		
		private OutputStream outputStream;
		
		private int tweetCount = 0;
		
		public StatusListenerImpl(OutputStream outputStream) {
			this.outputStream = outputStream;
		}

		public int getTweetCount(){
			return tweetCount;
		}
		
		@Override
		public void onException(Exception paramException) {
			// TODO Auto-generated method stub
			
		}

		 public void onStatus(Status status) {
             String tweet = DataObjectFactory.getRawJSON(status);
             if(tweet != null){
             	
             	try{
             		
             		byte[] bytes = convertStringToBytes(tweet);
             		if(bytes.length > 0){
             			outputStream.write(bytes);
             			outputStream.write(convertStringToBytes("\n"));
             			tweetCount += 1;
             		}
             	}catch(Exception e){
             		e.printStackTrace();
             	}
             			
             }               

         }
         
         private byte[] convertStringToBytes(String str){
         	ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
         	if(str != null){
         		char[] chars = str.toCharArray();
         		for(char c:chars){
         			byteArrayOutputStream.write((byte)c);
         		}            		
         	}
         	return byteArrayOutputStream.toByteArray();
         }


		@Override
		public void onDeletionNotice(
				StatusDeletionNotice paramStatusDeletionNotice) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void onTrackLimitationNotice(int paramInt) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void onScrubGeo(long paramLong1, long paramLong2) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void onStallWarning(StallWarning paramStallWarning) {
			// TODO Auto-generated method stub
			
		}
		
	}

	@Override
	public List<DBObject> findTopFollowed(int limit) {
		return twitterAnalyzerDao.findTopFollowed(limit);
		
	}

	@Override
	public List<DBObject> findTopRetweeted(int limit) {
		return twitterAnalyzerDao.findTopRetweeted(limit);
	}

	@Override
	public List<DBObject> findTopMentioned(int limit) {
		return twitterAnalyzerDao.findTopMentioned(limit);
	}
	
}
