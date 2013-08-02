/**
 * 
 */
package sample.main;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import sample.ITwitterAnalyzer;
import sample.business.TwitterAnalyzerService;

import com.mongodb.DBObject;
import com.mongodb.util.JSON;

/**
 * Main class acts as the entry point to this application.
 * Data analysis is performed in the following sequence:
 * 1. Data from sample file: sample.txt is inserted into the MongoDB database.
 * 2. A list of Json objects containing users being followed(in descending order) is returned.
 * 3. A list of Json objects containing tweets which were re-tweeted(in descending order) is returned.
 * 4. A list of Json objects containing users which were mentioned in tweets (in descending order) is returned.
 * @author mirza
 *
 */
public class Main {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
//		ITwitterAnalyzer twitterAnalyzer = new TwitterAnalyzerService();
//		twitterAnalyzer.getSampleStreams(200);
		insertDataFromTextFile();
		findTopFollowed();
		findTopRetweeted();
		findTopMentioned();
	}

	
	private static void insertDataFromTextFile(){
		InputStreamReader inputStreamReader = null;
		List<DBObject> dbList = null;
		try{
			InputStream inputStream = Main.class.getClassLoader().getResourceAsStream(
					"sample.txt");
			inputStreamReader = new InputStreamReader(inputStream, "UTF-8");
			BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
			String string = "";
			dbList = new ArrayList<DBObject>();
			while((string = bufferedReader.readLine()) != null){
				try{
					DBObject dbObject = (DBObject)JSON.parse(string);
					dbList.add(dbObject);
					
				}catch(Exception jsonException){}
			}
			if(dbList != null){
				ITwitterAnalyzer twitterAnalyzer = new TwitterAnalyzerService();
				twitterAnalyzer.feedData(dbList);
			}
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			if(inputStreamReader != null){
				try{
					inputStreamReader.close();
				}catch(IOException exception){}
			}
		}
	}
	
	private static void findTopFollowed(){
		ITwitterAnalyzer twitterAnalyzer = new TwitterAnalyzerService();
		List<DBObject> objects = twitterAnalyzer.findTopFollowed(5);
		if(objects != null){
			for(DBObject dbObject:objects){
				System.out.println("Main.findTopFollowed()"+dbObject.toString());	
			}
		}
	}
	
	private static void findTopRetweeted(){
		ITwitterAnalyzer twitterAnalyzer = new TwitterAnalyzerService();
		List<DBObject> objects = twitterAnalyzer.findTopRetweeted(5);
		if(objects != null){
			for(DBObject dbObject:objects){
				System.out.println("Main.findTopRetweeted()"+dbObject.toString());	
			}
			
		}
	}
	
	private static void findTopMentioned(){
		ITwitterAnalyzer twitterAnalyzer = new TwitterAnalyzerService();
		List<DBObject> objects = twitterAnalyzer.findTopMentioned(5);
		if(objects != null){
			for(DBObject dbObject:objects){
				System.out.println("Main.findTopMentioned()"+dbObject.toString());	
			}
			
		}
	}
}
