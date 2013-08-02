/**
 * 
 */
package sample;

import java.util.List;

import com.mongodb.DBObject;

/**
 * Interface defines methods for retrieving data from Twitter and perform 
 * database operations.
 * This interface acts as the DAO layer.
 * @author mirza
 *
 */
public interface ITwitterAnalyzerDao {

	/**
	 * Method inserts data into MongoDB database
	 * @param list - list of Json Objects
	 */
	public void feedData(List<DBObject> list);
	
	/**
	 * Method calculates the follower count for each user.
	 * It orders the result and returns a Json Object of the form-
	 * "value" : { "userId" : <User Name> , "count" : <Count of followers>}
	 * 
	 * @param limit - count of users to be returned
	 * @return - Json Objects in Descending order
	 */
	public List<DBObject> findTopFollowed(int limit);
	
	/**
	 * Method calculates the count of tweets which have been re-tweeted.
	 * It orders the result and returns a Json Object of the form-
	 * {"retweeted_status" : { "retweet_count" : <number of re-tweets>} , "text" : <tweet text> , "id" : <tweet id>}
	 * @param limit - count of tweets to be returned
	 * @return - Json Objects in Descending order
	 */
	public List<DBObject> findTopRetweeted(int limit);

	/**
	 * Method calculates number of mentions per user.
	 * It orders the result and returns a Json Object of the form-
	 * { "_id" : <User Id> , "value" : <Count of mentions>}
	 * 
	 * @param limit - count of user mentions to be returned
	 * @return - Json Objects in Descending order
	 */
	public List<DBObject> findTopMentioned(int limit);
	
}
