/**
 * 
 */
package sample.dao;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import sample.ITwitterAnalyzerDao;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MapReduceCommand;
import com.mongodb.MongoClient;
import com.mongodb.WriteResult;

/**
 * @author mirza
 *
 */
public class TwitterAnalyzerDaoImpl implements ITwitterAnalyzerDao {
	
	private static final String DATABASE_NAME = "TWITTER_DB";
	
	private static final String COLLECTION_NAME = "TWEETS";
	
	private static ITwitterAnalyzerDao twitterAnalyzerDao;
	
	private MongoClient mongoClient;
	
	private TwitterAnalyzerDaoImpl() {
		
		createConnection();
	}

	public static ITwitterAnalyzerDao createInstanceOfDao(){
		if(twitterAnalyzerDao == null){
			twitterAnalyzerDao = new TwitterAnalyzerDaoImpl();
		}
		return twitterAnalyzerDao;
	}
	

	private MongoClient createConnection(){
	
		InputStream inputStream = this.getClass().getClassLoader().getResourceAsStream("db.properties");
		try{
			Properties properties = new Properties();
			if(inputStream != null){
				properties.load(inputStream);
				if(mongoClient == null){
					String host = properties.getProperty("host");
					int port = Integer.parseInt(properties.getProperty("port"));
					mongoClient = new MongoClient(host, port);
					
					//create DB if it does not exist
					mongoClient.getDB(DATABASE_NAME);
				}

				//TODO: Also check if the connection is closed.
			}
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			if(inputStream != null){
				try{
					inputStream.close();
				}catch(IOException exception){
					
				}
			}
		}
		return mongoClient;
	}

	
	private void closeConnection() {
		if(this.mongoClient != null){
			mongoClient.close();
		}
	}

	@Override
	public void feedData(List<DBObject> list) {
		MongoClient client = createConnection();
		if(client != null){
			DB db = mongoClient.getDB(DATABASE_NAME);
			DBCollection collection = db.getCollection(COLLECTION_NAME);
			collection.drop();
			WriteResult result = collection.insert(list);
		}
		
	}

	

//	//TODO: use cursors to speed up operation
//	@Override
//	public String getAllData() {
//		if(mongoClient != null){
//			DB db = mongoClient.getDB(DATABASE_NAME);
//			Set<String> collectionNames = 
//					db.getCollectionNames();
//			if(collectionNames != null){
//				for(String collectionName:collectionNames){
//					DBCollection collection = db.getCollection(collectionName);
//					
//				}
//			}
//		}
//		return null;
//	}

	public List<DBObject> findTopFollowed(int limit){
		try{
				StringBuilder mapFunctionbuilder = new StringBuilder();
				mapFunctionbuilder.append("function() {");
				mapFunctionbuilder.append("emit(this.user.id, {userId:this.user.name,count:this.user.followers_count});");
				mapFunctionbuilder.append("}");
				
				StringBuilder reduceFunctionbuilder = new StringBuilder();
				reduceFunctionbuilder.append("function(userId, followerArray) {");
				reduceFunctionbuilder.append("retVal = {user_Id:userId,count:0};");
				reduceFunctionbuilder.append("for(var i in followerArray)");
				reduceFunctionbuilder.append("retVal.count += followerArray[i].count;");
				reduceFunctionbuilder.append("return retVal;");
				reduceFunctionbuilder.append("}");
				
				MongoClient client = createConnection();
				if(client != null){
					DB db = mongoClient.getDB(DATABASE_NAME);
					DBCollection collection = db.getCollection(COLLECTION_NAME);
				MapReduceCommand mapReduceCommand = 
						new MapReduceCommand(collection, mapFunctionbuilder.toString(),
								reduceFunctionbuilder.toString(), "followers",
								MapReduceCommand.OutputType.REPLACE, null);
				
				collection.mapReduce(mapReduceCommand);
				
				DBCursor cursor = db.getCollection("followers").find(new BasicDBObject(),new BasicDBObject("_id",0)).
				sort(new BasicDBObject("value.count",-1)).limit(limit);
				if(cursor.count() > 0){
					return cursor.toArray();
				}
			}
		}catch(Exception exception){
			exception.printStackTrace();
		}
		return null;
	}
	
	public List<DBObject> findTopRetweeted(int limit){
		try{
			MongoClient client = createConnection();
			if(client != null){
				DB db = mongoClient.getDB(DATABASE_NAME);
				DBCollection collection = db.getCollection(COLLECTION_NAME);
				DBObject projections = new BasicDBObject();
				projections.put("id", 1);
				projections.put("_id", 0);
				projections.put("text", 1);
				projections.put("retweeted_status.retweet_count", 1);
				DBCursor cursor = collection.
						find(new BasicDBObject("retweeted_status", new BasicDBObject("$exists", true)),projections).
						sort(new BasicDBObject("retweeted_status.retweet_count", -1)).limit(limit);
				if(cursor.count() > 0){
					return cursor.toArray();
				}
			}
		}catch(Exception exception){
			exception.printStackTrace();
		}
		return null;
	}
	
	public List<DBObject> findTopMentioned(int limit){
		try{
			MongoClient client = createConnection();
			if(client != null){
				DB db = mongoClient.getDB(DATABASE_NAME);
				DBCollection collection = db.getCollection(COLLECTION_NAME);
				DBObject query = new BasicDBObject("entities.user_mentions", new BasicDBObject("$exists", true));
				query.put("entities.user_mentions.name", new BasicDBObject("$exists", true));
				DBObject projections = new BasicDBObject();
				projections.put("id", 1);
				projections.put("_id", 0);
//				projections.put("text", 1);
				projections.put("entities.user_mentions.name", 1);
				DBCursor cursor = collection.
						find(query,projections);
				if(cursor.count() > 0){
					List<DBObject> list = cursor.toArray();
					List<DBObject> tempList = new ArrayList<DBObject>();
					//this workaround is needed since we are using projections. Projections do not get the complete DBObject.
					for(DBObject dbObject:list){
						tempList.add(new BasicDBObject(dbObject.toMap()));
					}
					
					DBCollection tempCollection = db.getCollection("temp");
					tempCollection.drop();
					tempCollection.insert(tempList);
					
					StringBuilder mapFunctionbuilder = new StringBuilder();
					mapFunctionbuilder.append("function() {");
					mapFunctionbuilder.append("for (var iD = 0; iD < this.entities.user_mentions.length; iD++) {");
					mapFunctionbuilder.append("emit(this.entities.user_mentions[iD].name, 1);}");
					mapFunctionbuilder.append("}");
					
					StringBuilder reduceFunctionbuilder = new StringBuilder();
					reduceFunctionbuilder.append("function(userName, countArray) {");
					reduceFunctionbuilder.append("sum=0;");
					reduceFunctionbuilder.append("for(var i in countArray)");
					reduceFunctionbuilder.append("sum += countArray[i];");
					reduceFunctionbuilder.append("return sum;");
					reduceFunctionbuilder.append("}");
					
					MapReduceCommand mapReduceCommand = 
							new MapReduceCommand(collection, mapFunctionbuilder.toString(),
									reduceFunctionbuilder.toString(), "mentioned",
									MapReduceCommand.OutputType.REPLACE, null);
					
					collection.mapReduce(mapReduceCommand);
					
					DBCursor newCursor = db.getCollection("mentioned").find().sort(new BasicDBObject("value",-1)).limit(limit);;
					if(newCursor.count() > 0){
						return newCursor.toArray();
					}
					
				}
				
				
			}
		}catch(Exception exception){
			exception.printStackTrace();
		}
		return null;
	}

}
