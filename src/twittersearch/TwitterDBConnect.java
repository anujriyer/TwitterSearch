package twittersearch;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;

import twitter4j.Status;
import twitter4j.TwitterObjectFactory;

import com.restfb.json.JsonObject;

public class TwitterDBConnect {
	private Connection connection = null;
	private final String dbUserName = "postgres";
	private final String dbPassword = "anuj";
	
	public TwitterDBConnect(){
		try { 
			Class.forName("org.postgresql.Driver");
			connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/mydb", dbUserName, dbPassword);
			if (connection == null) {				
				System.out.println("Failed to make connection to database! Contact Database admin!");
				System.exit(0);
			}
		} 
		catch (ClassNotFoundException e) { 
			System.out.println("JDBC Driver not found! Include in your library path!");
			e.printStackTrace();
			return; 
		} 
		catch (SQLException e) { 
			System.out.println("Connection Failed! Check output console");
			e.printStackTrace();
			return; 
		}
	}
	
	public void close() {
		try {
			connection.close();
		} catch (SQLException e) {
			System.out.println("Error in closing DB connection!");
			e.printStackTrace();
		}
	}
	
	public void processTweet(Status tweet, String product){
		String place, tweettext;
		if(tweet.getPlace() == null) {
            if(tweet.getUser().getLocation().isEmpty()) {
                place = "null";
            } else {
                place = tweet.getUser().getLocation().replaceAll(",", " ");
                place = place.replaceAll("(\\r\\n?|\\n)", " ").replaceAll("'", " ");
            }                            
        } else {
            place = tweet.getPlace().getName().replaceAll(","," ").replaceAll("'", " ");
        }
        
        /* Get the tweet text, where we need to remove all newline characters and commas */
        tweettext = tweet.getText().replaceAll("(\\r\\n?|\\n)", " ");
        tweettext = tweettext.replaceAll(",", " ").replaceAll("'", " ");
        
        JsonObject rawJson = new JsonObject(TwitterObjectFactory.getRawJSON(tweet).replaceAll("'", " "));
        
        JsonObject retweet, entities, coordinates;
        
        if (rawJson.has("retweeted_status"))
        	retweet = rawJson.getJsonObject("retweeted_status");
        else
        	retweet = new JsonObject("{}");
        
        entities = rawJson.getJsonObject("entities");
        
        if (rawJson.getString("coordinates").equals("null"))
        	coordinates = new JsonObject("{}");
        else 
        	coordinates = rawJson.getJsonObject("coordinates");
        
        insertTweetUser(rawJson.getJsonObject("user"));
        insertTweetData(Long.toString(tweet.getId()), tweet.getCreatedAt(), product, tweettext, 
        		tweet.getFavoriteCount(), retweet, entities, coordinates, Long.toString(tweet.getUser().getId()),
        		rawJson, place);
	}
	
	private void insertTweetUser(JsonObject userData) {
		String sql = "";
		try {			
			Statement s1 = connection.createStatement();
			
			sql = String.format("SELECT * FROM tweet_user WHERE user_id = '%s'", userData.getString("id"));
			ResultSet rs = s1.executeQuery(sql);
			
			if (!rs.isBeforeFirst()) {
				sql = String.format("INSERT INTO tweet_user VALUES ('%s', '%s', '%s', %s, %s)", 
						userData.getString("id"), 
						userData.getString("screen_name").replaceAll("'", " "), 
						userData.getString("time_zone").replaceAll("'", " "), 
						userData.getString("followers_count"), 
						userData.getString("friends_count"));
				s1.executeUpdate(sql);				
			} else {
				sql = String.format("UPDATE tweet_user SET follower_count = %s, friend_count = %s WHERE user_id = '%s'", 
						userData.getString("followers_count"), userData.getString("friends_count"), userData.getString("id"));
				s1.executeUpdate(sql);
			}
			s1.close();
		} catch (SQLException e) {
			System.out.println("Error executing statement: " + sql);
			e.printStackTrace();
		} catch (Exception e) {
			System.out.println("In User ins "+e);
		}
	}
	
	private void insertTweetData(String id, Date created_at, String product, String tweettext, int fav_count, JsonObject retweet, 
			JsonObject entities, JsonObject coordinates, String user_id, JsonObject rawJson, String place) {
		String sql = "";
		try {			
			Statement s1 = connection.createStatement();
			
			sql = String.format("SELECT * FROM twitter_table WHERE tweet_id = '%s'", id);
			ResultSet rs = s1.executeQuery(sql);
			
			if (!rs.isBeforeFirst()) {
				sql = String.format("INSERT INTO twitter_table VALUES ('%s', '%s', '%s', '%s', %s, '%s', '%s', '%s', '%s', '%s', '%s')", 
						id, created_at.toString(), product,	tweettext, Integer.toString(fav_count), retweet, entities, coordinates, 
						user_id, rawJson, place);
				s1.executeUpdate(sql);				
			} 
			s1.close();
		} catch (SQLException e) {
			System.out.println("Error executing statement: " + sql);
			e.printStackTrace();
		} catch (Exception e) {
			System.out.println("In Tweet ins "+e);
		}
	}
}
