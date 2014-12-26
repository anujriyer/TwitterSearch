package twittersearch;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import twitter4j.Status;

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
	
	public void processTweet(Status status, String product){
		
	}
	
	private void insertTweetUser() {
		String sql = "";
		try {			
			Statement s1 = connection.createStatement();
			sql = String.format("SELECT * FROM post_table WHERE post_id = '%s'", );
			ResultSet rs = s1.executeQuery(sql);
			if (!rs.isBeforeFirst()) {
				sql = String.format("INSERT INTO post_table VALUES ('%s', '%s', %s, %s, '%s', '%s', '%s', %s)", 
						);
				s1.executeUpdate(sql);				
			} else {
				//Update user details..
			}
			s1.close();
		} catch (SQLException e) {
			System.out.println("Error executing statement: " + sql);
			e.printStackTrace();
		}
	}
	
	private void insertTweetData() {
		String sql = "";
		try {			
			Statement s1 = connection.createStatement();
			String msg, story;
			
			//First check if post is already available:
			sql = String.format("SELECT * FROM post_table WHERE post_id = '%s'", );
			ResultSet rs = s1.executeQuery(sql);
			if (!rs.isBeforeFirst()) {
				sql = String.format("INSERT INTO post_table VALUES ('%s', '%s', %s, %s, '%s', '%s', '%s', %s)", 
						);
				s1.executeUpdate(sql);				
			} else {
				//Not sure what to do..
			}
			s1.close();
		} catch (SQLException e) {
			System.out.println("Error executing statement: " + sql);
			e.printStackTrace();
		}
	}
}
