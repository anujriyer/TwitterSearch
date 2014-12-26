package twittersearch;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;


public class Streamer {
	private static final String CONSUMER_KEY = "3NlOtFYWxDs0NpExFRVWOQ";
	private static final String CONSUMER_SECRET = "7kHzLNKeeEXBV0TiHd5ebZhFYOKwFu7Z8l7bguEs";
	private static final String ACCESS_TOKEN = "1681059552-5kHqUYw2XJKJCzrRSf1GOj2ASZdZd4C0PzI0yPS";
	private static final String ACCESS_TOKEN_SECRET = "y9VRnG9cXgL0RrHWfIOA02r0ZfaqjSstFlv2YnYzFeMca";
	private int pnum[] = {0,0,0,0,0,0,0,0,0};

    public void streamData(TwitterDBConnect dbc){
    	TwitterStream twitterStream = null;
    	try {
	        StatusListener listener;                
	        
	        listener = new StatusListener(){
	            public void onStatus(Status status) {
	                //System.out.println(String.valueOf(status) + "\n\n");
	                String[] track = {"dove",
	                    "lipton",
	                    "unilever",
	                    "axe",
	                    "vaseline",
	                    "lifebuoy",
	                    "rexona",
	                    "knorr",
	                    "lux"};
	                
	                String product = "";
	                int i = 0;
	                for(String prod : track) {
	                    if(String.valueOf(status).toLowerCase().matches("(.*)"+prod+"(.*)")){
	                        product = prod;
	                        pnum[i]++; 
	                    }
	                    i++;
	                }
	                try {
	                	if (!product.equals("")) {
		                	System.out.println("Dove: " + pnum[0] + " Lipton: " + pnum[1] + " Unilever: " + pnum[2] + " Axe: " + pnum[3] + " Vaseline: " + pnum[4] + " Lifebuoy: " + pnum[5] + " Rexona: " + pnum[6] + " Knorr: " + pnum[7] + " Lux: " + pnum[8]);
		                	dbc.processTweet(status, product);
	                	}
	                } catch(Exception e) {
	                    System.err.println("Exception: " + e.getMessage());
	                }
	            }
	            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {}
	            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {}
	            public void onException(Exception ex) {
	                ex.printStackTrace();
	            }
	            
	            @Override
	            public void onScrubGeo(long l, long l1) {
	                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	            }
	            
	            @Override
	            public void onStallWarning(StallWarning sw) {
	                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
	            }
	        };
	        
	        ConfigurationBuilder cb = new ConfigurationBuilder();
	        cb.setDebugEnabled(true).setOAuthConsumerKey(CONSUMER_KEY)
	                .setOAuthConsumerSecret(CONSUMER_SECRET)
	                .setOAuthAccessToken(ACCESS_TOKEN)
	                .setOAuthAccessTokenSecret(ACCESS_TOKEN_SECRET)
	                .setJSONStoreEnabled(true);
	        
	        twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
	        twitterStream.addListener(listener);
	        // sample() method internally creates a thread which manipulates TwitterStream and calls these adequate listener methods continuously.
	        //twitterStream.sample();        
	        FilterQuery query = new FilterQuery();
	        String[] track = {"dove",
	                "lipton",
	                "unilever",
	                "axe",
	                "vaseline",
	                "lifebuoy",
	                "rexona",
	                "knorr",
	                "lux"};
	        query.track(track);
	        String[] lang = {"en"};
	        query.language(lang);
	        twitterStream.filter(query);	        
    	}
    	catch (Exception e) {
    		System.out.println(e.getMessage());
    		twitterStream.shutdown();
    	}
    }
}
