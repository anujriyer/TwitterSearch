package twittersearch;

public class TwitterSearch{
	public static void main(String args[]) {
		TwitterDBConnect dbc = new TwitterDBConnect();
		try {
			Streamer s = new Streamer();
			s.streamData(dbc);
		} finally {
			dbc.close();
		}
	}
}