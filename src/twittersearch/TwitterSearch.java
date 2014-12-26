package twittersearch;

public class TwitterSearch{
	public static void main(String args[]) {
		TwitterDBConnect dbc = new TwitterDBConnect();
		Streamer s = new Streamer();
		s.streamData(dbc);
	}
}