package main.java;

// Assignment 5 :   Tools & Techniques for Large Scale Data Analytics
// Name         :   Rahul Kumar
// Student ID   :   20230113

// Q1. Write a standalone Spark Streaming program that connects to Twitter and prints a sample of the tweets it receives
// from twitter every second.

// Output: sample of tweets from twitter stream received every second.


// importing required libraries
import com.google.common.io.Files;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.*;
import org.apache.spark.streaming.api.java.*;
import twitter4j.*;

// Class containing the main()
class JavaAssignment5Q1{
    // Driver function defination
    public static void main(String[] args) {
    	
    	//Setting properties from Twitter Account
        System.setProperty("twitter4j.oauth.consumerKey", "<palce your token here>");
        System.setProperty("twitter4j.oauth.consumerSecret", "<palce your token here>");
        System.setProperty("twitter4j.oauth.accessToken", "<palce your token here>");
        System.setProperty("twitter4j.oauth.accessTokenSecret", "<palce your token here>");

        // setting up the Spark configuration and Spark Streaming context
        SparkConf config = new SparkConf().setMaster("local[2]").setAppName("Assignment5");
        JavaStreamingContext jssc = new JavaStreamingContext(config, new Duration(1000));

        // Create a tweet_stream DStream containing objects of type twitter4j.Status
        JavaDStream<Status> tweet_stream = TwitterUtils.createStream(jssc);

        // Transform the tweet_stream DStream to create a DStream of tweets using map function to extract the text from the tweet
        JavaDStream<String> tweets = tweet_stream.map(status -> status.getText()   );

        // print records of the DStream
        tweets.print();

        // Create checkpoint
        jssc.checkpoint(Files.createTempDir().getAbsolutePath());

        // set the context to start running the computation we have setup.
        jssc.start();
        try {
            jssc.awaitTermination();
        } 
        catch (InterruptedException e) {
          e.printStackTrace();
        }
    }
}





