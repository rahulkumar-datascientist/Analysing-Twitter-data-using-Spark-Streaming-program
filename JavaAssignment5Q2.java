package main.java;

// Assignment 5 :   Tools & Techniques for Large Scale Data Analytics
// Name         :   Rahul Kumar
// Student ID   :   20230113

// Q2. Extend the code to count the number of characters, words and extract the hashtags in each tweet.

// Output: for each tweet from twitter stream (received every second)  ------>
//                                  1. list of words of the tweet (obtained after splitting the tweet at "SPACES") (Printing to verify the counts).
//                                  2. Word count, character count and list of Hashtags of the tweet.

// importing required libraries
import com.google.common.io.Files;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.*;
import org.apache.spark.streaming.api.java.*;
import twitter4j.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

// Class containing the main()
class JavaAssignment5Q2{
    // Driver function definition
    public static void main(String[] args) {

        //Setting properties from Twitter Account
        System.setProperty("twitter4j.oauth.consumerKey", "7uTS3H1dK9NOiNWIBFoSnvnSh");
        System.setProperty("twitter4j.oauth.consumerSecret", "038lHmjdRA6GDvxYIFBepSu9KkA7mbehlxHaAyuWvNCd1IZSOy");
        System.setProperty("twitter4j.oauth.accessToken", "1333378396999520256-PbE4etyHZyZbZRbebwzlaOp4PvpaFC");
        System.setProperty("twitter4j.oauth.accessTokenSecret", "6qXT4m7vTMA75MaNht1wTqxwdsWc1f4c1wvxIZ61kAaMO");

        // setting up the Spark configuration and Spark Streaming context
        SparkConf config = new SparkConf().setMaster("local[2]").setAppName("Assignment5");
        JavaStreamingContext jssc = new JavaStreamingContext(config, new Duration(1000));

        // Create a tweet_stream DStream containing objects of type twitter4j.Status
        JavaDStream<Status> tweet_stream = TwitterUtils.createStream(jssc);

        // Transform the tweet_stream DStream to create a DStream of tweets using map function to extract the text from the tweet
        JavaDStream<String> tweets = tweet_stream.map(Status::getText);


        // **************************************** Calculating count per tweet ****************************************************

        // A JavaDStream that maps the tweets to its word count, charater count and a list of extracted Hashtags
        JavaDStream<String> counts = tweets.map(tweet_text -> {

            List<String> word_list = Arrays.asList(tweet_text.split(" "));            // words in a tweet
            List<String> Hashtags = new ArrayList<>();                                      // List of Hashtags in a tweet

            int word_count = word_list.size();                                              // count of words in a tweet
            int char_count = tweet_text.length();                                           // count of chars in a tweet

            for (String temp : word_list) {                                                 // extracting Hashtags from the tweet
                if(temp.startsWith("#")){                                                   // and saving in a list
                    Hashtags.add(temp);
                }
            }

            // returning the actual tweet as a list of words and the word, character counts and list of hashtags to verify the same
            return "\n\n" + word_list + "\nWord count : " + word_count + " , Charater Count :  " +
                    char_count + " , Hashtags : " + Hashtags;
        });

        // **************************************** Printing phase ****************************************************

        // print tweet with the count details
        counts.print();

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





