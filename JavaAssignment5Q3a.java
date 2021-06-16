package main.java;

// Assignment 5 :   Tools & Techniques for Large Scale Data Analytics
// Name         :   Rahul Kumar
// Student ID   :   20230113

// Q3a. Extend the code to find the average number of characters and words per tweet.

// Output: for tweets from twitter stream (received every second) ------>
//                          1. sum of all words and character of all tweets every second     (printing to verify the average)
//                          2. number of tweets in every second                              (printing to verify the average)
//                          3. Average number of words and characters per tweet every second

// importing required libraries
import com.google.common.io.Files;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.*;
import org.apache.spark.streaming.api.java.*;
import scala.Tuple2;
import twitter4j.*;
import java.util.Arrays;
import java.util.List;

// Class containing the main()
class JavaAssignment5Q3a{
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

        // A JavaPairDStream that maps the tweets to its word count, character count
        JavaPairDStream<Integer, Integer> counts = tweets.mapToPair(tweet_text -> {

            List<String> word_list = Arrays.asList(tweet_text.split(" "));      // words in a tweet

            int word_count = word_list.size();                          // count of words in a tweet
            int char_count = tweet_text.length();                           // count of chars in a tweet

            return new Tuple2<>(word_count, char_count);                  // returning the word count and character counts
        });


        // **************************************** Printing Phase *****************************************************************

        // use foreachRDD to access individual RDD inside the JavaDStream
        counts.foreachRDD(result -> {

            // printing out each tweets word count and character count present in RDD -- for viewing
            //for(Tuple2<Integer, Integer> rs :result.collect()){
            //    System.out.println("\nWord Count of Tweet : " + rs._1 + "\t\t Character count of Tweet: " + rs._2);
            //}

            // printing out the total number of word count and character count in the RDD
            System.out.format("\n\nTotal word count of tweets in this stream (RDD) : %.2f \t\t " +
                              "Total character count of tweets in this stream (RDD) :  %.2f \n\n" ,
                    result.mapToDouble(rs -> rs._1).sum(), result.mapToDouble(rs -> rs._2).sum());

            // printing out the total numbers of tweets present in the RDD
            System.out.println("\n\n********** Total no. of tweets in this stream (RDD): " + result.count() + "   **********\n\n");

            // printing out the average word count and character count in a tweet
            try {
                System.out.format("\n\nAverage word count per tweet : %.4f \t Average character count per tweet :  %.4f \n\n" ,
                        result.mapToDouble(rs -> rs._1).mean(), result.mapToDouble(rs -> rs._2).mean());
            }
            catch (UnsupportedOperationException e){
                System.out.println("\n\nThis DStream has no data at the moment\n\n");
            }
        });

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





