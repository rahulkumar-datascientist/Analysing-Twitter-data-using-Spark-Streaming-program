package main.java;

// Assignment 5 :   Tools & Techniques for Large Scale Data Analytics
// Name         :   Rahul Kumar
// Student ID   :   20230113

// Q3c. For the last 5 minutes of tweets, continuously repeat the computation of step A and B every 30 seconds.

// Output: for tweets from twitter stream (5 minute window sliding every 30 seconds) ------>
//                          1. Average number of words and characters per tweet in last 30 seconds
//                          2. Top 10 Hashtags in last 30 seconds

// importing required libraries
import com.google.common.io.Files;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.*;
import org.apache.spark.streaming.api.java.*;
import scala.Tuple2;
import twitter4j.*;
import java.util.Arrays;
import java.util.List;

// Class containing the main()
class JavaAssignment5Q3c{
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



        // ************************ Calculating count per tweet with window of 5 minutes sliding every 30 seconds ********************

        // A Stream of Tuples where the tweets are mapped to its word count, character count
        JavaPairDStream<Integer, Integer> counts = tweets.mapToPair(tweet_text -> {

            List<String> word_list = Arrays.asList(tweet_text.split(" "));      // words in a tweet

            int word_count = word_list.size();                          // count of words in a tweet
            int char_count = tweet_text.length();                           // count of chars in a tweet

            return new Tuple2<>(word_count, char_count);                  // returning the word count and character counts
        }).window(Durations.minutes(5), Durations.seconds(30));      // get the tweets in a window of 5 minutes, sliding every 30 seconds



        // ************************* Calculating Top 10 Hashtags with window of 5 minutes sliding every 30 seconds ********************

        // For each twitter DStream frame, split each tweet on spaces to get the words.
        // Filter out the words that start with # to get hasTags
        // use flatmap to get all the hashtags in the stream
        JavaDStream<String> Hashtags = tweets.flatMap( tweet_text ->
                Arrays.stream(tweet_text.split(" ")).filter(word -> word.startsWith("#")).iterator());

        // A stream of Tuples where each Hashtag is mapped to 1 as (key, 1) pair ----------> (Hashtag, 1)
        // Map phase
        JavaPairDStream<String, Integer> Hashtags_counts = Hashtags.mapToPair(tag -> new Tuple2<>(tag, 1));

        // A Stream of Tuples where each hashtags is reduced by adding up the values
        // Reduce phase    ---> (A,1)  (B,1)   (A,1)   ----> (A,2) (B,1)
        JavaPairDStream<String, Integer> Hashtag_map_reduce =

                // using reduce by key and window --> to return a new single-element stream,
                // created by aggregating elements in the stream over a sliding interval
                // get the hashtags for 5 minutes , where the window slides every 30 seconds
                Hashtags_counts.reduceByKeyAndWindow((Integer x, Integer y) -> x+y , Durations.minutes(5), Durations.seconds(30));


        // Swapping the key and values in order to use sort by keys
        // (A,2)  (B,3)   -----> (2,A)  (3,B)
        JavaPairDStream<Integer, String> Swap_Hashtags_counts = Hashtag_map_reduce.mapToPair(tup -> tup.swap());

        // sort by key to get the stream of tuples (count,hashtags) in descending order
        JavaPairDStream<Integer, String> Sorted_Hashtags_counts =
                Swap_Hashtags_counts.transformToPair(rdd-> rdd.sortByKey(false));



        // **************************************** Printing Phase ****************************************************

        // use foreachRDD to access individual RDD inside the JavaDStream
        counts.foreachRDD(result -> {
            // printing out the average word count and character count in a tweet
            try {
                System.out.format("\n\nAverage word count per tweet in last 30 seconds: %.4f \t " +
                                  "Average character count per tweet in last 30 seconds:  %.4f \n\n" ,
                        result.mapToDouble(rs -> rs._1).mean(), result.mapToDouble(rs -> rs._2).mean());
            }
            catch (UnsupportedOperationException e){
                System.out.println("\n\nThis DStream has no data at the moment\n\n");
            }
                });


        // use foreachRDD to convert the stream to RDD
        Sorted_Hashtags_counts.foreachRDD( rdd -> {

            System.out.println("\n\nTop 10 Hashtags# in the last 30 seconds\n\n");
            for (Tuple2<Integer, String> t : rdd.take(10)) {            // rdd.take() --> to pick the elements for RDD
                System.out.println(t.toString() + "\n");
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





