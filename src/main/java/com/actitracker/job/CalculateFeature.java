package com.actitracker.job;

import com.actitracker.data.DataManager;
import com.actitracker.data.ExtractFeature;
import com.actitracker.data.PrepareData;
import com.actitracker.model.SamplePoint;
import com.datastax.spark.connector.japi.CassandraRow;
import com.datastax.spark.connector.japi.rdd.CassandraJavaRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;

import static com.actitracker.data.ExtractFeature.*;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.*;

public class CalculateFeature {

    static String lastReadFile = "lastread";

    public static void main(String[] args) {

        lastSeen();
        writeLastSeen(System.currentTimeMillis());
        lastSeen();
        // define Spark context
        /*SparkConf sparkConf = new SparkConf()
                .setAppName("Activity classifier")
                .set("spark.cassandra.connection.host", "127.0.0.1")
                .setMaster("local[*]");

        Logger.getLogger("org").setLevel(Level.DEBUG);
        Logger.getLogger("akka").setLevel(Level.DEBUG);
        Logger log = Logger.getLogger("org");

        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // retrieve data from Cassandra and create an CassandraRDD
        CassandraJavaRDD<CassandraRow> cassandraRowsRDD = javaFunctions(sc).cassandraTable("accelerometer", "user_accel_data_test");
        JavaRDD<String> users = cassandraRowsRDD.select("user_id").distinct().map(CassandraRow::toMap).map(entry -> entry.get("user_id").toString()).cache();

        Set<String> user_ids = new HashSet<>(users.collect());
        log.debug(">>>> total users: " + user_ids.size());
        List<SamplePoint> sampleList = new ArrayList<>();
        for (String user_id : user_ids) {
            log.debug("Processing user id: " + user_id);
            // create bucket of sorted data by ascending timestamp by (user, activity)
            JavaRDD<Long> times = cassandraRowsRDD.select("timestamp")
                    .where("user_id=?", user_id)
                    .withAscOrder()
                    .map(CassandraRow::toMap)
                    .map(entry -> (long) entry.get("timestamp"))
                    .cache();

            JavaRDD<CassandraRow> dataTotal = cassandraRowsRDD.select("timestamp", "x", "y", "z")
                    .where("user_id=?", user_id)
                    .withAscOrder().cache();
            log.debug(">> Data row count: " + times.count());

            // if data

            if (100 < times.count()) {

                //////////////////////////////////////////////////////////////////////////////
                // PREPARE THE DATA: define the windows for each activity records intervals //
                //////////////////////////////////////////////////////////////////////////////
                List<Long[]> intervals = PrepareData.defineWindows(times);
                for (Long[] interval : intervals) {

                    log.debug("Interval Start: " + interval[0] + ", Interval End: " + interval[1] + ", Number of windows: " + interval[2]);
                    for (int j = 0; j <= interval[2]; j++) {

                        JavaRDD<CassandraRow> data = PrepareData.getDataIntervalData(dataTotal, interval[0], j);

                        if (data.count() > 0) {
                            // transform into double array
                            JavaRDD<double[]> doubles = DataManager.toDouble(data);
                            // transform into vector without timestamp
                            JavaRDD<Vector> vectors = doubles.map(Vectors::dense);
                            // data with only timestamp and acc
                            JavaRDD<long[]> timestamp = DataManager.withTimestamp(data);

                            ////////////////////////////////////////
                            // extract features from this windows //
                            ////////////////////////////////////////
                            ExtractFeature extractFeature = new ExtractFeature(vectors);

                            // the average acceleration
                            double[] mean = extractFeature.computeAvgAcc();

                            // the variance
                            double[] variance = extractFeature.computeVariance();

                            // the average absolute difference
                            double[] avgAbsDiff = computeAvgAbsDifference(doubles, mean);

                            // the average resultant acceleration
                            double resultant = computeResultantAcc(doubles);

                            // the average time between peaks
                            double avgTimePeak = extractFeature.computeAvgTimeBetweenPeak(timestamp);

                            sampleList.add(new SamplePoint(user_id, mean, variance, avgAbsDiff, resultant, avgTimePeak));

                        }
                    }
                }
            }
        }

        if (sampleList.size() > 0) {
            System.out.println("Total Samples: " + sampleList.size());
            DataManager.saveDataToMySQL(sampleList);
        } */
    }

    public static void lastSeen() {
        try {
            String content = new Scanner(new File(lastReadFile)).next();
            System.out.println(Long.parseLong(content));
        } catch(Exception e) {
            System.out.println(-1);
        }
    }


    public static void writeLastSeen(Long timestamp) {
        File file = new File(lastReadFile);
        try {
            if(!file.exists())
                file.createNewFile();
            PrintWriter writer = new PrintWriter(file);
            writer.write(timestamp.toString());
            writer.close();
        } catch (Exception e) {
            System.out.println("Could not create file");
        }
    }
}
