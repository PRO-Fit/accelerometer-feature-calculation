package com.actitracker.data;


import com.datastax.spark.connector.japi.CassandraRow;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PrepareData {

    /**
     * identify Jump
     */
    public static JavaPairRDD<Long[], Long> boudariesDiff(JavaRDD<Long> timestamps, Long firstElement, Long lastElement) {

        JavaRDD<Long> firstRDD = timestamps.filter(record -> record > firstElement);
        JavaRDD<Long> secondRDD = timestamps.filter(record -> record < lastElement);

        // define periods of recording
        return firstRDD.zip(secondRDD)
                .mapToPair(pair -> new Tuple2<>(new Long[]{pair._1, pair._2}, pair._1 - pair._2));
    }

    public static JavaPairRDD<Long, Long> defineJump(JavaPairRDD<Long[], Long> tsBoundaries) {

        return tsBoundaries.filter(pair -> pair._2 > Constants.jump)
                .mapToPair(pair -> new Tuple2<>(pair._1[1], pair._1[0]));
    }

    // (min, max)
    public static List<Long[]> defineInterval(JavaPairRDD<Long, Long> tsJump, Long firstElement, Long lastElement, long windows) {
        List<Long> flatten = tsJump.flatMap(pair -> Arrays.asList(pair._1, pair._2))
                .sortBy(t -> t, true, 1)
                .collect();

        int size = flatten.size(); // always even

        List<Long[]> results = new ArrayList<>();
        // init condition
        if (size > 0) {
            results.add(new Long[]{firstElement, flatten.get(0), (long) Math.round((flatten.get(0) - firstElement) / windows)});
            for (int i = 1; i < size - 1; i += 2) {
                results.add(new Long[]{flatten.get(i), flatten.get(i + 1), (long) Math.round((flatten.get(i + 1) - flatten.get(i)) / windows)});
            }
            // end condition
            results.add(new Long[]{flatten.get(size - 1), lastElement, (long) Math.round((lastElement - flatten.get(size - 1)) / windows)});

        } else {
            results.add(new Long[]{firstElement, lastElement, (long) Math.round((lastElement - firstElement) / windows)});
        }

        return results;
    }

    public static List<Long[]> defineWindows(JavaRDD<Long> times) {
        // first find jumps to define the continuous periods of data
        Long firstElement = times.first();
        Long lastElement = times.sortBy(time -> time, false, 1).first();

        // compute the difference between each timestamp
        JavaPairRDD<Long[], Long> tsBoundariesDiff = PrepareData.boudariesDiff(times, firstElement, lastElement);

        // define periods of recording
        // if the difference is greater than 100 000 000, it must be different periods of recording
        // ({min_boundary, max_boundary}, max_boundary - min_boundary > 100 000 000)
        JavaPairRDD<Long, Long> jumps = PrepareData.defineJump(tsBoundariesDiff);

        // Now define the intervals
        return PrepareData.defineInterval(jumps, firstElement, lastElement, Constants.interval);
    }

    /**
     * Get data slices based on window interval.
     * @param data
     * @param interval
     * @param j
     * @return JavaRDD<CassandraRow> - interval data from the jump
     */
    public static JavaRDD<CassandraRow> getDataIntervalData(JavaRDD<CassandraRow> data, long interval, int j) {
        return data.filter(raw ->
                Long.valueOf(raw.getString("timestamp")) < interval + (j + 1) * Constants.interval && Long.valueOf(raw.getString("timestamp")) > interval + j * Constants.interval
        );
    }

}
