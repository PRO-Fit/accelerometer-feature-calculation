package com.actitracker.data;


import com.actitracker.job.CalculateFeature;
import com.actitracker.model.SamplePoint;
import com.datastax.spark.connector.japi.CassandraRow;
import org.apache.spark.api.java.JavaRDD;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public class DataManager {

    public static JavaRDD<double[]> toDouble(JavaRDD<CassandraRow> data) {

        // first transform CassandraRDD into a RDD<Map>
        return data.map(CassandraRow::toMap)
                // then build  a double array from the RDD<Map>
                .map(entry -> new double[]{(double) entry.get("x"), (double) entry.get("y"), (double) entry.get("z")});
    }

    public static JavaRDD<long[]> withTimestamp(JavaRDD<CassandraRow> data) {

        // first transform CassandraRDD into a RDD<Map>
        return data.map(CassandraRow::toMap)
                // then build  a double array from the RDD<Map>
                .map(entry -> new long[]{(long) entry.get("timestamp"), ((Double) entry.get("y")).longValue()});
    }

    private static Connection getConnection() {
        Connection conn = null;
        try{
            conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/profit", "root", "admin");
            System.out.println("Connection successful");
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        return conn;
    }

    public static void saveDataToMySQL(List<SamplePoint> sampleList) {
        Connection conn = getConnection();
        String query = "INSERT INTO t_user_activity_features\n" +
                "(user_id, mean0, mean1, mean2, variance0, variance1, variance2, avgabsdiff0, avgabsdiff1, " +
                "avgabsdiff2, resultant, avgtimepeak, distance, start_timestamp)" +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        try {
            for(SamplePoint sample: sampleList) {
                String user_id = sample.getUser_id();
                List<Double> features = sample.getFeatures();
                PreparedStatement stmt = conn.prepareStatement(query);
                stmt.setString(1, user_id);
                for(int i=2; i <= features.size()+1; i++) {
                    stmt.setDouble(i, features.get(i-2));
                }
                stmt.setLong(features.size()+2, sample.getStartTimestamp());
                stmt.execute();
            }
            conn.close();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
}
