package com.actitracker.model;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by jainikkumar on 3/5/16.
 */
public class SamplePoint {
    String user_id;
    double[] mean;
    double[] variance;
    double[] avgAbsDiff;
    double resultant;
    double avgTimePeak;
    double distance;
    long startTimestamp;

    public SamplePoint(String user_id, double[] mean, double[] variance, double[] avgAbsDiff, double resultant, double avgTimePeak, double distance, long startTimestamp) {
        this.user_id = user_id;
        this.mean = mean;
        this.variance = variance;
        this.avgAbsDiff = avgAbsDiff;
        this.resultant = resultant;
        this.avgTimePeak = avgTimePeak;
        this.distance = distance;
        this.startTimestamp = startTimestamp;
    }

    public String getUser_id() {
        return user_id;
    }

    public double[] getMean() {
        return mean;
    }

    public double[] getVariance() {
        return variance;
    }

    public double[] getAvgAbsDiff() {
        return avgAbsDiff;
    }

    public double getResultant() {
        return resultant;
    }

    public double getAvgTimePeak() {
        return avgTimePeak;
    }

    public void setUser_id(String user_id) {
        this.user_id = user_id;
    }

    public void setMean(double[] mean) {
        this.mean = mean;
    }

    public void setVariance(double[] variance) {
        this.variance = variance;
    }

    public void setAvgAbsDiff(double[] avgAbsDiff) {
        this.avgAbsDiff = avgAbsDiff;
    }

    public void setResultant(double resultant) {
        this.resultant = resultant;
    }

    public void setAvgTimePeak(double avgTimePeak) {
        this.avgTimePeak = avgTimePeak;
    }

    public long getStartTimestamp() {
        return startTimestamp;
    }

    public void getStartTimestamp(long startTimestamp) {
        this.startTimestamp = startTimestamp;
    }

    public List<Double> getFeatures() {
        Double[] features = new Double[]{
                mean[0],
                mean[1],
                mean[2],
                variance[0],
                variance[1],
                variance[2],
                avgAbsDiff[0],
                avgAbsDiff[1],
                avgAbsDiff[2],
                resultant,
                avgTimePeak,
                distance
        };
        return new ArrayList<>(Arrays.asList(features));
    }
}
