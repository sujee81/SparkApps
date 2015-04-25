package com.sparkexpert;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.StatCounter;

public class Main implements Serializable {

    private static final org.apache.log4j.Logger LOGGER = org.apache.log4j.Logger.getLogger(Main.class);

    private static final JavaSparkContext sc =
            new JavaSparkContext(new SparkConf().setAppName("SparkStats").setMaster("local[*]"));

    public static void main(String[] args) {
        //Sample test data - All numbers from 1 to 99999
        List<Double> testData = IntStream.range(1, 100000).mapToDouble(d -> d).collect(ArrayList::new, ArrayList::add,
                                                                                     ArrayList::addAll);

        JavaDoubleRDD rdd = sc.parallelizeDoubles(testData);

        LOGGER.info("Mean: " + rdd.mean());

        //For efficiency, use StatCounter if more than one stats are required.
        StatCounter statCounter = rdd.stats();

        LOGGER.info("Using StatCounter");
        LOGGER.info("Count:    " + statCounter.count());
        LOGGER.info("Min:      " + statCounter.min());
        LOGGER.info("Max:      " + statCounter.max());
        LOGGER.info("Sum:      " + statCounter.sum());
        LOGGER.info("Mean:     " + statCounter.mean());
        LOGGER.info("Variance: " + statCounter.variance());
        LOGGER.info("Stdev:    " + statCounter.stdev());
    }
}
