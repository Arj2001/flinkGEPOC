package com.example;

public class DataStreamMain {

    public static void main(String[] args) {

        DataStreamJob dataStreamJob = new DataStreamJob();
        try {
            dataStreamJob.job();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
