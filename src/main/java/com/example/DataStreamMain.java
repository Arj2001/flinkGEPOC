package com.example;

public class DataStreamMain {

    public static void main(String[] args) {

        DataStreamJob dataStreamJob = new DataStreamJob();
        try {
//            String pythonInterpreter = "python3";
//            // Path to the Python script you want to execute. Update it with the correct path.
//            String pythonScript = "app.py";
//
//            // Create the ProcessBuilder instance with the Python command and script arguments
//            ProcessBuilder processBuilder = new ProcessBuilder(pythonInterpreter, pythonScript);
//
//            // Start the Python process
//            Process process = processBuilder.start();
            dataStreamJob.job();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
