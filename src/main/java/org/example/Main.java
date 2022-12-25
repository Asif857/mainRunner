package org.example;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.*;

import java.io.*;

public class Main {
    private static AWSCredentials returnCreds() throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader("/home/assiph/Desktop/key/credentials"));
        String line;
        String accessKey = "";
        String secretKey = "";
        String sessionToken = "";
        while ((line = reader.readLine()) != null) {
            String[] parts = line.split("=");
            if (parts.length == 2) {
                String key = parts[0];
                String value = parts[1];
                if (key.equals("accessKey")) {
                    accessKey = value;
                } else if (key.equals("secretKey")) {
                    secretKey = value;
                } else if (key.equals("sessionToken")) {
                    sessionToken = value;
                }
            }
        }
        reader.close();
        return (new BasicSessionCredentials(accessKey, secretKey, sessionToken));
    }
    public static void main(String[] args) throws IOException {
        AWSCredentials credentials = returnCreds();
        AmazonElasticMapReduce mapReduce = new
                AmazonElasticMapReduceClient(credentials);
        HadoopJarStepConfig hadoopJarStepFirstMapReduce = new HadoopJarStepConfig()
                .withJar("s3://second-amazon-project/jars/FirstMapReduce.jar") // This should be a full map reduce application.
                .withMainClass("Main") //contains the main class in the jar.
                .withArgs("s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/3gram/data", "s3://second-amazon-project/output/output");
        StepConfig stepConfigFirst = new StepConfig()
                .withName("EMR1")
                .withHadoopJarStep(hadoopJarStepFirstMapReduce)
                .withActionOnFailure("TERMINATE_JOB_FLOW");
        HadoopJarStepConfig hadoopJarStepSecondMapReduce = new HadoopJarStepConfig()
                .withJar("s3://second-amazon-project/jars/SecondMapReduce.jar") // This should be a full map reduce application.
                .withMainClass("Main") //contains the main class in the jar.
                .withArgs("s3://second-amazon-project/output/output", "s3://second-amazon-project/output/output2");
        StepConfig stepConfigSecond = new StepConfig()
                .withName("EMR2")
                .withHadoopJarStep(hadoopJarStepSecondMapReduce)
                .withActionOnFailure("TERMINATE_JOB_FLOW");
        HadoopJarStepConfig hadoopJarStepThirdMapReduce = new HadoopJarStepConfig()
                .withJar("s3://second-amazon-project/jars/ThirdMapReduce.jar") // This should be a full map reduce application.
                .withMainClass("Main") //contains the main class in the jar.
                .withArgs("s3://second-amazon-project/output/output2", "s3://second-amazon-project/output/output3");
        StepConfig stepConfigThird = new StepConfig()
                .withName("EMR3")
                .withHadoopJarStep(hadoopJarStepThirdMapReduce)
                .withActionOnFailure("TERMINATE_JOB_FLOW");
        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(5)
                .withHadoopVersion("3.3.2")
                .withMasterInstanceType(InstanceType.M4Large.toString())
                .withSlaveInstanceType(InstanceType.M4Large.toString())
                .withEc2KeyName("keyForAMI")
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType("us-east-1a"));
        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName("tryjob")
                .withInstances(instances)
                .withReleaseLabel("emr-5.30.1")
                .withSteps(stepConfigFirst,stepConfigSecond,stepConfigThird) // stepConfigSecond,stepConfigThird
                .withLogUri("s3://second-amazon-project/logs/logs");
        runFlowRequest.setServiceRole("EMR_DefaultRole");
        runFlowRequest.setJobFlowRole("EMR_EC2_DefaultRole");
        RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("Ran job flow with id: " + jobFlowId);
    }
}