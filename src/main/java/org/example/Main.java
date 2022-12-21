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
                .withArgs("s3://second-amazon-project/input/input", "s3://second-amazon-project/output/output");
        StepConfig stepConfigFirst = new StepConfig()
                .withName("step1")
                .withHadoopJarStep(hadoopJarStepFirstMapReduce)
                .withActionOnFailure("TERMINATE_JOB_FLOW");
     /*   HadoopJarStepConfig hadoopJarStepSecondMapReduce = new HadoopJarStepConfig()
                .withJar("s3n://yourbucket/yourfile.jar") // This should be a full map reduce application.
                .withMainClass("Main") //contains the main class in the jar.
                .withArgs("s3n://yourbucket/input/", "s3n://yourbucket/output/");
        StepConfig stepConfigSecond = new StepConfig()
                .withName("stepname")
                .withHadoopJarStep(hadoopJarStepSecondMapReduce)
                .withActionOnFailure("TERMINATE_JOB_FLOW");
        HadoopJarStepConfig hadoopJarStepThirdMapReduce = new HadoopJarStepConfig()
                .withJar("s3n://yourbucket/yourfile.jar") // This should be a full map reduce application.
                .withMainClass("Main") //contains the main class in the jar.
                .withArgs("s3n://yourbucket/input/", "s3n://yourbucket/output/");
        StepConfig stepConfigThird = new StepConfig()
                .withName("stepname")
                .withHadoopJarStep(hadoopJarStepThirdMapReduce)
                .withActionOnFailure("TERMINATE_JOB_FLOW"); */
        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(2)
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
                .withSteps(stepConfigFirst) // stepConfigSecond,stepConfigThird
                .withLogUri("s3://second-amazon-project/logs/logs");
        runFlowRequest.setServiceRole("EMR_DefaultRole");
        runFlowRequest.setJobFlowRole("EMR_EC2_DefaultRole");
        RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("Ran job flow with id: " + jobFlowId);
    }
}