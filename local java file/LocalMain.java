
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.PlacementType;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;


public class LocalMain {

	public static final String propertiesFilePath = "prop.properties";
	public static String inputPath = "s3n://razbucket2/input/";
	public static final String outputPath = "s3n://razbucket2/output/";
	public static final String stepOnePath = "s3n://razbucket2/jars/StepOne.jar";
	public static final String stepTwoPath = "s3n://razbucket2/jars/StepTwo.jar";
	public static final String stepThreePath = "s3n://razbucket2/jars/StepThree.jar";
	public static final String stepFourPath = "s3n://razbucket2/jars/StepFour.jar";
	public static final String logLocation = "s3n://razbucket2/logs/";

	public static void main(String[] args) throws FileNotFoundException, IOException {
		
		if(args[2].equals("eng"))
            inputPath = "s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/2gram/data";
        else
            inputPath = "s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data";

		AWSCredentials credentials = new PropertiesCredentials(new FileInputStream(propertiesFilePath));
		AmazonElasticMapReduce mapReduce = new AmazonElasticMapReduceClient(credentials);

		HadoopJarStepConfig hadoopJarStep1 = new HadoopJarStepConfig().withJar(stepOnePath)
				.withMainClass("Main.StepOne")
				.withArgs(inputPath, args[2], args[3]);

		HadoopJarStepConfig hadoopJarStep2 = new HadoopJarStepConfig().withJar(stepTwoPath)
				.withMainClass("Main.StepTwo");

		HadoopJarStepConfig hadoopJarStep3 = new HadoopJarStepConfig().withJar(stepThreePath)
				.withMainClass("Main.StepThree");

		HadoopJarStepConfig hadoopJarStep4 = new HadoopJarStepConfig().withJar(stepFourPath)
				.withMainClass("Main.StepFour")
				.withArgs(outputPath, args[0], args[1]);


		StepConfig stepConfig1 = new StepConfig()
				.withName("job 1")
				.withHadoopJarStep(hadoopJarStep1)
				.withActionOnFailure("TERMINATE_JOB_FLOW");

		StepConfig stepConfig2 = new StepConfig()
				.withName("job 2")
				.withHadoopJarStep(hadoopJarStep2)
				.withActionOnFailure("TERMINATE_JOB_FLOW");


		StepConfig stepConfig3 = new StepConfig()
				.withName("job 3")
				.withHadoopJarStep(hadoopJarStep3)
				.withActionOnFailure("TERMINATE_JOB_FLOW");


		StepConfig stepConfig4 = new StepConfig()
				.withName("job 4")
				.withHadoopJarStep(hadoopJarStep4)
				.withActionOnFailure("TERMINATE_JOB_FLOW");

		JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
				.withInstanceCount(15)
				.withMasterInstanceType(InstanceType.M1Large.toString())
				.withSlaveInstanceType(InstanceType.M1Large.toString())
				.withHadoopVersion("2.2.0").withEc2KeyName("shahard92")
				.withKeepJobFlowAliveWhenNoSteps(false)
				.withPlacement(new PlacementType("us-east-1a"));

		RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
				.withServiceRole("EMR_DefaultRole")
				.withJobFlowRole("EMR_EC2_DefaultRole")
				.withReleaseLabel("emr-4.7.0")
				.withName("CollocationExtraction")
				.withInstances(instances)
				.withSteps(stepConfig1,stepConfig2,stepConfig3,stepConfig4)
				.withLogUri(logLocation);

		RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
		String jobFlowId = runJobFlowResult.getJobFlowId();
		System.out.println("Ran job flow with id: " + jobFlowId);

	}

}