import java.io.File;
import java.sql.Timestamp;
import java.util.List;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.SDKGlobalConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.ibm.oauth.BasicIBMOAuthCredentials;

public class CosExample
{

    private static AmazonS3 _s3Client,_s3Client_writer;

    /**
     * @param args
     */
    public static void main(String[] args)
    {

        SDKGlobalConfiguration.IAM_ENDPOINT = "https://iam.bluemix.net/oidc/token";

        String bucketName = "mybucketcptb";
        String api_key = "HDfkMjIbgMH1rALTfvNCQjtYSTt7QKSkv3ylMNXyl25e";
        String service_instance_id = "crn:v1:bluemix:public:iam-identity::a/566dc2a364a1aee9c74bcf1067594328::serviceid:ServiceId-539f546c-6e39-4987-95ad-3528ffb88074";
        String api_key_writer = "NPWAOf39u-AK-3V8fkDQcoBMDa-oQNBqSrz0meVGqI-K";
        String service_instance_id_writer = "crn:v1:bluemix:public:iam-identity::a/566dc2a364a1aee9c74bcf1067594328::serviceid:ServiceId-823130b7-3093-4d6c-adf9-aa06774a59dd";

        
        // String endpoint_url ="https://s3.eu-geo.objectstorage.softlayer.net";
       String endpoint_url = "https://s3-api.us-geo.objectstorage.softlayer.net";
        String location = "us-geo";

        System.out.println("Current time: " + new Timestamp(System.currentTimeMillis()).toString());
        _s3Client = createClient(api_key, service_instance_id, endpoint_url, location);
        _s3Client_writer = createClient(api_key_writer, service_instance_id_writer, endpoint_url, location);
       listObjects(bucketName, _s3Client);
       addObjects(bucketName,_s3Client_writer);
        //listBuckets(_s3Client);
    }

    /**
     * @param bucketName
     * @param clientNum
     * @param api_key
     *            (or access key)
     * @param service_instance_id
     *            (or secret key)
     * @param endpoint_url
     * @param location
     * @return AmazonS3
     */
    public static AmazonS3 createClient(String api_key, String service_instance_id, String endpoint_url, String location)
    {
        AWSCredentials credentials;
        if (endpoint_url.contains("objectstorage.softlayer.net")) {
            credentials = new BasicIBMOAuthCredentials(api_key, service_instance_id);
        } else {
            String access_key = api_key;
            String secret_key = service_instance_id;
            credentials = new BasicAWSCredentials(access_key, secret_key);
        }
        ClientConfiguration clientConfig = new ClientConfiguration().withRequestTimeout(5000);
        clientConfig.setUseTcpKeepAlive(true);

        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withEndpointConfiguration(new EndpointConfiguration(endpoint_url, location)).withPathStyleAccessEnabled(true)
                .withClientConfiguration(clientConfig).build();
        return s3Client;
    }

    /**
     * @param bucketName
     * @param s3Client
     */
    public static void listObjects(String bucketName, AmazonS3 s3Client)
    {
        System.out.println("Listing objects in bucket " + bucketName);
        ObjectListing objectListing = s3Client.listObjects(new ListObjectsRequest().withBucketName(bucketName));
        for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
            System.out.println(" - " + objectSummary.getKey() + "  " + "(size = " + objectSummary.getSize() + ")");
        }
        System.out.println();
    }
    
    public static void addObjects(String bucketName, AmazonS3 s3Client)
    {
        System.out.println("Insert a file objects in bucket " + bucketName);
        PutObjectResult objectListing = s3Client.putObject(bucketName, "file_test", new File("/Users/suneth/test/my_test.txt"));
        System.out.println("done" + objectListing.getVersionId());
    }

    /**
     * @param s3Client
     */
    public static void listBuckets(AmazonS3 s3Client)
    {
        System.out.println("Listing buckets");
        final List<Bucket> bucketList = _s3Client.listBuckets();
        for (final Bucket bucket : bucketList) {
            System.out.println(bucket.getName());
        }
        System.out.println();
    }

}