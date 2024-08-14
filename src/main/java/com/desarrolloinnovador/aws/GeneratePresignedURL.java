package com.desarrolloinnovador.aws;

import com.amazonaws.HttpMethod;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class GeneratePresignedURL implements RequestHandler<Object, List<String>> {
    private final AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();

    @Override
    public List<String> handleRequest(Object input, Context context) {

        String bucketName = "employee-test-artifact";

        Date expiration = new Date();
        long expTimeMillis = expiration.getTime();
        expTimeMillis += 1000 * 60 * 60; // 1 hour
        expiration.setTime(expTimeMillis);

        var signedUrls= new ArrayList<String>();

        ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(bucketName);
        ListObjectsV2Result result;

        do {
            result = s3Client.listObjectsV2(req);

            for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
                context.getLogger().log(" - " + objectSummary.getKey() + "  " +
                        "(size = " + objectSummary.getSize() + ")");


                GeneratePresignedUrlRequest generatePresignedUrlRequest =
                        new GeneratePresignedUrlRequest(bucketName, objectSummary.getKey())
                                .withMethod(HttpMethod.GET)
                                .withExpiration(expiration);

                URL url = s3Client.generatePresignedUrl(generatePresignedUrlRequest);
                context.getLogger().log(url.toString());
                signedUrls.add(url.toString());

            }
            req.setContinuationToken(result.getNextContinuationToken());
        } while (result.isTruncated());

        return signedUrls;
    }
}
