/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.fs.s3hadoop;

import org.apache.flink.fs.s3.common.writer.S3AccessHelper;

import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.UploadPartResult;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.WriteOperationHelper;
import org.apache.hadoop.fs.s3a.impl.PutObjectOptions;
import org.apache.hadoop.fs.s3a.impl.write.WriteObjectFlags;
import org.apache.hadoop.fs.s3a.statistics.S3AStatisticsContext;
import org.apache.hadoop.fs.statistics.DurationTrackerFactory;
import org.apache.hadoop.fs.store.audit.AuditSpan;
import org.apache.hadoop.fs.store.audit.AuditSpanSource;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** An implementation of the {@link S3AccessHelper} for the Hadoop S3A filesystem. */
public class HadoopS3AccessHelper implements S3AccessHelper {

    private final S3AFileSystem s3a;

    private final InternalWriteOperationHelper s3accessHelper;

    public HadoopS3AccessHelper(S3AFileSystem s3a, Configuration conf) {
        checkNotNull(s3a);
        this.s3a = s3a;
        this.s3accessHelper =
                new InternalWriteOperationHelper(
                        s3a,
                        checkNotNull(conf),
                        s3a.createStoreContext().getInstrumentation(),
                        s3a.getAuditSpanSource(),
                        s3a.getActiveAuditSpan(),
                        new S3AccessHelperCallbacks());
    }

    @Override
    public String startMultiPartUpload(String key) throws IOException {
        // Create minimal PutObjectOptions for Hadoop 3.4.2 compatibility
        PutObjectOptions putOptions =
                new PutObjectOptions(
                        false, // multipartUpload
                        null, // storageClass
                        Collections.emptyMap(), // headers
                        EnumSet.noneOf(WriteObjectFlags.class), // flags
                        null // customUserAgent
                        );
        return s3accessHelper.initiateMultiPartUpload(key, putOptions);
    }

    @Override
    public UploadPartResult uploadPart(
            String key, String uploadId, int partNumber, File inputFile, long length)
            throws IOException {
        try {
            // Create AWS SDK v2 UploadPartRequest
            software.amazon.awssdk.services.s3.model.UploadPartRequest request =
                    software.amazon.awssdk.services.s3.model.UploadPartRequest.builder()
                            .bucket(s3a.getBucket())
                            .key(key)
                            .uploadId(uploadId)
                            .partNumber(partNumber)
                            .contentLength(length)
                            .build();

            // Create RequestBody from file
            RequestBody requestBody = RequestBody.fromFile(inputFile);

            // Use our callback to perform the upload via Hadoop's WriteOperationHelper
            software.amazon.awssdk.services.s3.model.UploadPartResponse response =
                    s3accessHelper.uploadPart(
                            request, requestBody, s3a.createStoreContext().getInstrumentation());

            // Convert AWS SDK v2 response to AWS SDK v1 response for interface compatibility
            UploadPartResult result = new UploadPartResult();
            result.setPartNumber(partNumber); // Use the original part number
            result.setETag(response.eTag());
            return result;

        } catch (Exception e) {
            throw new IOException("Failed to upload part " + partNumber + " for key " + key, e);
        }
    }

    @Override
    public PutObjectResult putObject(String key, File inputFile) throws IOException {
        try {
            // Create minimal PutObjectOptions
            PutObjectOptions putOptions =
                    new PutObjectOptions(
                            false, // multipartUpload
                            null, // storageClass
                            Collections.emptyMap(), // headers
                            EnumSet.noneOf(WriteObjectFlags.class), // flags
                            null // customUserAgent
                            );

            // Create AWS SDK v2 PutObjectRequest
            software.amazon.awssdk.services.s3.model.PutObjectRequest request =
                    software.amazon.awssdk.services.s3.model.PutObjectRequest.builder()
                            .bucket(s3a.getBucket())
                            .key(key)
                            .contentLength(inputFile.length())
                            .build();

            // Alternative approach: Use S3AFileSystem's create() method to write the file
            // This is simpler and doesn't require dealing with internal DataBlock classes
            org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path("/" + key);

            try (java.io.FileInputStream fis = new java.io.FileInputStream(inputFile);
                    org.apache.hadoop.fs.FSDataOutputStream outputStream = s3a.create(hadoopPath)) {

                // Copy file content to S3
                byte[] buffer = new byte[8192];
                int bytesRead;
                while ((bytesRead = fis.read(buffer)) != -1) {
                    outputStream.write(buffer, 0, bytesRead);
                }
            }

            // Get the object metadata to retrieve the ETag
            HeadObjectResponse headResponse = s3a.getObjectMetadata(hadoopPath);

            // Convert AWS SDK v2 response to AWS SDK v1 response for interface compatibility
            PutObjectResult result = new PutObjectResult();
            result.setETag(headResponse.eTag());
            return result;

        } catch (Exception e) {
            throw new IOException("Failed to put object for key " + key, e);
        }
    }

    @Override
    public CompleteMultipartUploadResult commitMultiPartUpload(
            String destKey,
            String uploadId,
            List<PartETag> partETags,
            long length,
            AtomicInteger errorCount)
            throws IOException {
        try {
            // Convert AWS SDK v1 PartETag list to AWS SDK v2 CompletedPart list
            List<CompletedPart> completedParts =
                    partETags.stream()
                            .map(
                                    partETag ->
                                            CompletedPart.builder()
                                                    .partNumber(partETag.getPartNumber())
                                                    .eTag(partETag.getETag())
                                                    .build())
                            .collect(java.util.stream.Collectors.toList());

            // Create minimal PutObjectOptions
            PutObjectOptions putOptions =
                    new PutObjectOptions(
                            false, // multipartUpload
                            null, // storageClass
                            Collections.emptyMap(), // headers
                            EnumSet.noneOf(WriteObjectFlags.class), // flags
                            null // customUserAgent
                            );

            // Use WriteOperationHelper to complete the multipart upload
            software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse response =
                    s3accessHelper.completeMPUwithRetries(
                            destKey, uploadId, completedParts, length, errorCount, putOptions);

            // Convert AWS SDK v2 response to AWS SDK v1 response for interface compatibility
            CompleteMultipartUploadResult result = new CompleteMultipartUploadResult();
            result.setBucketName(s3a.getBucket());
            result.setKey(destKey);
            result.setETag(response.eTag());
            result.setLocation(response.location());
            return result;

        } catch (Exception e) {
            throw new IOException("Failed to complete multipart upload for key " + destKey, e);
        }
    }

    @Override
    public boolean deleteObject(String key) throws IOException {
        return s3a.delete(new org.apache.hadoop.fs.Path('/' + key), false);
    }

    @Override
    public long getObject(String key, File targetLocation) throws IOException {
        long numBytes = 0L;
        try (final OutputStream outStream = new FileOutputStream(targetLocation);
                final org.apache.hadoop.fs.FSDataInputStream inStream =
                        s3a.open(new org.apache.hadoop.fs.Path('/' + key))) {
            final byte[] buffer = new byte[32 * 1024];

            int numRead;
            while ((numRead = inStream.read(buffer)) != -1) {
                outStream.write(buffer, 0, numRead);
                numBytes += numRead;
            }
        }

        // some sanity checks
        if (numBytes != targetLocation.length()) {
            throw new IOException(
                    String.format(
                            "Error recovering writer: "
                                    + "Downloading the last data chunk file gives incorrect length. "
                                    + "File=%d bytes, Stream=%d bytes",
                            targetLocation.length(), numBytes));
        }

        return numBytes;
    }

    @Override
    public ObjectMetadata getObjectMetadata(String key) throws IOException {
        try {
            // Use S3AFileSystem to get the head object response (Hadoop 3.4.2 returns
            // HeadObjectResponse)
            HeadObjectResponse headResponse =
                    s3a.getObjectMetadata(new org.apache.hadoop.fs.Path("/" + key));

            // Convert AWS SDK v2 HeadObjectResponse to AWS SDK v1 ObjectMetadata for interface
            // compatibility
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentLength(headResponse.contentLength());
            metadata.setContentType(headResponse.contentType());
            // Note: ObjectMetadata in AWS SDK v1 doesn't have setETag() method - ETag is read-only
            if (headResponse.lastModified() != null) {
                metadata.setLastModified(java.util.Date.from(headResponse.lastModified()));
            }

            // Copy user metadata if present
            if (headResponse.metadata() != null) {
                headResponse.metadata().forEach(metadata::addUserMetadata);
            }

            return metadata;

        } catch (Exception e) {
            throw new IOException("Failed to get object metadata for key " + key, e);
        }
    }

    /**
     * Full implementation of WriteOperationHelperCallbacks for Hadoop 3.4.2 compatibility. This
     * implementation uses the S3AFileSystem's internal S3 client to perform actual S3 operations.
     */
    private final class S3AccessHelperCallbacks
            implements WriteOperationHelper.WriteOperationHelperCallbacks {

        private final S3Client s3Client;

        public S3AccessHelperCallbacks() {
            // Extract the S3 client from S3AFileSystem using reflection if needed
            // For now, we'll use a direct approach - WriteOperationHelper will call these methods
            // when it needs to perform S3 operations
            this.s3Client = getS3ClientFromFileSystem();
        }

        @Override
        public software.amazon.awssdk.services.s3.model.UploadPartResponse uploadPart(
                software.amazon.awssdk.services.s3.model.UploadPartRequest request,
                RequestBody body,
                DurationTrackerFactory durationTrackerFactory) {
            // Perform the actual S3 uploadPart operation
            return s3Client.uploadPart(request, body);
        }

        @Override
        public software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse
                completeMultipartUpload(
                        software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest
                                request) {
            // Perform the actual S3 completeMultipartUpload operation
            return s3Client.completeMultipartUpload(request);
        }

        @Override
        public void finishedWrite(String key, long length, PutObjectOptions putObjectOptions) {
            // Callback for post-write operations - can be used for metrics, cleanup, etc.
            // For now, we'll leave this empty as it's optional
        }

        /**
         * Get the S3 client from the S3AFileSystem. In Hadoop 3.4.2, the S3AFileSystem uses an
         * internal S3 client that we can access.
         */
        private S3Client getS3ClientFromFileSystem() {
            try {
                // Use reflection to access the internal S3 client from S3AFileSystem
                java.lang.reflect.Field s3Field = s3a.getClass().getDeclaredField("s3");
                s3Field.setAccessible(true);
                return (S3Client) s3Field.get(s3a);
            } catch (Exception e) {
                // If reflection fails, try alternative field names
                try {
                    java.lang.reflect.Field s3ClientField =
                            s3a.getClass().getDeclaredField("s3Client");
                    s3ClientField.setAccessible(true);
                    return (S3Client) s3ClientField.get(s3a);
                } catch (Exception e2) {
                    // If all reflection attempts fail, throw a descriptive error
                    throw new RuntimeException(
                            "Failed to access S3 client from S3AFileSystem. "
                                    + "This may be due to changes in Hadoop's internal structure.",
                            e2);
                }
            }
        }
    }

    /**
     * Internal {@link WriteOperationHelper} that is wrapped so that it only exposes the
     * functionality we need for the {@link S3AccessHelper}.
     */
    private static final class InternalWriteOperationHelper extends WriteOperationHelper {

        InternalWriteOperationHelper(
                S3AFileSystem owner,
                Configuration conf,
                S3AStatisticsContext statisticsContext,
                AuditSpanSource auditSpanSource,
                AuditSpan auditSpan,
                WriteOperationHelperCallbacks callbacks) {
            super(owner, conf, statisticsContext, auditSpanSource, auditSpan, callbacks);
        }
    }
}
