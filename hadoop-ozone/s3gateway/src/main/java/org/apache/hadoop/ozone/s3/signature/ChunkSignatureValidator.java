/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.s3.signature;

import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.SIGNATURE_DOES_NOT_MATCH;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.apache.hadoop.ozone.om.AWSV4AuthValidator;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;
import org.apache.hadoop.ozone.s3.util.S3Utils;
import org.apache.kerby.util.Hex;

/**
 * Validator for validating the signature of each chunk in the chunked upload with signature.
 */
public class ChunkSignatureValidator {

  /**
   * MessageDigest instance for calculating the hash of the chunk payload for signature verification.
   */
  private final MessageDigest messageDigest;

  /**
   * Secret key for calculating the signature of the chunk payload for signature verification.
   */
  private final byte[] derivedKey;

  /**
   * Previous chunk signature, used for calculating the string to sign for the current chunk.
   */
  private String previousSignature;

  /**
   * Expected signature for the current chunk, used for verifying the signature of the current chunk.
   */
  private String expectedSignature;

  /**
   * Signature info for the chunked upload, used for calculating the string to sign for the current chunk.
   */
  private final SignatureInfo signatureInfo;

  private static final String EMPTY_STRING_HASH =
      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

  private static final char NEWLINE = '\n';

  private final String amzContentSha256Header;

  private final String resource;

  public ChunkSignatureValidator(
      byte[] derivedKey, SignatureInfo signatureInfo, String amzContentSha256Header, String resource) {
    this.derivedKey = (derivedKey == null) ? null : derivedKey.clone();
    this.previousSignature = signatureInfo.getSignature();
    this.signatureInfo = signatureInfo;
    this.amzContentSha256Header = amzContentSha256Header;
    this.resource = resource;

    try {
      messageDigest = MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalArgumentException(
          "Failed to initialize MessageDigest that implements the SHA-256 algorithm.", e);
    }
  }

  public void setExpectedSignature(String expectedSignature) {
    this.expectedSignature = expectedSignature;
  }

  public void validateChunkSignature() {
    if (derivedKey == null || !S3Utils.isHMACSHA256SignedPayload(amzContentSha256Header)) {
      return;
    }
    String strToSign = buildChunkStringToSign(
        String.format("%064x", new java.math.BigInteger(1, messageDigest.digest())));
    System.out.println("strToSign: " + strToSign + ", expectedSignature: " + expectedSignature + ", derivedKey: " +
        Hex.encode(derivedKey));
    if (!AWSV4AuthValidator.validateChunk(expectedSignature, strToSign, derivedKey)) {
      throw S3ErrorTable.newError(SIGNATURE_DOES_NOT_MATCH, resource);
    }
    messageDigest.reset();
    previousSignature = expectedSignature;
  }

  public void update(byte[] data, int offset, int length) {
    if (derivedKey == null) {
      return;
    }
    messageDigest.update(data, offset, length);
  }

  public void update(byte b) {
    if (derivedKey == null) {
      return;
    }
    messageDigest.update(b);
  }

  private String buildChunkStringToSign(String chunkPayloadHash) {
    // For the chunked upload with signature, the string to sign for each chunk should be calculated as below:
    // StringToSign = AWS4-HMAC-SHA256-PAYLOAD\n
    //                <ISO8601-formatted-date>\n
    //                <CredentialScope>\n
    //                <PreviousSignature>\n
    //                <EmptyStringHash>\n
    //                <ChunkPayloadHash>
    //
    // For more details refer to AWS documentation: https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-streaming.html

    StringBuilder stringToSign = new StringBuilder();
    stringToSign.append(amzContentSha256Header).append(NEWLINE);
    stringToSign.append(signatureInfo.getDateTime()).append(NEWLINE);
    stringToSign.append(signatureInfo.getCredentialScope()).append(NEWLINE);
    stringToSign.append(previousSignature).append(NEWLINE);
    stringToSign.append(EMPTY_STRING_HASH).append(NEWLINE);
    stringToSign.append(chunkPayloadHash);
    return stringToSign.toString();
  }
}
