package io.airbyte.integrations.destination.s3;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.identitymanagement.AmazonIdentityManagement;
import com.amazonaws.services.identitymanagement.AmazonIdentityManagementClientBuilder;
import com.amazonaws.services.identitymanagement.model.AttachRolePolicyRequest;
import com.amazonaws.services.identitymanagement.model.AttachedPolicy;
import com.amazonaws.services.identitymanagement.model.CreatePolicyRequest;
import com.amazonaws.services.identitymanagement.model.CreatePolicyResult;
import com.amazonaws.services.identitymanagement.model.DeletePolicyRequest;
import com.amazonaws.services.identitymanagement.model.DetachRolePolicyRequest;
import com.amazonaws.services.identitymanagement.model.GetPolicyRequest;
import com.amazonaws.services.identitymanagement.model.GetRolePolicyRequest;
import com.amazonaws.services.identitymanagement.model.ListAttachedRolePoliciesRequest;
import com.amazonaws.services.identitymanagement.model.ListRolePoliciesRequest;
import com.amazonaws.services.identitymanagement.model.Policy;
import io.airbyte.cdk.integrations.destination.s3.S3BaseChecks;
import io.airbyte.cdk.integrations.destination.s3.S3DestinationConfig;
import io.airbyte.cdk.integrations.destination.s3.S3DestinationConfigFactory;
import io.airbyte.cdk.integrations.destination.s3.credential.S3AssumeRoleCredentialConfig;
import io.airbyte.protocol.models.v0.AirbyteConnectionStatus.Status;
import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalUnit;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

public class S3DestinationAssumeRoleTest {
  @Test
  void testFailsWithNoEnvCredentials() {
    S3Destination dest = new S3Destination(new S3DestinationConfigFactory(), Collections.emptyMap());
    assertEquals(Status.FAILED, dest.check(S3DestinationTestUtils.getAssumeRoleConfig()).getStatus());
  }

  @Test
  void testPassesWithAllCredentials() {
    S3Destination dest = new S3Destination(new S3DestinationConfigFactory(), S3DestinationTestUtils.getAssumeRoleInternalCredentials());
    assertEquals(Status.SUCCEEDED, dest.check(S3DestinationTestUtils.getAssumeRoleConfig()).getStatus());
  }

  @Test
  void testFailsWithWrongExternalId() {
    Map<String, String> envWithBadExternalId = new HashMap<>(S3DestinationTestUtils.getAssumeRoleInternalCredentials());
    envWithBadExternalId.put("AWS_ASSUME_ROLE_EXTERNAL_ID", "dummyValue");
    S3Destination dest = new S3Destination(new S3DestinationConfigFactory(), envWithBadExternalId);
    assertEquals(Status.FAILED, dest.check(S3DestinationTestUtils.getAssumeRoleConfig()).getStatus());
  }

  private static final String REVOKE_OLD_TOKENS_POLICY = """
         {
           "Version": "2012-10-17",
           "Statement": {
             "Effect": "Deny",
             "Action": "*",
             "Resource": [
                 "arn:aws:s3:::airbyte-integration-test-destination-s3/*",
                 "arn:aws:s3:::airbyte-integration-test-destination-s3"
             ],
             "Condition": {
               "DateLessThan": {"aws:TokenIssueTime": "%s"}
             }
           }
         }
         """;
  private static final String POLICY_NAME_PREFIX = S3DestinationAssumeRoleTest.class.getSimpleName() + "Policy-";
  private static final String ASSUMED_ROLE_NAME = "s3_acceptance_test_iam_assume_role_role";

  @Test
  @Timeout(value = 1, unit = TimeUnit.HOURS)
  void testAutomaticRenewal() throws IOException, InterruptedException {
    final AmazonIdentityManagement iam =
        AmazonIdentityManagementClientBuilder.standard().withRegion(Regions.DEFAULT_REGION).withCredentials(S3AssumeRoleCredentialConfig.getCredentialProvider(S3DestinationTestUtils.getPolicyManagerCredentials())).build();
    ListAttachedRolePoliciesRequest listPoliciesRequest = new ListAttachedRolePoliciesRequest().withRoleName(ASSUMED_ROLE_NAME);
    List<AttachedPolicy> policies = iam.listAttachedRolePolicies(listPoliciesRequest).getAttachedPolicies();
    for (AttachedPolicy policy:policies) {
      if (policy.getPolicyName().startsWith(POLICY_NAME_PREFIX)) {
        DetachRolePolicyRequest detachPolicyRequest = new DetachRolePolicyRequest().withPolicyArn(policy.getPolicyArn()).withRoleName(ASSUMED_ROLE_NAME);
        iam.detachRolePolicy(detachPolicyRequest);
        DeletePolicyRequest deleteRequest = new DeletePolicyRequest().withPolicyArn(policy.getPolicyArn());
        iam.deletePolicy(deleteRequest);
      }
    }


    Map<String, String> environmentForAssumeRole = S3DestinationTestUtils.getAssumeRoleInternalCredentials();
    S3DestinationConfig config = S3DestinationConfig.getS3DestinationConfig(S3DestinationTestUtils.getAssumeRoleConfig(), environmentForAssumeRole);
    var s3Client = config.getS3Client();
    S3BaseChecks.testSingleUpload(
        s3Client,
        config.getBucketName(),
        config.getBucketPath()
    );

    // We wait 1 second so that the less than is strictly greater than the token generation time
    Thread.sleep(1000);
    String now = Instant.now().truncatedTo(ChronoUnit.SECONDS).toString();
    String policyName = POLICY_NAME_PREFIX + now.toString().replace(':', '-');

    String policyDocument = REVOKE_OLD_TOKENS_POLICY.formatted(now);
    CreatePolicyRequest createPolicyRequest = new CreatePolicyRequest()
        .withPolicyName(policyName)
        .withPolicyDocument(policyDocument);
    String arn = iam.createPolicy(createPolicyRequest).getPolicy().getArn();
    try {
      AttachRolePolicyRequest attachPolicyRequest =
          new AttachRolePolicyRequest()
              .withRoleName(ASSUMED_ROLE_NAME)
              .withPolicyArn(arn);

      iam.attachRolePolicy(attachPolicyRequest);
      // Experience showed than under 30sec, the policy is not applied. Giving it some buffer
      Thread.sleep(60_000);

      // We check that the deny policy is enforced
      assertThrows(Exception.class, () -> S3BaseChecks.testSingleUpload(
          s3Client,
          config.getBucketName(),
          config.getBucketPath()
      ));
      // and force-refresh the token
      config.getS3CredentialConfig().getS3CredentialsProvider().refresh();
      S3BaseChecks.testSingleUpload(
          s3Client,
          config.getBucketName(),
          config.getBucketPath()
      );
    } finally {
      DetachRolePolicyRequest detachPolicyRequest = new DetachRolePolicyRequest().withPolicyArn(arn).withRoleName(ASSUMED_ROLE_NAME);
      iam.detachRolePolicy(detachPolicyRequest);
      DeletePolicyRequest deleteRequest = new DeletePolicyRequest().withPolicyArn(arn);
      iam.deletePolicy(deleteRequest);
    }
  }
}
