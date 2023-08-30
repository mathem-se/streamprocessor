package org.streamprocessor.core.utils;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpResponseException;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.IdTokenCredentials;
import com.google.auth.oauth2.IdTokenProvider;
import java.io.IOException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataContractUtils {

    private static final Logger LOG = LoggerFactory.getLogger(DataContractUtils.class);

    private static HttpResponse makeGetRequest(String baseUrl, String endpointName)
            throws IOException {
        if (baseUrl.endsWith("/")) {
            baseUrl = baseUrl.substring(0, baseUrl.length() - 1);
        }

        String serviceUrl = baseUrl + "/" + endpointName;

        GoogleCredentials credentials = GoogleCredentials.getApplicationDefault();
        if (!(credentials instanceof IdTokenProvider)) {
            throw new IllegalArgumentException(
                    "Credentials are not an instance of IdTokenProvider.");
        }

        IdTokenCredentials tokenCredential =
                IdTokenCredentials.newBuilder()
                        .setIdTokenProvider((IdTokenProvider) credentials)
                        .setTargetAudience(baseUrl)
                        .build();

        GenericUrl genericUrl = new GenericUrl(serviceUrl);
        HttpCredentialsAdapter adapter = new HttpCredentialsAdapter(tokenCredential);
        HttpTransport transport = new NetHttpTransport();
        HttpRequest request = transport.createRequestFactory(adapter).buildGetRequest(genericUrl);
        return request.execute();
    }

    public static JSONObject getDataContract(String endpoint)
            throws IOException, InterruptedException, CustomExceptionsUtils.InvalidEntityException {
        String[] endpointComponents = endpoint.split("contract/");
        String baseUrl = endpointComponents[0];
        String topic = endpointComponents[1];
        String endpointName = "contract/" + topic;

        int attempt = 0;
        int maxAttempts = 3;
        while (maxAttempts >= attempt) {
            attempt++;
            try {
                HttpResponse response = makeGetRequest(baseUrl, endpointName);
                return new JSONObject(response.parseAsString());
            } catch (HttpResponseException e) {
                if (e.getStatusCode() == 404) {
                    throw new CustomExceptionsUtils.InvalidEntityException(
                            String.format("Entity [%s] don't exist.", topic));
                }
                Thread.sleep(2000);
                LOG.error(
                        "Failed to get data contract: [{}]. Attempt [{}] of [{}]. Retrying..."
                                + " error: [{}]",
                        attempt,
                        maxAttempts,
                        topic,
                        e.toString());
            }
        }
        throw new IOException(
                String.format(
                        "Failed to get data contract [%s] after [%d] attempts.", topic, attempt));
    }
}
