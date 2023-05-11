package org.streamprocessor.core.utils;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.IdTokenCredentials;
import com.google.auth.oauth2.IdTokenProvider;
import java.io.IOException;
import org.json.JSONObject;

public class DataContractUtils {

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

    public static JSONObject getDataContract(String endpoint) throws IOException {
        String[] enpointCompnents = endpoint.split("contract/");
        String baseUrl = enpointCompnents[0];
        String topic = enpointCompnents[1];
        String endpointName = "contract/" + topic;

        HttpResponse response = makeGetRequest(baseUrl, endpointName);

        return new JSONObject(response.parseAsString());
    }
}
