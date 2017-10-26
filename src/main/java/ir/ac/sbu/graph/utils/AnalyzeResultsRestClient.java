package ir.ac.sbu.graph.utils;

import com.google.gson.Gson;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;

public class AnalyzeResultsRestClient {

    public static void main(String[] args) throws IOException {
        CloseableHttpClient client = HttpClients.createDefault();
        String hostname = args[0];
        String url = "http://" + hostname + ":18080/api/v1/";
        String applicationUrl = url + "applications";
        HttpGet getApp = new HttpGet(applicationUrl);
        CloseableHttpResponse response = client.execute(getApp);
        String json = EntityUtils.toString(response.getEntity());
        Gson gson = new Gson();
        Application[] applications = gson.fromJson(json, Application[].class);
        System.out.println("application num: " + applications.length);
        for (Application application : applications) {
            System.out.println("jobId: " + application.getId());
        }
        client.close();
    }


}
