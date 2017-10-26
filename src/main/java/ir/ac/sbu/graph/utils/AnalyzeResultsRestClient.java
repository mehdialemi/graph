package ir.ac.sbu.graph.utils;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;

/**
 * Created by mehdi on 10/26/17.
 */
public class AnalyzeResultsRestClient {

    public static void main(String[] args) throws IOException {
        CloseableHttpClient client = HttpClients.createDefault();
        String hostname = args[0];
        String url = "http://" + hostname + ":18080/api/v1/";

        HttpGet getApp = new HttpGet(url + "/applications");
        CloseableHttpResponse response = client.execute(getApp);
        String json = EntityUtils.toString(response.getEntity());
        System.out.println(json);
    }


}
