package ir.ac.sbu.graph.utils;

import com.google.gson.Gson;
import ir.ac.sbu.graph.spark.ArgumentReader;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;

public class AnalyzeResultsRestClient {


    public static final String BATCH_COMMAND = "batch";

    public static void main(String[] args) throws IOException {
        CloseableHttpClient client = HttpClients.createDefault();
        ArgumentReader argumentReader = new ArgumentReader(args);

        String hostname = argumentReader.nextString("localhost");
        String outputDir = argumentReader.nextString("/tmp/analyze");
        String command = argumentReader.nextString(BATCH_COMMAND);
        int limit = argumentReader.nextInt(0);
        String appId = argumentReader.nextString("0");

        String url = "http://" + hostname + ":18080/api/v1/";

        String applicationUrl = url + "applications" + (command.equals(BATCH_COMMAND) ? "?limit=" + limit: "");

        Application[] applications = getApplications(client, applicationUrl);
        System.out.println("application num: " + applications.length);
        for (Application application : applications) {

            if (command.equals(BATCH_COMMAND) || appId.equals(application.getId()) ) {
                System.out.println("Analyzing results for " + application.getId());
                AnalyzeAppResults analyzeAppResults = new AnalyzeAppResults(url, client, application, outputDir);
                analyzeAppResults.start();
            }
        }

        client.close();
    }

    public static Application[] getApplications(CloseableHttpClient client, String applicationUrl) throws IOException {
        HttpGet get = new HttpGet(applicationUrl);
        CloseableHttpResponse response = client.execute(get);
        String json = EntityUtils.toString(response.getEntity());

        Gson gson = new Gson();
        return gson.fromJson(json, Application[].class);
    }
}
