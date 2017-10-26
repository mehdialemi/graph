package ir.ac.sbu.graph.utils;

import com.google.gson.Gson;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;

public class AnalyzeResultsRestClient {

    public static void main(String[] args) throws IOException {
        CloseableHttpClient client = HttpClients.createDefault();
        String hostname = args[0];
        String logDir = "/tmp/analyze";
        if (args.length > 1)    logDir = args[1];

        String url = "http://" + hostname + ":18080/api/v1/";
        String applicationUrl = url + "applications";
        HttpGet get = new HttpGet(applicationUrl);
        CloseableHttpResponse response = client.execute(get);
        String json = EntityUtils.toString(response.getEntity());

        Gson gson = new Gson();
        Application[] applications = gson.fromJson(json, Application[].class);
        System.out.println("application num: " + applications.length);
        for (Application application : applications) {

            String directory = logDir + "/" + application.getName() + "-" + application.getId();
            System.out.println("dir: " + directory);
            File dir = new File(directory);
            boolean successful = dir.mkdir();
            if (!successful)
                System.err.println("unable to create directory " + dir);


            PrintWriter pwOverall = new PrintWriter(new File(dir + "/overall.txt"));

            pwOverall.println("startTime: " + application.getAttempts().get(0).getStartTime());
            pwOverall.println("endTime: " + application.getAttempts().get(0).getEndTime());
            pwOverall.println("duration: " + application.getAttempts().get(0).getDuration());

            String urlJobs = applicationUrl + "/" + application.getId() + "/jobs";
            response = client.execute(new HttpGet(urlJobs));
            json = EntityUtils.toString(response.getEntity());
            Job[] jobs = gson.fromJson(json, Job[].class);
            if (jobs == null)
                continue;

            pwOverall.println("numJobs: " + jobs.length);
            pwOverall.close();


            PrintWriter pwJobs = new PrintWriter(new File(dir + "/jobs.txt"));
            PrintWriter pwJobsJson = new PrintWriter(new File(dir + "/jobs-json.txt"));
            PrintWriter pwStagesJson = new PrintWriter(new File(dir + "/stages-json.txt"));

            pwJobs.println("duration    inputBytes  shuffleReadBytes    shuffleReadRecords " +
                            "shuffleWriteBytes shuffleWriteRecords  memoryBytesSpilled  diskBytesSpilled");

            for (Job job : jobs) {

                long duration = DiffTime.diffMillis(job.getSubmissionTime(), job.getCompletionTime());
                long inputBytes = 0;
                long shuffleReadBytes = 0;
                long shuffleReadRecords = 0;
                long shuffleWriteBytes = 0;
                long shuffleWriteRecords = 0;
                long memoryBytesSpilled = 0;
                long diskBytesSpilled = 0;

                for (Integer stageId : job.getStageIds()) {
                    String urlStage = applicationUrl + "/" + application.getId() + "/stages/" + stageId;
                    response = client.execute(new HttpGet(urlStage));
                    json = EntityUtils.toString(response.getEntity());
                    Stage stage = gson.fromJson(json, Stage.class);

                    if (stage.getInputBytes() > inputBytes)
                        inputBytes = stage.getInputBytes();

                    if (stage.getShuffleReadBytes() > shuffleReadBytes)
                        shuffleReadBytes = stage.getShuffleReadBytes();

                    if (stage.getShuffleReadRecords() > shuffleReadRecords)
                        shuffleReadRecords = stage.getShuffleReadRecords();

                    if (stage.getShuffleWriteBytes() > shuffleWriteBytes)
                        shuffleWriteBytes = stage.getShuffleWriteBytes();

                    if (stage.getShuffleWriteRecords() > shuffleWriteRecords)
                        shuffleWriteRecords = stage.getShuffleWriteRecords();

                    if (stage.getMemoryBytesSpilled() > memoryBytesSpilled)
                        memoryBytesSpilled = stage.getMemoryBytesSpilled();

                    if (stage.getDiskBytesSpilled() > diskBytesSpilled)
                        diskBytesSpilled = stage.getDiskBytesSpilled();

                    pwStagesJson .println(gson.toJson(stage));
                    pwStagesJson.println();
                    pwStagesJson.println();
                    pwStagesJson.println("************************");
                    pwStagesJson.println();
                    pwStagesJson.println();
                }

                pwJobs.println(duration + " " + inputBytes + " " + shuffleReadBytes + " " +
                        shuffleReadRecords + " " + shuffleWriteBytes + " " + shuffleWriteRecords + " " +
                        memoryBytesSpilled + " " + diskBytesSpilled);

                pwJobsJson.println(gson.toJson(job));
                pwJobsJson.println();
                pwJobsJson.println();
                pwJobsJson.println("************************");
                pwJobsJson.println();
                pwJobsJson.println();

            }
        }

        client.close();
    }


}
