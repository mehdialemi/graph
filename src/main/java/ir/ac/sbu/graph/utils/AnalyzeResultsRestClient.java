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

        int limit = 100;
        if (args.length > 1)
            limit = Integer.parseInt(args[1]);

        String logDir = "/tmp/analyze";
        if (args.length > 2)    logDir = args[2];

        String url = "http://" + hostname + ":18080/api/v1/";
        String applicationUrl = url + "applications?limit=" + limit;
        HttpGet get = new HttpGet(applicationUrl);
        CloseableHttpResponse response = client.execute(get);
        String json = EntityUtils.toString(response.getEntity());

        Gson gson = new Gson();
        Application[] applications = gson.fromJson(json, Application[].class);
        System.out.println("application num: " + applications.length);
        for (Application application : applications) {

            try {
                String directory = logDir + "/" + application.getName() + "-" + application.getId();
                System.out.println("dir: " + directory);
                File dir = new File(directory);
                boolean successful = dir.mkdirs();
                if (!successful)
                    System.err.println("unable to create directory " + dir);


                PrintWriter pwOverall = new PrintWriter(new File(dir + "/overall.txt"));

                pwOverall.println("startTime: " + application.getAttempts().get(0).getStartTime());
                pwOverall.println("endTime: " + application.getAttempts().get(0).getEndTime());
                pwOverall.println("duration: " + application.getAttempts().get(0).getDuration());

                String appUrl = url + "applications/" + application.getId();
                response = client.execute(new HttpGet(appUrl + "/jobs"));
                json = EntityUtils.toString(response.getEntity());
                Job[] jobs;
                try {
                    jobs = gson.fromJson(json, Job[].class);
                } catch (Exception e) {
                    jobs = new Job[]{gson.fromJson(json, Job.class)};
                }
                if (jobs == null)
                    continue;

                pwOverall.println("numJobs: " + jobs.length);
                pwOverall.println("duration");
                pwOverall.println("inputBytes");
                pwOverall.println("shuffleReadBytes");
                pwOverall.println("shuffleReadRecords");
                pwOverall.println("shuffleWriteBytes");
                pwOverall.println("shuffleWriteRecords");
                pwOverall.println("memoryBytesSpilled");
                pwOverall.println("diskBytesSpilled");

                pwOverall.close();


                PrintWriter pwJobs = new PrintWriter(new File(dir + "/jobs.txt"));
                PrintWriter pwJobsJson = new PrintWriter(new File(dir + "/jobs-json.txt"));
                PrintWriter pwStagesJson = new PrintWriter(new File(dir + "/stages-json.txt"));

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
                        String urlStage = appUrl + "/stages/" + stageId;
                        response = client.execute(new HttpGet(urlStage));
                        json = EntityUtils.toString(response.getEntity());

                        if (json.startsWith("unknown stage"))
                            continue;

                        Stage[] stages;
                        try {
                            stages = gson.fromJson(json, Stage[].class);
                        } catch (Exception e) {
                            stages = new Stage[]{gson.fromJson(json, Stage.class)};
                        }
                        if (stages == null || stages.length == 0)
                            continue;

                        Stage stage = stages[0];

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

                        pwStagesJson.println(gson.toJson(stage));
                        pwStagesJson.println();
                        pwStagesJson.println();
                        pwStagesJson.println("************************");
                        pwStagesJson.println();
                        pwStagesJson.println();
                    }

                    pwJobs.println(duration + "," + inputBytes + "," + shuffleReadBytes + "," +
                            shuffleReadRecords + "," + shuffleWriteBytes + "," + shuffleWriteRecords + "," +
                            memoryBytesSpilled + "," + diskBytesSpilled);

                    pwJobsJson.println(gson.toJson(job));
                    pwJobsJson.println();
                    pwJobsJson.println();
                    pwJobsJson.println("************************");
                    pwJobsJson.println();
                    pwJobsJson.println();

                }

                pwJobs.close();
                pwJobsJson.close();
                pwStagesJson.close();
                pwOverall.close();
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("skip " + application.getName() + "-" + application.getId());
                continue;
            }
        }

        client.close();
    }
}
