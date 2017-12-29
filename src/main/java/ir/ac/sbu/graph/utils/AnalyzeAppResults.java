package ir.ac.sbu.graph.utils;

import com.google.gson.Gson;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;

import java.io.File;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class AnalyzeAppResults {

    public static final String INPUT_BYTES_MAX = "inputBytesMax";
    public static final String SHUFFLE_READ_BYTES_MAX = "shuffleReadBytesMax";
    public static final String SHUFFLE_READ_RECORDS_MAX = "shuffleReadRecordsMax";
    public static final String SHUFFLE_WRITE_BYTES_MAX = "shuffleWriteBytesMax";
    public static final String SHUFFLE_WRITE_RECORDS_MAX = "shuffleWriteRecordsMax";
    public static final String JOB_DURATION = "jobDuration";
    private String url;
    private final CloseableHttpClient client;
    private final Application application;
    private String outputDir;

    public AnalyzeAppResults(String url, CloseableHttpClient client, Application application, String outputDir) {
        this.url = url;
        this.client = client;
        this.application = application;
        this.outputDir = outputDir;
    }

    public void start() {

        try {

            String name = application.getName();
            int startIndex = name.indexOf('(');
            int endIndex = name.indexOf(')');
            String graph = name.substring(startIndex, endIndex);
            String app = name.substring(0, startIndex);
            String directory = outputDir + "/" + graph + "/" + app;
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

            PrintWriter pwJobs = new PrintWriter(new File(dir + "/jobs.txt"));

            int jobId = 0;

            Map<Integer, Map<String, Long>> jobsMap = new HashMap<>();

            while (true) {
                CloseableHttpResponse response = client.execute(new HttpGet(appUrl + "/jobs/" + jobId));
                String json = EntityUtils.toString(response.getEntity());
                if (json.contains("unknown job"))
                    break;

                Gson gson = new Gson();
                Job job = gson.fromJson(json, Job.class);
                if (!job.getStatus().toUpperCase().equals("SUCCEEDED")) {
                    jobsMap.clear();
                    break;
                }

                Map<String, Long> statsMap = new HashMap<>();
                statsMap.put(INPUT_BYTES_MAX, 0L);
                statsMap.put(SHUFFLE_READ_BYTES_MAX, 0L);
                statsMap.put(SHUFFLE_READ_RECORDS_MAX, 0L);
                statsMap.put(SHUFFLE_WRITE_BYTES_MAX, 0L);
                statsMap.put(SHUFFLE_WRITE_RECORDS_MAX, 0L);
                jobsMap.put(jobId, statsMap);

                long duration = DiffTime.diffMillis(job.getSubmissionTime(), job.getCompletionTime());
                statsMap.put(JOB_DURATION, duration);


                for (Integer stageId : job.getStageIds()) {
                    String urlStage = appUrl + "/stages/" + stageId;
                    response = client.execute(new HttpGet(urlStage));
                    json = EntityUtils.toString(response.getEntity());
                    if (json.contains("unknown stage"))
                        break;

                    Stage[] stages;
                    try {
                        stages = gson.fromJson(json, Stage[].class);
                    } catch (Exception e) {
                        stages = new Stage[]{gson.fromJson(json, Stage.class)};
                    }
                    if (stages == null || stages.length == 0)
                        continue;

                    Stage stage = stages[0];

                    if (stage.getInputBytes() > statsMap.get(INPUT_BYTES_MAX))
                        statsMap.put(INPUT_BYTES_MAX, stage.getInputBytes());

                    if (stage.getShuffleReadBytes() > statsMap.get(SHUFFLE_READ_BYTES_MAX))
                        statsMap.put(SHUFFLE_READ_BYTES_MAX, stage.getShuffleReadBytes());

                    if (stage.getShuffleReadRecords() > statsMap.get(SHUFFLE_READ_RECORDS_MAX))
                        statsMap.put(SHUFFLE_READ_RECORDS_MAX, stage.getShuffleReadRecords());

                    if (stage.getShuffleWriteBytes() > statsMap.get(SHUFFLE_WRITE_BYTES_MAX))
                        statsMap.put(SHUFFLE_WRITE_BYTES_MAX, stage.getShuffleWriteBytes());

                    if (stage.getShuffleWriteRecords() > statsMap.get(SHUFFLE_WRITE_RECORDS_MAX))
                        statsMap.put(SHUFFLE_WRITE_RECORDS_MAX, stage.getShuffleWriteRecords());
                }

                jobId++;
            }

            if (jobsMap.isEmpty()) {
                pwOverall.println("Application has some failed jobs");
                pwOverall.close();
                return;
            }

            long inputBytesMax = 0;
            long shuffleReadBytesMax = 0;
            long shuffleReadRecordsMax = 0;
            long shuffleWriteBytesMax = 0;
            long shuffleWriteRecordsMax = 0;

            long totalDuration = 0;
            long inputBytesSum = 0;
            long shuffleReadBytesSum = 0;
            long shuffleReadRecordsSum = 0;
            long shuffleWriteBytesSum = 0;
            long shuffleWriteRecordsSum = 0;

            for (Map.Entry<Integer, Map<String, Long>> jobMap : jobsMap.entrySet()) {
                Map<String, Long> map = jobMap.getValue();
                Long duration = map.get(JOB_DURATION);
                Long inputBytes = map.get(INPUT_BYTES_MAX);
                Long shuffleReadBytes = map.get(SHUFFLE_READ_BYTES_MAX);
                Long shuffleReadRecords = map.get(SHUFFLE_READ_RECORDS_MAX);
                Long shuffleWriteBytes = map.get(SHUFFLE_WRITE_BYTES_MAX);
                Long shuffleWriteRecords = map.get(SHUFFLE_WRITE_RECORDS_MAX);

                totalDuration += duration;
                inputBytesSum += inputBytes;
                shuffleReadBytesSum += shuffleReadBytes;
                shuffleReadRecordsSum += shuffleReadRecords;
                shuffleWriteBytesSum += shuffleWriteBytes;
                shuffleWriteRecordsSum += shuffleWriteRecords;

                pwJobs.println(duration + "," +
                        inputBytes + "," +
                        shuffleReadBytes + "," +
                        shuffleReadRecords + "," +
                        shuffleWriteBytes + "," +
                        shuffleWriteRecords);

                if (inputBytes > inputBytesMax)
                    inputBytesMax = inputBytes;

                if (shuffleReadBytes > shuffleReadBytesMax)
                    shuffleReadBytesMax = shuffleReadBytes;

                if (shuffleReadRecords > shuffleReadRecordsMax)
                    shuffleReadRecordsMax = shuffleReadRecords;

                if (shuffleWriteBytes > shuffleWriteBytesMax)
                    shuffleWriteBytesMax = shuffleWriteBytes;

                if (shuffleWriteRecords > shuffleWriteRecordsMax)
                    shuffleWriteRecordsMax = shuffleWriteRecords;

            }

            pwOverall.println("numJobs: " + jobId);
            pwOverall.println("total duration: " + totalDuration);

            pwOverall.println("inputBytesMax: " + inputBytesMax);
            pwOverall.println("shuffleReadBytesMax: " + shuffleReadBytesMax);
            pwOverall.println("shuffleReadRecordsMax: " + shuffleReadRecordsMax);
            pwOverall.println("shuffleWriteBytesMax: " + shuffleWriteBytesMax);
            pwOverall.println("shuffleWriteRecordsMax: " + shuffleWriteRecordsMax);

            pwOverall.println("inputBytesSum: " + inputBytesSum);
            pwOverall.println("shuffleReadBytesSum: " + shuffleReadBytesSum);
            pwOverall.println("shuffleReadRecordsSum: " + shuffleReadRecordsSum);
            pwOverall.println("shuffleWriteBytesSum: " + shuffleWriteBytesSum);
            pwOverall.println("shuffleWriteRecordsSum: " + shuffleWriteRecordsSum);

            pwJobs.close();
            pwOverall.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Error in " + application.getId());
            return;
        }
    }
}
