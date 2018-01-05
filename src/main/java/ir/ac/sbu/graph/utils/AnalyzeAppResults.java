package ir.ac.sbu.graph.utils;

import com.google.gson.Gson;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;

import java.io.File;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.util.HashMap;
import java.util.List;
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
    public static final String DURATION = "duration";
    public static final String INPUT = "input";
    public static final String SHUFFLE = "shuffle";
    public static final double GB = 1024 * 1024 * 1024.0;
    private static DecimalFormat df2 = new DecimalFormat("#.##");
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
            String graph = name.substring(startIndex + 1, endIndex).trim();
            String app = name.substring(0, startIndex).trim();
            String directory = outputDir + "/" + graph + "/" + app;
            System.out.println("dir: " + directory);
            File dir = new File(directory);
            boolean successful = dir.mkdirs();
            if (!successful) {
                System.err.println("unable to create directory " + dir);
                return;
            }

            PrintWriter pwSteps = new PrintWriter(new File(dir + "/steps.txt"));
            PrintWriter pwOverall = new PrintWriter(new File(dir + "/overall.txt"));
            pwOverall.println("startTime: " + application.getAttempts().get(0).getStartTime());
            pwOverall.println("endTime: " + application.getAttempts().get(0).getEndTime());
            pwOverall.println("duration: " + application.getAttempts().get(0).getDuration());

            String appUrl = url + "applications/" + application.getId();

            PrintWriter pwJobs = new PrintWriter(new File(dir + "/jobs.txt"));

            Integer jobId = 0;

            Map<Integer, Map<String, Long>> jobsMap = new HashMap<>();

            boolean stop = false;
            Map<String, Long> loadingMap = new HashMap<>();
            Map<String, Long> neighborMap = new HashMap<>();
            Map<String, Long> kcoreMap = new HashMap<>();
            Map<String, Long> tsetMap = new HashMap<>();
            Map<String, Long> ktrussMap = new HashMap<>();

            boolean logKCore;
            boolean logKTruss = false;
            boolean logTSet = false;
            while (!stop) {
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

                if (job.getName().contains("KCore"))
                    logKCore = true;
                else
                    logKCore = false;

                if (!logKTruss && job.getName().contains("KTrussTSet")) {
                    logKTruss = true;
                    logTSet = true;
                }

                Map<String, Long> sMap = new HashMap<>();

                long jobDuration = DiffTime.diffMillis(job.getSubmissionTime(), job.getCompletionTime());
                sMap.put(JOB_DURATION, jobDuration);

                long inputBytesMax = 0;
                long shuffleReadBytesMax = 0;
                long shuffleReadRecordsMax = 0;
                long shuffleWriteBytesMax = 0;
                long shuffleWriteRecordsMax = 0;
                for (Integer stageId : job.getStageIds()) {

                    String urlStage = appUrl + "/stages/" + stageId;
                    response = client.execute(new HttpGet(urlStage));
                    json = EntityUtils.toString(response.getEntity());
                    if (json.contains("unknown stage"))
                        continue;

                    Stage[] stages = gson.fromJson(json, Stage[].class);
                    Stage stage = null;
                    for (Stage st : stages) {
                        if (st.getStatus().equals("COMPLETE")) {
                            stage = st;
                            break;
                        }
                    }

                    if (stage == null) {
                        jobsMap.clear();
                        stop = true;
                        break;
                    }

                    long stageDuration =
                            DiffTime.diffMillis(stage.getSubmissionTime(), stage.getCompletionTime());

                    if (stage.getInputBytes() > inputBytesMax)
                        inputBytesMax = stage.getInputBytes();

                    if (stage.getShuffleReadBytes() > shuffleReadBytesMax)
                        shuffleReadBytesMax = stage.getShuffleReadBytes();

                    if (stage.getShuffleReadRecords() > shuffleReadRecordsMax)
                        shuffleReadRecordsMax = stage.getShuffleReadRecords();

                    if (stage.getShuffleWriteBytes() > shuffleWriteBytesMax)
                        shuffleWriteBytesMax = stage.getShuffleWriteBytes();

                    if (stage.getShuffleWriteRecords() > shuffleWriteRecordsMax)
                        shuffleWriteRecordsMax = stage.getShuffleWriteRecords();

                    if (logKCore) {
                        if (loadingMap.isEmpty()) {
                            loadingMap.put(DURATION, stageDuration);
                            loadingMap.put(INPUT, stage.getInputBytes());
                            loadingMap.put(SHUFFLE, stage.getShuffleWriteBytes());
                        } else if (neighborMap.isEmpty()) {
                            neighborMap.put(DURATION, stageDuration);
                            neighborMap.put(INPUT, stage.getInputBytes());
                            neighborMap.put(SHUFFLE, stage.getShuffleWriteBytes());
                        } else {
                            long lastDuration = kcoreMap.getOrDefault(DURATION, 0L);
                            kcoreMap.put(DURATION, lastDuration + stageDuration);

                            if (stage.getInputBytes() > kcoreMap.getOrDefault(INPUT, 0L))
                                kcoreMap.put(INPUT, stage.getInputBytes());

                            if (stage.getShuffleWriteBytes() > kcoreMap.getOrDefault(SHUFFLE, 0L))
                                kcoreMap.put(SHUFFLE, stage.getShuffleWriteBytes());
                        }
                    }

                    if (logTSet) {
                        if (loadingMap.isEmpty()) {
                            loadingMap.put(DURATION, stageDuration);
                            loadingMap.put(INPUT, stage.getInputBytes());
                            loadingMap.put(SHUFFLE, stage.getShuffleWriteBytes());
                        } else if (neighborMap.isEmpty()) {
                            neighborMap.put(DURATION, stageDuration);
                            neighborMap.put(INPUT, stage.getInputBytes());
                            neighborMap.put(SHUFFLE, stage.getShuffleWriteBytes());
                        } else {
                            if (stage.getInputBytes() > tsetMap.getOrDefault(INPUT, 0L))
                                tsetMap.put(INPUT, stage.getInputBytes());

                            if (stage.getShuffleWriteBytes() > tsetMap.getOrDefault(SHUFFLE, 0L))
                                tsetMap.put(SHUFFLE, stage.getShuffleWriteBytes());
                        }
                    }
                }

                if (logTSet) {
                    Long loadTime = loadingMap.get(DURATION);
                    System.out.println("loadTime: " + loadTime);

                    Long neighborTime = neighborMap.get(DURATION);
                    System.out.println("neighborTime: " + neighborTime);

                    long sumTwo = loadTime + neighborTime;
                    long tsetTime = jobDuration - sumTwo;
                    System.out.println("sumTwo: " + sumTwo);
                    System.out.println("jobDuration: " + jobDuration);
                    System.out.println("tsetTime: " + tsetTime);
                    tsetMap.put(DURATION, tsetTime);
                    logTSet = false;
                }
                else if (logKTruss) {
                    long lastJobDuration = ktrussMap.getOrDefault(DURATION, 0L);
                    ktrussMap.put(DURATION, jobDuration + lastJobDuration);

                    if (inputBytesMax > ktrussMap.getOrDefault(INPUT, 0L))
                        ktrussMap.put(INPUT, inputBytesMax);

                    if (shuffleWriteBytesMax > ktrussMap.getOrDefault(SHUFFLE, 0L))
                        ktrussMap.put(SHUFFLE, shuffleWriteBytesMax);
                }

                sMap.put(INPUT_BYTES_MAX, inputBytesMax);
                sMap.put(SHUFFLE_READ_BYTES_MAX, shuffleReadBytesMax);
                sMap.put(SHUFFLE_READ_RECORDS_MAX, shuffleReadRecordsMax);
                sMap.put(SHUFFLE_WRITE_BYTES_MAX, shuffleWriteBytesMax);
                sMap.put(SHUFFLE_WRITE_RECORDS_MAX, shuffleWriteRecordsMax);

                jobsMap.put(jobId, sMap);
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

            for (Map.Entry<Integer, Map<String, Long>> entry : jobsMap.entrySet()) {
                Map<String, Long> map = entry.getValue();
                Long duration = map.get(JOB_DURATION);
                Long inputBytes = map.getOrDefault(INPUT_BYTES_MAX, 0L);
                Long shuffleReadBytes = map.getOrDefault(SHUFFLE_READ_BYTES_MAX, 0L);
                Long shuffleReadRecords = map.getOrDefault(SHUFFLE_READ_RECORDS_MAX, 0L);
                Long shuffleWriteBytes = map.getOrDefault(SHUFFLE_WRITE_BYTES_MAX, 0L);
                Long shuffleWriteRecords = map.getOrDefault(SHUFFLE_WRITE_RECORDS_MAX, 0L);

                totalDuration += duration;
                inputBytesSum += inputBytes;
                shuffleReadBytesSum += shuffleReadBytes;
                shuffleReadRecordsSum += shuffleReadRecords;
                shuffleWriteBytesSum += shuffleWriteBytes;
                shuffleWriteRecordsSum += shuffleWriteRecords;


                String line = duration + ", " + inputBytes + ", " + shuffleWriteBytes;
                pwJobs.println(line);

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

            double ms = 1000.0;
            pwSteps.println("load: " +
                    df2.format(loadingMap.getOrDefault(DURATION, 0L) / ms) + ", " +
                    df2.format(loadingMap.getOrDefault(INPUT, 0L) / GB) + ", " +
                    df2.format(loadingMap.getOrDefault(SHUFFLE, 0L) / GB));

            pwSteps.println("neighbor: " +
                    df2.format(neighborMap.getOrDefault(DURATION, 0L) / ms) + ", " +
                    df2.format(neighborMap.getOrDefault(INPUT, 0L) / GB)+ ", " +
                    df2.format(neighborMap.getOrDefault(SHUFFLE, 0L)/ GB));

            pwSteps.println("kcore: " +
                    df2.format(kcoreMap.getOrDefault(DURATION, 0L) / ms) + ", " +
                    df2.format(kcoreMap.getOrDefault(INPUT, 0L)/ GB) + ", " +
                    df2.format(kcoreMap.getOrDefault(SHUFFLE, 0L)/ GB));

            pwSteps.println("tset: " +
                    df2.format(tsetMap.getOrDefault(DURATION, 0L)/ ms) + ", " +
                    df2.format(tsetMap.getOrDefault(INPUT, 0L) / GB) + ", " +
                    df2.format(tsetMap.getOrDefault(SHUFFLE, 0L)/ GB));

            pwSteps.println("ktruss: " +
                    df2.format(ktrussMap.getOrDefault(DURATION, 0L) / ms) + ", " +
                    df2.format(ktrussMap.getOrDefault(INPUT, 0L) / GB)+ ", " +
                    df2.format(ktrussMap.getOrDefault(SHUFFLE, 0L)/ GB));

            pwOverall.println("numJobs: " + jobId);
            pwOverall.println("total duration: " + totalDuration);
            pwOverall.println();

            pwOverall.println();
            pwOverall.println("inputBytesMax: " + inputBytesMax);
            pwOverall.println("shuffleReadBytesMax: " + shuffleReadBytesMax);
            pwOverall.println("shuffleReadRecordsMax: " + shuffleReadRecordsMax);
            pwOverall.println("shuffleWriteBytesMax: " + shuffleWriteBytesMax);
            pwOverall.println("shuffleWriteRecordsMax: " + shuffleWriteRecordsMax);
            pwOverall.println();
            pwOverall.println("inputBytesSum: " + inputBytesSum);
            pwOverall.println("shuffleReadBytesSum: " + shuffleReadBytesSum);
            pwOverall.println("shuffleReadRecordsSum: " + shuffleReadRecordsSum);
            pwOverall.println("shuffleWriteBytesSum: " + shuffleWriteBytesSum);
            pwOverall.println("shuffleWriteRecordsSum: " + shuffleWriteRecordsSum);

            pwJobs.close();
            pwOverall.close();
            pwSteps.close();
        } catch (
                Exception e)

        {
            e.printStackTrace();
            System.out.println("Error in " + application.getId());
            return;
        }

    }

    public static Map<String, Long> createMapStage(List<Stage> stages) throws ParseException {
        long inputBytesMax = 0;
        long shuffleReadBytesMax = 0;
        long shuffleReadRecordsMax = 0;
        long shuffleWriteBytesMax = 0;
        long shuffleWriteRecordsMax = 0;

        long duration = 0;
        for (Stage stage : stages) {
            String launchedTime = stage.getFirstTaskLaunchedTime();
            String completionTime = stage.getCompletionTime();
            duration += TimeUtil.diff(completionTime, launchedTime);

            if (stage.getInputBytes() > inputBytesMax)
                inputBytesMax = stage.getInputBytes();

            if (stage.getShuffleReadBytes() > shuffleReadBytesMax)
                shuffleReadBytesMax = stage.getShuffleReadBytes();

            if (stage.getShuffleReadRecords() > shuffleReadRecordsMax)
                shuffleReadRecordsMax = stage.getShuffleReadRecords();

            if (stage.getShuffleWriteBytes() > shuffleWriteBytesMax)
                shuffleWriteBytesMax = stage.getShuffleWriteBytes();

            if (stage.getShuffleWriteRecords() > shuffleWriteRecordsMax)
                shuffleWriteRecordsMax = stage.getShuffleWriteRecords();
        }

        Map<String, Long> sMap = new HashMap<>();
        sMap.put(INPUT_BYTES_MAX, inputBytesMax);
        sMap.put(SHUFFLE_READ_BYTES_MAX, shuffleReadRecordsMax);
        sMap.put(SHUFFLE_READ_RECORDS_MAX, shuffleReadRecordsMax);
        sMap.put(SHUFFLE_WRITE_BYTES_MAX, shuffleWriteBytesMax);
        sMap.put(SHUFFLE_WRITE_RECORDS_MAX, shuffleWriteRecordsMax);
        sMap.put(JOB_DURATION, duration);

        return sMap;
    }
}
