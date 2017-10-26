package ir.ac.sbu.graph.utils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
"status",
"stageId",
"attemptId",
"numActiveTasks",
"numCompleteTasks",
"numFailedTasks",
"executorRunTime",
"executorCpuTime",
"submissionTime",
"firstTaskLaunchedTime",
"completionTime",
"inputBytes",
"inputRecords",
"outputBytes",
"outputRecords",
"shuffleReadBytes",
"shuffleReadRecords",
"shuffleWriteBytes",
"shuffleWriteRecords",
"memoryBytesSpilled",
"diskBytesSpilled",
"name",
"schedulingPool",
"accumulatorUpdates"
})
public class Stage {

@JsonProperty("status")
private String status;
@JsonProperty("stageId")
private Long stageId;
@JsonProperty("attemptId")
private Long attemptId;
@JsonProperty("numActiveTasks")
private Long numActiveTasks;
@JsonProperty("numCompleteTasks")
private Long numCompleteTasks;
@JsonProperty("numFailedTasks")
private Long numFailedTasks;
@JsonProperty("executorRunTime")
private Long executorRunTime;
@JsonProperty("executorCpuTime")
private Long executorCpuTime;
@JsonProperty("submissionTime")
private String submissionTime;
@JsonProperty("firstTaskLaunchedTime")
private String firstTaskLaunchedTime;
@JsonProperty("completionTime")
private String completionTime;
@JsonProperty("inputBytes")
private Long inputBytes;
@JsonProperty("inputRecords")
private Long inputRecords;
@JsonProperty("outputBytes")
private Long outputBytes;
@JsonProperty("outputRecords")
private Long outputRecords;
@JsonProperty("shuffleReadBytes")
private Long shuffleReadBytes;
@JsonProperty("shuffleReadRecords")
private Long shuffleReadRecords;
@JsonProperty("shuffleWriteBytes")
private Long shuffleWriteBytes;
@JsonProperty("shuffleWriteRecords")
private Long shuffleWriteRecords;
@JsonProperty("memoryBytesSpilled")
private Long memoryBytesSpilled;
@JsonProperty("diskBytesSpilled")
private Long diskBytesSpilled;
@JsonProperty("name")
private String name;
@JsonProperty("schedulingPool")
private String schedulingPool;
@JsonProperty("accumulatorUpdates")
private List<AccumulatorUpdate> accumulatorUpdates = null;
@JsonIgnore
private Map<String, Object> additionalProperties = new HashMap<String, Object>();

@JsonProperty("status")
public String getStatus() {
return status;
}

@JsonProperty("status")
public void setStatus(String status) {
this.status = status;
}

@JsonProperty("stageId")
public Long getStageId() {
return stageId;
}

@JsonProperty("stageId")
public void setStageId(Long stageId) {
this.stageId = stageId;
}

@JsonProperty("attemptId")
public Long getAttemptId() {
return attemptId;
}

@JsonProperty("attemptId")
public void setAttemptId(Long attemptId) {
this.attemptId = attemptId;
}

@JsonProperty("numActiveTasks")
public Long getNumActiveTasks() {
return numActiveTasks;
}

@JsonProperty("numActiveTasks")
public void setNumActiveTasks(Long numActiveTasks) {
this.numActiveTasks = numActiveTasks;
}

@JsonProperty("numCompleteTasks")
public Long getNumCompleteTasks() {
return numCompleteTasks;
}

@JsonProperty("numCompleteTasks")
public void setNumCompleteTasks(Long numCompleteTasks) {
this.numCompleteTasks = numCompleteTasks;
}

@JsonProperty("numFailedTasks")
public Long getNumFailedTasks() {
return numFailedTasks;
}

@JsonProperty("numFailedTasks")
public void setNumFailedTasks(Long numFailedTasks) {
this.numFailedTasks = numFailedTasks;
}

@JsonProperty("executorRunTime")
public Long getExecutorRunTime() {
return executorRunTime;
}

@JsonProperty("executorRunTime")
public void setExecutorRunTime(Long executorRunTime) {
this.executorRunTime = executorRunTime;
}

@JsonProperty("executorCpuTime")
public Long getExecutorCpuTime() {
return executorCpuTime;
}

@JsonProperty("executorCpuTime")
public void setExecutorCpuTime(Long executorCpuTime) {
this.executorCpuTime = executorCpuTime;
}

@JsonProperty("submissionTime")
public String getSubmissionTime() {
return submissionTime;
}

@JsonProperty("submissionTime")
public void setSubmissionTime(String submissionTime) {
this.submissionTime = submissionTime;
}

@JsonProperty("firstTaskLaunchedTime")
public String getFirstTaskLaunchedTime() {
return firstTaskLaunchedTime;
}

@JsonProperty("firstTaskLaunchedTime")
public void setFirstTaskLaunchedTime(String firstTaskLaunchedTime) {
this.firstTaskLaunchedTime = firstTaskLaunchedTime;
}

@JsonProperty("completionTime")
public String getCompletionTime() {
return completionTime;
}

@JsonProperty("completionTime")
public void setCompletionTime(String completionTime) {
this.completionTime = completionTime;
}

@JsonProperty("inputBytes")
public Long getInputBytes() {
return inputBytes;
}

@JsonProperty("inputBytes")
public void setInputBytes(Long inputBytes) {
this.inputBytes = inputBytes;
}

@JsonProperty("inputRecords")
public Long getInputRecords() {
return inputRecords;
}

@JsonProperty("inputRecords")
public void setInputRecords(Long inputRecords) {
this.inputRecords = inputRecords;
}

@JsonProperty("outputBytes")
public Long getOutputBytes() {
return outputBytes;
}

@JsonProperty("outputBytes")
public void setOutputBytes(Long outputBytes) {
this.outputBytes = outputBytes;
}

@JsonProperty("outputRecords")
public Long getOutputRecords() {
return outputRecords;
}

@JsonProperty("outputRecords")
public void setOutputRecords(Long outputRecords) {
this.outputRecords = outputRecords;
}

@JsonProperty("shuffleReadBytes")
public Long getShuffleReadBytes() {
return shuffleReadBytes;
}

@JsonProperty("shuffleReadBytes")
public void setShuffleReadBytes(Long shuffleReadBytes) {
this.shuffleReadBytes = shuffleReadBytes;
}

@JsonProperty("shuffleReadRecords")
public Long getShuffleReadRecords() {
return shuffleReadRecords;
}

@JsonProperty("shuffleReadRecords")
public void setShuffleReadRecords(Long shuffleReadRecords) {
this.shuffleReadRecords = shuffleReadRecords;
}

@JsonProperty("shuffleWriteBytes")
public Long getShuffleWriteBytes() {
return shuffleWriteBytes;
}

@JsonProperty("shuffleWriteBytes")
public void setShuffleWriteBytes(Long shuffleWriteBytes) {
this.shuffleWriteBytes = shuffleWriteBytes;
}

@JsonProperty("shuffleWriteRecords")
public Long getShuffleWriteRecords() {
return shuffleWriteRecords;
}

@JsonProperty("shuffleWriteRecords")
public void setShuffleWriteRecords(Long shuffleWriteRecords) {
this.shuffleWriteRecords = shuffleWriteRecords;
}

@JsonProperty("memoryBytesSpilled")
public Long getMemoryBytesSpilled() {
return memoryBytesSpilled;
}

@JsonProperty("memoryBytesSpilled")
public void setMemoryBytesSpilled(Long memoryBytesSpilled) {
this.memoryBytesSpilled = memoryBytesSpilled;
}

@JsonProperty("diskBytesSpilled")
public Long getDiskBytesSpilled() {
return diskBytesSpilled;
}

@JsonProperty("diskBytesSpilled")
public void setDiskBytesSpilled(Long diskBytesSpilled) {
this.diskBytesSpilled = diskBytesSpilled;
}

@JsonProperty("name")
public String getName() {
return name;
}

@JsonProperty("name")
public void setName(String name) {
this.name = name;
}

@JsonProperty("schedulingPool")
public String getSchedulingPool() {
return schedulingPool;
}

@JsonProperty("schedulingPool")
public void setSchedulingPool(String schedulingPool) {
this.schedulingPool = schedulingPool;
}

@JsonProperty("accumulatorUpdates")
public List<AccumulatorUpdate> getAccumulatorUpdates() {
return accumulatorUpdates;
}

@JsonProperty("accumulatorUpdates")
public void setAccumulatorUpdates(List<AccumulatorUpdate> accumulatorUpdates) {
this.accumulatorUpdates = accumulatorUpdates;
}

@JsonAnyGetter
public Map<String, Object> getAdditionalProperties() {
return this.additionalProperties;
}

@JsonAnySetter
public void setAdditionalProperty(String name, Object value) {
this.additionalProperties.put(name, value);
}

}
