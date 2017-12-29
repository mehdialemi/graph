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
"jobId",
"name",
"submissionTime",
"completionTime",
"stageIds",
"status",
"numTasks",
"numActiveTasks",
"numCompletedTasks",
"numSkippedTasks",
"numFailedTasks",
"numActiveStages",
"numCompletedStages",
"numSkippedStages",
"numFailedStages"
})
public class Job {

@JsonProperty("jobId")
private Integer jobId;
@JsonProperty("name")
private String name;
@JsonProperty("submissionTime")
private String submissionTime;
@JsonProperty("completionTime")
private String completionTime;
@JsonProperty("stageIds")
private List<Integer> stageIds = null;
@JsonProperty("status")
private String status;
@JsonProperty("numTasks")
private Integer numTasks;
@JsonProperty("numActiveTasks")
private Integer numActiveTasks;
@JsonProperty("numCompletedTasks")
private Integer numCompletedTasks;
@JsonProperty("numSkippedTasks")
private Integer numSkippedTasks;
@JsonProperty("numFailedTasks")
private Integer numFailedTasks;
@JsonProperty("numActiveStages")
private Integer numActiveStages;
@JsonProperty("numCompletedStages")
private Integer numCompletedStages;
@JsonProperty("numSkippedStages")
private Integer numSkippedStages;
@JsonProperty("numFailedStages")
private Integer numFailedStages;

@JsonProperty("jobId")
public Integer getJobId() {
return jobId;
}

@JsonProperty("jobId")
public void setJobId(Integer jobId) {
this.jobId = jobId;
}

@JsonProperty("name")
public String getName() {
return name;
}

@JsonProperty("name")
public void setName(String name) {
this.name = name;
}

@JsonProperty("submissionTime")
public String getSubmissionTime() {
return submissionTime;
}

@JsonProperty("submissionTime")
public void setSubmissionTime(String submissionTime) {
this.submissionTime = submissionTime;
}

@JsonProperty("completionTime")
public String getCompletionTime() {
return completionTime;
}

@JsonProperty("completionTime")
public void setCompletionTime(String completionTime) {
this.completionTime = completionTime;
}

@JsonProperty("stageIds")
public List<Integer> getStageIds() {
return stageIds;
}

@JsonProperty("stageIds")
public void setStageIds(List<Integer> stageIds) {
this.stageIds = stageIds;
}

@JsonProperty("status")
public String getStatus() {
return status;
}

@JsonProperty("status")
public void setStatus(String status) {
this.status = status;
}

@JsonProperty("numTasks")
public Integer getNumTasks() {
return numTasks;
}

@JsonProperty("numTasks")
public void setNumTasks(Integer numTasks) {
this.numTasks = numTasks;
}

@JsonProperty("numActiveTasks")
public Integer getNumActiveTasks() {
return numActiveTasks;
}

@JsonProperty("numActiveTasks")
public void setNumActiveTasks(Integer numActiveTasks) {
this.numActiveTasks = numActiveTasks;
}

@JsonProperty("numCompletedTasks")
public Integer getNumCompletedTasks() {
return numCompletedTasks;
}

@JsonProperty("numCompletedTasks")
public void setNumCompletedTasks(Integer numCompletedTasks) {
this.numCompletedTasks = numCompletedTasks;
}

@JsonProperty("numSkippedTasks")
public Integer getNumSkippedTasks() {
return numSkippedTasks;
}

@JsonProperty("numSkippedTasks")
public void setNumSkippedTasks(Integer numSkippedTasks) {
this.numSkippedTasks = numSkippedTasks;
}

@JsonProperty("numFailedTasks")
public Integer getNumFailedTasks() {
return numFailedTasks;
}

@JsonProperty("numFailedTasks")
public void setNumFailedTasks(Integer numFailedTasks) {
this.numFailedTasks = numFailedTasks;
}

@JsonProperty("numActiveStages")
public Integer getNumActiveStages() {
return numActiveStages;
}

@JsonProperty("numActiveStages")
public void setNumActiveStages(Integer numActiveStages) {
this.numActiveStages = numActiveStages;
}

@JsonProperty("numCompletedStages")
public Integer getNumCompletedStages() {
return numCompletedStages;
}

@JsonProperty("numCompletedStages")
public void setNumCompletedStages(Integer numCompletedStages) {
this.numCompletedStages = numCompletedStages;
}

@JsonProperty("numSkippedStages")
public Integer getNumSkippedStages() {
return numSkippedStages;
}

@JsonProperty("numSkippedStages")
public void setNumSkippedStages(Integer numSkippedStages) {
this.numSkippedStages = numSkippedStages;
}

@JsonProperty("numFailedStages")
public Integer getNumFailedStages() {
return numFailedStages;
}

@JsonProperty("numFailedStages")
public void setNumFailedStages(Integer numFailedStages) {
this.numFailedStages = numFailedStages;
}

}