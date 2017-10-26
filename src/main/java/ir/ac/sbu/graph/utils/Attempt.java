package ir.ac.sbu.graph.utils;

import java.util.HashMap;
import java.util.Map;
import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
"startTime",
"endTime",
"lastUpdated",
"duration",
"sparkUser",
"completed",
"startTimeEpoch",
"endTimeEpoch",
"lastUpdatedEpoch"
})

public class Attempt {

@JsonProperty("startTime")
private String startTime;
@JsonProperty("endTime")
private String endTime;
@JsonProperty("lastUpdated")
private String lastUpdated;
@JsonProperty("duration")
private Integer duration;
@JsonProperty("sparkUser")
private String sparkUser;
@JsonProperty("completed")
private Boolean completed;
@JsonProperty("startTimeEpoch")
private Integer startTimeEpoch;
@JsonProperty("endTimeEpoch")
private Integer endTimeEpoch;
@JsonProperty("lastUpdatedEpoch")
private Integer lastUpdatedEpoch;
@JsonIgnore
private Map<String, Object> additionalProperties = new HashMap<String, Object>();

@JsonProperty("startTime")
public String getStartTime() {
return startTime;
}

@JsonProperty("startTime")
public void setStartTime(String startTime) {
this.startTime = startTime;
}

@JsonProperty("endTime")
public String getEndTime() {
return endTime;
}

@JsonProperty("endTime")
public void setEndTime(String endTime) {
this.endTime = endTime;
}

@JsonProperty("lastUpdated")
public String getLastUpdated() {
return lastUpdated;
}

@JsonProperty("lastUpdated")
public void setLastUpdated(String lastUpdated) {
this.lastUpdated = lastUpdated;
}

@JsonProperty("duration")
public Integer getDuration() {
return duration;
}

@JsonProperty("duration")
public void setDuration(Integer duration) {
this.duration = duration;
}

@JsonProperty("sparkUser")
public String getSparkUser() {
return sparkUser;
}

@JsonProperty("sparkUser")
public void setSparkUser(String sparkUser) {
this.sparkUser = sparkUser;
}

@JsonProperty("completed")
public Boolean getCompleted() {
return completed;
}

@JsonProperty("completed")
public void setCompleted(Boolean completed) {
this.completed = completed;
}

@JsonProperty("startTimeEpoch")
public Integer getStartTimeEpoch() {
return startTimeEpoch;
}

@JsonProperty("startTimeEpoch")
public void setStartTimeEpoch(Integer startTimeEpoch) {
this.startTimeEpoch = startTimeEpoch;
}

@JsonProperty("endTimeEpoch")
public Integer getEndTimeEpoch() {
return endTimeEpoch;
}

@JsonProperty("endTimeEpoch")
public void setEndTimeEpoch(Integer endTimeEpoch) {
this.endTimeEpoch = endTimeEpoch;
}

@JsonProperty("lastUpdatedEpoch")
public Integer getLastUpdatedEpoch() {
return lastUpdatedEpoch;
}

@JsonProperty("lastUpdatedEpoch")
public void setLastUpdatedEpoch(Integer lastUpdatedEpoch) {
this.lastUpdatedEpoch = lastUpdatedEpoch;
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