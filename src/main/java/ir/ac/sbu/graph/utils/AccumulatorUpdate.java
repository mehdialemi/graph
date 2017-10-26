package ir.ac.sbu.graph.utils;

import com.fasterxml.jackson.annotation.*;

import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
"id",
"name",
"value"
})
public class AccumulatorUpdate {

@JsonProperty("id")
private Long id;
@JsonProperty("name")
private String name;
@JsonProperty("value")
private String value;
@JsonIgnore
private Map<String, Object> additionalProperties = new HashMap<String, Object>();

@JsonProperty("id")
public Long getId() {
return id;
}

@JsonProperty("id")
public void setId(Long id) {
this.id = id;
}

@JsonProperty("name")
public String getName() {
return name;
}

@JsonProperty("name")
public void setName(String name) {
this.name = name;
}

@JsonProperty("value")
public String getValue() {
return value;
}

@JsonProperty("value")
public void setValue(String value) {
this.value = value;
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
