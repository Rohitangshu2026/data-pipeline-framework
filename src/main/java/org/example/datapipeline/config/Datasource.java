package org.example.datapipeline.config;

import jakarta.xml.bind.annotation.*;
import org.example.datapipeline.config.action.Param;
import java.util.ArrayList;
import java.util.List;

@XmlAccessorType(XmlAccessType.FIELD)
public class Datasource {
    @XmlAttribute
    private String id;

    @XmlAttribute
    private String type;

    @XmlElement(name = "param")
    private List<Param> params = new ArrayList<>();

    public String getId() { return id; }
    public String getType() { return type; }
    public List<Param> getParams() { return params; }
}
