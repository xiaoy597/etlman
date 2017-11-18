package io.jacob.etlman.metastore;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by xiaoy on 1/3/2017.
 */
public class ETLTask {
    private String taskName;
    private String schemaName;
    private String developerName;
    private String comments;
    private String sysName;
    private ETLEntity etlEntity;

    public String getTaskName() {
        return taskName;
    }

    public void setTaskName(String taskName) {
        this.taskName = taskName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getDeveloperName() {
        return developerName;
    }

    public void setDeveloperName(String developerName) {
        this.developerName = developerName;
    }

    public String getComments() {
        return comments;
    }

    public void setComments(String comments) {
        this.comments = comments;
    }

    public String getSysName() {
        return sysName;
    }

    public void setSysName(String sysName) {
        this.sysName = sysName;
    }

    public ETLEntity getEtlEntity() {
        return etlEntity;
    }

    public void setEtlEntity(ETLEntity etlEntity) {
        this.etlEntity = etlEntity;
    }

    public String toString(){
        return "ETL Task: " + taskName + ", developer=" + developerName + "\n" + etlEntity.toString();
    }
}
