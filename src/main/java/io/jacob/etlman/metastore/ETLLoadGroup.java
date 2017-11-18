package io.jacob.etlman.metastore;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by xiaoy on 1/3/2017.
 */
public class ETLLoadGroup {
    private int loadGroup;
    private List<ETLColumnMapping> columnMappings = new ArrayList<ETLColumnMapping>();
    private ETLLoadBatch loadBatch;
    private String workingTable;

    public String getWorkingTable() {
        return workingTable;
    }

    public void setWorkingTable(String workingTable) {
        this.workingTable = workingTable;
    }

    public ETLLoadBatch getLoadBatch() {
        return loadBatch;
    }

    public void setLoadBatch(ETLLoadBatch loadBatch) {
        this.loadBatch = loadBatch;
    }

    public int getLoadGroup() {
        return loadGroup;
    }

    public void setLoadGroup(int loadGroup) {
        this.loadGroup = loadGroup;
    }

    public List<ETLColumnMapping> getColumnMappings() {
        return columnMappings;
    }

    public void setColumnMappings(List<ETLColumnMapping> columnMappings) {
        this.columnMappings = columnMappings;
    }

    public String toString(){
        StringBuilder buffer = new StringBuilder();

        buffer.append("Load Group: ").append(loadGroup).append(" >>>>>").append("\n");
        for (ETLColumnMapping mapping : columnMappings)
            buffer.append(mapping.toString());
        buffer.append("Load Group: ").append(loadGroup).append(" <<<<<").append("\n");

        return buffer.toString();
    }
}
