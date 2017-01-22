package io.jacob.etlman.metastore;

import java.util.Comparator;

/**
 * Created by xiaoy on 1/3/2017.
 */
public class ETLSourceTable {
    private int loadBatch;
    private String schemaName;
    private String tableName;
    private String sysName;
    private int rowCount;
    private String interfaceTable;
    private boolean isIncExtract;
    private String tableAlias;
    private int joinOrder;
    private String joinType;
    private String joinCondition;
    private String filterCondition;
    private String comments;

    public String toString() {

        return "Source Table: " + sysName + "." + schemaName + "." + tableName + "\n"
                + ">>>alias:" + tableAlias + ",join order:" + joinOrder + ",join type:" + joinType + "\n"
                + ">>>join condition:" + joinCondition + "\n"
                + ">>>filter condition:" + filterCondition + "\n"
                + ">>>interface:" + interfaceTable + ",row count:" + rowCount + ", incExt:" + isIncExtract + "\n"
                + ">>>comments:" + comments + "\n";
    }

    public int getRowCount() {
        return rowCount;
    }

    public void setRowCount(int rowCount) {
        this.rowCount = rowCount;
    }

    public String getInterfaceTable() {
        return interfaceTable;
    }

    public void setInterfaceTable(String interfaceTable) {
        this.interfaceTable = interfaceTable;
    }

    public int getLoadBatch() {
        return loadBatch;
    }

    public void setLoadBatch(int loadBatch) {
        this.loadBatch = loadBatch;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getSysName() {
        return sysName;
    }

    public void setSysName(String sysName) {
        this.sysName = sysName;
    }

    public String getTableAlias() {
        return tableAlias;
    }

    public void setTableAlias(String tableAlias) {
        this.tableAlias = tableAlias;
    }

    public int getJoinOrder() {
        return joinOrder;
    }

    public void setJoinOrder(int joinOrder) {
        this.joinOrder = joinOrder;
    }

    public String getJoinType() {
        return joinType;
    }

    public void setJoinType(String joinType) {
        this.joinType = joinType;
    }

    public String getJoinCondition() {
        return joinCondition;
    }

    public void setJoinCondition(String joinCondition) {
        this.joinCondition = joinCondition;
    }

    public String getFilterCondition() {
        return filterCondition;
    }

    public void setFilterCondition(String filterCondition) {
        this.filterCondition = filterCondition;
    }

    public String getComments() {
        return comments;
    }

    public void setComments(String comments) {
        this.comments = comments;
    }

    public boolean isIncExtract() {
        return isIncExtract;
    }

    public void setIncExtract(boolean incExtract) {
        isIncExtract = incExtract;
    }

    public static class ETLSourceTableComparator implements Comparator<ETLSourceTable> {
        @Override
        public int compare(ETLSourceTable t1, ETLSourceTable t2){
            if (t1.getJoinOrder() < t2.getJoinOrder())
                return -1;
            else if (t1.getJoinOrder() == t2.getJoinOrder())
                return t1.getJoinType().compareTo(t2.getJoinType());
            else
                return 1;
        }
    }
}
