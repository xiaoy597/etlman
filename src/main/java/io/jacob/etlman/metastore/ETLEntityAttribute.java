package io.jacob.etlman.metastore;

/**
 * Created by xiaoy on 1/3/2017.
 */
public class ETLEntityAttribute {

    private String columnName;
    private String tableName;
    private String schemaName;
    private String sysName;
    private String phyName;
    private int columnId;
    private String dataType;
    private String aggPeriod;
    private boolean isPK;
    private boolean chainCompare;
    private int isPartitionKey;
    private String comments;

    public String toString(){
        return "Entity Attribute: " + tableName + "." + columnName + ", phyName=" + phyName + ", columnId=" + columnId + ", dataType=" + dataType
                + ", isPK=" + isPK + ", isPartKey=" + isPartitionKey + ", chainComp=" + chainCompare + "\n";
    }

    public int getColumnId() {
        return columnId;
    }

    public void setColumnId(int columnId) {
        this.columnId = columnId;
    }

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public String getAggPeriod() {
        return aggPeriod;
    }

    public void setAggPeriod(String aggPeriod) {
        this.aggPeriod = aggPeriod;
    }

    public boolean isPK() {
        return isPK;
    }

    public void setPK(boolean PK) {
        isPK = PK;
    }

    public boolean isChainCompare() {
        return chainCompare;
    }

    public void setChainCompare(boolean chainCompare) {
        this.chainCompare = chainCompare;
    }

    public int getPartitionKey() {
        return isPartitionKey;
    }

    public void setPartitionKey(int partitionKey) {
        isPartitionKey = partitionKey;
    }

    public String getComments() {
        return comments;
    }

    public void setComments(String comments) {
        this.comments = comments;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getSysName() {
        return sysName;
    }

    public void setSysName(String sysName) {
        this.sysName = sysName;
    }

    public String getPhyName() {
        return phyName;
    }

    public void setPhyName(String phyName) {
        this.phyName = phyName;
    }

}
