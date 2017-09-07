package io.jacob.etlman.metastore;

import io.jacob.etlman.ETLMan;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by xiaoy on 1/3/2017.
 */
public class ETLEntity {
    private String schemaName;
    private String entityName;
    private String phyTableName;
    private String loadMode;
    private String clearMode;
    private boolean keepLoadDate;
    private boolean doAggregate;
    private boolean isFactTable;
    private boolean isSingleSource;
    private String subjectName;
    private String comments;

    private List<ETLEntityAttribute> etlEntityAttributes = new ArrayList<ETLEntityAttribute>();
    private List<ETLSourceTable> commonSourceTables = new ArrayList<ETLSourceTable>();
    private List<ETLLoadBatch> etlLoadBatches = new ArrayList<ETLLoadBatch>();

    public List<ETLEntityAttribute> getEtlEntityAttributes() {
        return etlEntityAttributes;
    }

    public void setEtlEntityAttributes(List<ETLEntityAttribute> etlEntityAttributes) {
        this.etlEntityAttributes = etlEntityAttributes;
    }

    public List<ETLSourceTable> getCommonSourceTables() {
        return commonSourceTables;
    }

    public void setCommonSourceTables(List<ETLSourceTable> commonSourceTables) {
        this.commonSourceTables = commonSourceTables;
    }

    public List<ETLLoadBatch> getEtlLoadBatches() {
        return etlLoadBatches;
    }

    public void setEtlLoadBatches(List<ETLLoadBatch> etlLoadBatches) {
        this.etlLoadBatches = etlLoadBatches;
    }

    public String toString() {
        StringBuilder buffer = new StringBuilder();

        buffer.append("Entity: ").append(schemaName).append(".").append(entityName).append(" >>>>>").append("\n");
        buffer.append(">>> Physical Name: ").append(phyTableName).append("\n");
        buffer.append(">>> Load Mode: ").append(loadMode).append("\n");
        buffer.append(">>> Clear Mode: ").append(clearMode).append("\n");
        buffer.append(">>> Keep Load Date: ").append(keepLoadDate).append("\n");

        buffer.append(">>> Attribute List:\n");
        for (ETLEntityAttribute attribute : etlEntityAttributes)
            buffer.append(attribute.toString());

        buffer.append(">>> Common Source Tables:\n");
        for (ETLSourceTable sourceTable : commonSourceTables)
            buffer.append(sourceTable.toString());

        buffer.append(">>> Load Batches:\n");
        for (ETLLoadBatch loadBatch : etlLoadBatches)
            buffer.append(loadBatch.toString());

        buffer.append("Entity: ").append(schemaName).append(".").append(entityName).append(" <<<<<").append("\n");

        return buffer.toString();
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getEntityName() {
        return entityName;
    }

    public void setEntityName(String entityName) {
        this.entityName = entityName;
    }

    public String getPhyTableName() {
        return phyTableName;
    }

    public void setPhyTableName(String phyTableName) {
        this.phyTableName = phyTableName;
    }

    public String getLoadMode() {
        return loadMode;
    }

    public void setLoadMode(String loadMode) {
        this.loadMode = loadMode;
    }

    public String getClearMode() {
        return clearMode;
    }

    public void setClearMode(String clearMode) {
        this.clearMode = clearMode;
    }

    public boolean isKeepLoadDate() {
        return keepLoadDate;
    }

    public void setKeepLoadDate(boolean keepLoadDate) {
        this.keepLoadDate = keepLoadDate;
    }

    public boolean isDoAggregate() {
        return doAggregate;
    }

    public void setDoAggregate(boolean doAggregate) {
        this.doAggregate = doAggregate;
    }

    public boolean isFactTable() {
        return isFactTable;
    }

    public void setFactTable(boolean factTable) {
        isFactTable = factTable;
    }

    public String getSubjectName() {
        return subjectName;
    }

    public void setSubjectName(String subjectName) {
        this.subjectName = subjectName;
    }

    public boolean isSingleSource() {
        return isSingleSource;
    }

    public void setSingleSource(boolean singleSource) {
        isSingleSource = singleSource;
    }

    public String getComments() {
        return comments;
    }

    public void setComments(String comments) {
        this.comments = comments;
    }

    public ETLEntity(String schemaName, String entityName) throws Exception {
        this.entityName = entityName;
        this.schemaName = schemaName;

        initialize();
    }

    private void initialize() throws Exception {
        Connection connection = ETLMan.getMetaDBConn();

        Statement statement = connection.createStatement();

        ResultSet rs = statement.executeQuery("select * from dw_table where schema_name = '" + schemaName + "' and table_name = '" + entityName + "'");
        while(rs.next()){
            phyTableName = rs.getString("phy_name");
            loadMode = rs.getString("load_mode");
            clearMode = rs.getString("clear_mode");
            keepLoadDate = rs.getBoolean("keep_load_dt");
            doAggregate = rs.getBoolean("do_aggregate");
            isFactTable = rs.getBoolean("is_fact");
            isSingleSource = rs.getBoolean("is_single_source");
            subjectName = rs.getString("subject_name");
            comments = rs.getString("comments");

            break;
        }

        rs.close();

        rs = statement.executeQuery("select * from dw_columns "
                                    + "where schema_name = '" + schemaName + "' and table_name = '" + entityName + "' "
                                    + "order by column_id");
        while(rs.next()) {
            ETLEntityAttribute entityAttribute = new ETLEntityAttribute();

            entityAttribute.setSysName(rs.getString("sys_name"));
            entityAttribute.setSchemaName(rs.getString("schema_name"));
            entityAttribute.setTableName(rs.getString("table_name"));
            entityAttribute.setColumnName(rs.getString("column_name"));
            entityAttribute.setComments(rs.getString("comments"));
            entityAttribute.setPhyName(rs.getString("phy_name"));
            entityAttribute.setAggPeriod(rs.getString("agg_period"));
            entityAttribute.setChainCompare(rs.getBoolean("chain_compare"));
            entityAttribute.setColumnId(rs.getInt("column_id"));
            entityAttribute.setDataType(rs.getString("data_type"));
            entityAttribute.setPartitionKey(rs.getInt("is_partition_key"));
            entityAttribute.setPK(rs.getBoolean("is_pk"));

            etlEntityAttributes.add(entityAttribute);
        }

        rs.close();

        rs = statement.executeQuery("select a.*, m.load_batch, m.table_alias, m.join_order, m.join_type, m.join_condition, m.filter_condition "
                                        + "from dw_table_mapping m, src_table_analysis a "
                                        + "where m.schema_name = '" + schemaName + "' and m.table_name = '" + entityName + "' "
                                        + "and m.src_sys_name = a.sys_name and m.src_schema = a.schema_name and m.src_table_name = a.table_name "
                                        + "order by load_batch");

        ETLLoadBatch curLoadBatch = null;
        while(rs.next()){
            ETLSourceTable etlSourceTable = new ETLSourceTable();

            etlSourceTable.setSchemaName(rs.getString("schema_name"));
            etlSourceTable.setTableName(rs.getString("table_name"));
            etlSourceTable.setSysName(rs.getString("sys_name"));
            etlSourceTable.setFilterCondition(rs.getString("filter_condition"));
            etlSourceTable.setJoinType(rs.getString("join_type"));
            etlSourceTable.setJoinOrder(rs.getInt("join_order"));
            etlSourceTable.setJoinCondition(rs.getString("join_condition"));
            etlSourceTable.setTableAlias(rs.getString("table_alias"));
            etlSourceTable.setRowCount(rs.getInt("row_count"));
            etlSourceTable.setInterfaceTable(rs.getString("stbl_name"));
            etlSourceTable.setComments(rs.getString("comments"));
            etlSourceTable.setLoadBatch(rs.getInt("load_batch"));
            etlSourceTable.setIncExtract(rs.getBoolean("is_inc_ext"));

            if (etlSourceTable.getLoadBatch() == 0){
                commonSourceTables.add(etlSourceTable);
            }else{
                if (curLoadBatch == null || etlSourceTable.getLoadBatch() != curLoadBatch.getLoadBatch()){

                    if (curLoadBatch != null)
                        etlLoadBatches.add(curLoadBatch);

                    ETLLoadBatch newLoadBatch = new ETLLoadBatch();
                    newLoadBatch.setLoadBatch(etlSourceTable.getLoadBatch());

                    curLoadBatch = newLoadBatch;
                }

                curLoadBatch.getSourceTableList().add(etlSourceTable);
            }
        }

        if (curLoadBatch != null)
            etlLoadBatches.add(curLoadBatch);

        rs.close();

        for (ETLLoadBatch loadBatch : etlLoadBatches)
            loadBatch.initialize(this);
    }
}
