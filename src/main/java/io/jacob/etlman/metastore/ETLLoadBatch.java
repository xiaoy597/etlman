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
public class ETLLoadBatch {
    private int loadBatch;
    private List<ETLSourceTable> sourceTableList = new ArrayList<ETLSourceTable>();
    private List<ETLLoadGroup> loadGroupList = new ArrayList<ETLLoadGroup>();
    private ETLEntity entity;

    public List<ETLLoadGroup> getLoadGroupList() {
        return loadGroupList;
    }

    public void setLoadGroupList(List<ETLLoadGroup> loadGroupList) {
        this.loadGroupList = loadGroupList;
    }

    public int getLoadBatch() {
        return loadBatch;
    }

    public void setLoadBatch(int loadBatch) {
        this.loadBatch = loadBatch;
    }

    public List<ETLSourceTable> getSourceTableList() {
        return sourceTableList;
    }

    public void setSourceTableList(List<ETLSourceTable> sourceTableList) {
        this.sourceTableList = sourceTableList;
    }

    public void initialize(ETLEntity entity) throws Exception {
        this.entity = entity;

        Connection connection = ETLMan.getMetaDBConn();

        Statement statement = connection.createStatement();

        ResultSet rs = statement.executeQuery("select column_name, load_group, src_table_name, src_column_name, "
                + "src_sys_name, src_schema, column_expr, comments "
                + "from dw_column_mapping where "
                + "schema_name = '" + entity.getSchemaName() + "' and table_name = '" + entity.getEntityName() + "' "
                + "and load_batch = " + loadBatch + " "
                + "order by load_group");

        ETLLoadGroup etlLoadGroup = null;
        ETLColumnMapping columnMapping = null;
        while (rs.next()) {
            int loadGroup = rs.getInt("load_group");
            if (etlLoadGroup == null || etlLoadGroup.getLoadGroup() != loadGroup) {
                // Create a new load group.
                ETLLoadGroup newLoadGroup = new ETLLoadGroup();
                newLoadGroup.setLoadGroup(loadGroup);
                newLoadGroup.setLoadBatch(this);

                if (etlLoadGroup != null) {
                    etlLoadGroup.getColumnMappings().add(columnMapping);
                    columnMapping = null;
                    loadGroupList.add(etlLoadGroup);
                }
                etlLoadGroup = newLoadGroup;
            }

            String columnName = rs.getString("column_name");
            if (columnMapping == null || !columnMapping.getEntityAttribute().getColumnName().equals(columnName)) {
                // Create a new column mapping.
                ETLColumnMapping newColumnMapping = new ETLColumnMapping();

                ETLEntityAttribute entityAttribute = new ETLEntityAttribute();
                entityAttribute.setSchemaName(entity.getSchemaName());
                entityAttribute.setTableName(entity.getEntityName());
                entityAttribute.setColumnName(columnName);

                newColumnMapping.setEntityAttribute(entityAttribute);

                if (columnMapping != null)
                    etlLoadGroup.getColumnMappings().add(columnMapping);

                columnMapping = newColumnMapping;
            }

            ETLSourceColumn sourceColumn = new ETLSourceColumn();
            sourceColumn.setSysName(rs.getString("src_sys_name"));
            sourceColumn.setSchemaName(rs.getString("src_schema"));
            sourceColumn.setTableName(rs.getString("src_table_name"));
            sourceColumn.setColumnName(rs.getString("src_column_name"));

            columnMapping.getSrcColumnList().add(sourceColumn);
            columnMapping.setExpression(rs.getString("column_expr"));
            columnMapping.setComments(rs.getString("comments"));
        }

        rs.close();

        if (etlLoadGroup != null) {
            etlLoadGroup.getColumnMappings().add(columnMapping);
            loadGroupList.add(etlLoadGroup);
        }

    }

    public String toString(){
        StringBuilder buffer = new StringBuilder();

        buffer.append("Load Batch: ").append(loadBatch).append(" >>>>>\n");

        buffer.append("Source Tables: \n");
        for (ETLSourceTable sourceTable : sourceTableList)
            buffer.append(sourceTable.toString());
        buffer.append("Load Groups: \n");
        for (ETLLoadGroup loadGroup : loadGroupList)
            buffer.append(loadGroup.toString());

        buffer.append("Load Batch: ").append(loadBatch).append(" <<<<<\n");

        return buffer.toString();
    }
}
