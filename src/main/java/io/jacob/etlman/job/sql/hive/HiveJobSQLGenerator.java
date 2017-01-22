package io.jacob.etlman.job.sql.hive;

import io.jacob.etlman.job.sql.JobSQLGenerator;
import io.jacob.etlman.job.sql.JobSQLGeneratorConfig;
import io.jacob.etlman.metastore.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.lang.System.exit;

/**
 * Created by xiaoy on 1/5/2017.
 */
public class HiveJobSQLGenerator extends JobSQLGenerator {

    private String workingTable;
    private int numGroupBlock;

    public HiveJobSQLGenerator(ETLTask etlTask) {
        super(etlTask);
    }

    @Override
    protected String genJobPreprocess() {
        StringBuilder buffer = new StringBuilder();

        buffer.append("\nUSE ").append(etlTask.getEtlEntity().getSchemaName()).append(";");

        if (hasIncSourceData && etlTask.getEtlEntity().getLoadMode().equals("更新")) {
            workingTable = etlTask.getEtlEntity().getPhyTableName() + "_" + JobSQLGeneratorConfig.workDateVarName;
            buffer.append("\nDROP TABLE IF EXISTS ").append(workingTable).append(";");
            buffer.append("\nCREATE TABLE ").append(workingTable).append(" LIKE ").append(etlTask.getEtlEntity().getPhyTableName()).append(";");
        } else
            workingTable = etlTask.getEtlEntity().getPhyTableName();

        buffer.append("\nALTER TABLE ").append(workingTable).append(
                String.format(" DROP IF EXISTS PARTITION (%s = '%s');", JobSQLGeneratorConfig.loadDateColName, JobSQLGeneratorConfig.workDateVarName));

        return buffer.toString();
    }

    @Override
    protected String genJobPostprocess() {
        StringBuilder buffer = new StringBuilder();

        if (!(hasIncSourceData && etlTask.getEtlEntity().getLoadMode().equals("更新"))) {
            return "";
        }

        buffer.append("\nINSERT OVERWRITE TABLE ").append(etlTask.getEtlEntity().getPhyTableName());
        buffer.append(String.format("\nPARTITION (%s = '%s'", JobSQLGeneratorConfig.loadDateColName, JobSQLGeneratorConfig.workDateVarName));

        for (ETLEntityAttribute partKey : getPartitionKeys())
            buffer.append(", ").append(partKey.getPhyName());
        buffer.append(")");

        boolean isFirstColumn = true;
        buffer.append("\nSELECT ");
        for (ETLEntityAttribute column : getNonPartitionKeys()) {
            if (!isFirstColumn)
                buffer.append("\n, ");
            else
                buffer.append("\n");

            buffer.append("coalesce(t1.").append(column.getPhyName()).append(", t2.").append(column.getPhyName()).append(")");
            isFirstColumn = false;
        }

        for (ETLEntityAttribute column : getPartitionKeys()) {
            buffer.append("\n, ").append("coalesce(t1.").append(column.getPhyName()).append(", t2.").append(column.getPhyName()).append(")");
        }

        buffer.append("\nFROM ").append(workingTable).append(" t1 FULL OUTER JOIN ");

        buffer.append("(SELECT * FROM ").append(etlTask.getEtlEntity().getPhyTableName())
                .append(String.format(" WHERE %s = '%s')", JobSQLGeneratorConfig.loadDateColName, JobSQLGeneratorConfig.lastWorkDateVarName))
                .append(" t2");

        buffer.append("\nON");

        isFirstColumn = true;
        for (ETLEntityAttribute column : getPrimaryKeys()) {
            if (!isFirstColumn)
                buffer.append(" AND");
            buffer.append(" t1.").append(column.getPhyName()).append(" =").append(" t2.").append(column.getPhyName());
            isFirstColumn = false;
        }

        buffer.append(";");

        buffer.append("\nDROP TABLE IF EXISTS ").append(workingTable).append(";");

        return buffer.toString();
    }

    @Override
    protected String genBatchBody(ETLLoadBatch loadBatch) throws Exception {
        StringBuilder buffer = new StringBuilder();
        StringBuilder buffer4Groups = new StringBuilder();

        numGroupBlock = 0;
        buffer.append(genFromClause(loadBatch));

        for (ETLLoadGroup loadGroup : loadBatch.getLoadGroupList())
            buffer4Groups.append(genGroupScript(loadGroup));

        if (numGroupBlock > 0) {
            buffer.append(buffer4Groups);
            buffer.append("\n;");
        }else
            buffer = buffer4Groups;

        return buffer.toString();
    }

    @Override
    protected String genGroupBody(ETLLoadGroup loadGroup) throws Exception {
        StringBuilder buffer = new StringBuilder();

        buffer.append("\nINSERT INTO TABLE ").append(workingTable);
        buffer.append(String.format("\nPARTITION (%s = '%s'", JobSQLGeneratorConfig.loadDateColName, JobSQLGeneratorConfig.workDateVarName));
        for (ETLEntityAttribute partKey : getPartitionKeys())
            buffer.append(", ").append(partKey.getPhyName());
        buffer.append(")");

        boolean isFirstColumn = true;
        buffer.append("\nSELECT ");
        for (ETLEntityAttribute column : getNonPartitionKeys()) {
            if (!isFirstColumn)
                buffer.append("\n,");
            else
                buffer.append("\n");
            ETLColumnMapping mapping = getMapping(loadGroup, column);
            if (mapping == null){
                buffer.delete(0, buffer.length());
                buffer.append("\n-- Fatal Error, mapping not found for group: " + loadGroup.getLoadGroup() +
                        ", column: " + column.getColumnName() + " !");
                return buffer.toString();
            }

            if ((mapping.getExpression() == null || mapping.getExpression().trim().length() == 0)
                && mapping.getSrcColumnList().get(0).getColumnName().equals("")){
                System.out.println(String.format("Entity: %s, Batch: %d, Group: %d, Expression for Mapping [%s] <- [%s] is not specified.",
                        etlTask.getEtlEntity().getEntityName(), loadGroup.getLoadBatch().getLoadBatch(), loadGroup.getLoadGroup(),
                        mapping.getEntityAttribute().getColumnName(), mapping.getSrcColumnList().get(0).getColumnName()));
                exit(-1);
            }

            if (mapping.getExpression().trim().length() == 0) {
                buffer.append(getQualifiedColumnName(loadGroup.getLoadBatch().getLoadBatch(),
                        mapping.getSrcColumnList().get(0)));
            } else
                buffer.append(mapping.getExpression());

            buffer.append(" -- ").append(mapping.getEntityAttribute().getColumnName());
            isFirstColumn = false;
        }

        for (ETLEntityAttribute column : getPartitionKeys()) {
            buffer.append("\n,");
            ETLColumnMapping mapping = getMapping(loadGroup, column);

            if ((mapping.getExpression() == null || mapping.getExpression().trim().length() == 0)
                    && mapping.getSrcColumnList().get(0).getColumnName().equals("")){
                System.out.println(String.format("Entity: %s, Batch: %d, Group: %d, Expression for Mapping [%s] <- [%s] is not specified.",
                        etlTask.getEtlEntity().getEntityName(), loadGroup.getLoadBatch().getLoadBatch(), loadGroup.getLoadGroup(),
                        mapping.getEntityAttribute().getColumnName(), mapping.getSrcColumnList().get(0).getColumnName()));
                exit(-1);
            }

            if (mapping.getExpression().trim().length() == 0) {
                buffer.append(getQualifiedColumnName(loadGroup.getLoadBatch().getLoadBatch(),
                        mapping.getSrcColumnList().get(0)));
            } else
                buffer.append(mapping.getExpression());

            buffer.append(" -- ").append(mapping.getEntityAttribute().getColumnName());
        }

        buffer.append(genWhereClause(loadGroup));

        numGroupBlock += 1;

        return buffer.toString();
    }

    private String genWhereClause(ETLLoadGroup loadGroup) {
        StringBuilder buffer = new StringBuilder();

        List<ETLSourceTable> sourceTableList = new ArrayList<ETLSourceTable>();
        sourceTableList.addAll(etlTask.getEtlEntity().getCommonSourceTables());
        sourceTableList.addAll(loadGroup.getLoadBatch().getSourceTableList());

        // Sort the involved tables according to their join order.
        Collections.sort(sourceTableList, new ETLSourceTable.ETLSourceTableComparator());

        buffer.append("\nWHERE");
        boolean isFirstTable = true;
        int filterLength = 0;
        for (ETLSourceTable sourceTable : sourceTableList) {
            if (!isFirstTable)
                buffer.append("\nAND");

            int groupIdx = 0;
            for (ETLLoadGroup group : loadGroup.getLoadBatch().getLoadGroupList()) {
                if (group.getLoadGroup() == loadGroup.getLoadGroup())
                    break;
                groupIdx++;
            }

            String[] fileterConditions = sourceTable.getFilterCondition().split(";");
            if (groupIdx > fileterConditions.length - 1)
                groupIdx = fileterConditions.length - 1;

            buffer.append(" ").append("(").append(fileterConditions[groupIdx]).append(")");
            isFirstTable = false;

            filterLength += sourceTable.getFilterCondition().trim().length();
        }

        if (filterLength > 0)
            return buffer.toString();
        else
            return "";
    }

    private String genFromClause(ETLLoadBatch loadBatch) {
        StringBuilder buffer = new StringBuilder();

        buffer.append("\nFROM ");

        List<ETLSourceTable> sourceTableList = new ArrayList<ETLSourceTable>();
        sourceTableList.addAll(etlTask.getEtlEntity().getCommonSourceTables());
        sourceTableList.addAll(loadBatch.getSourceTableList());

        // Sort the involved tables according to their join order.
        Collections.sort(sourceTableList, new ETLSourceTable.ETLSourceTableComparator());

//        boolean allInnerJoin = true;
//        for (ETLSourceTable sourceTable : sourceTableList)
//            if (sourceTable.getJoinType().trim().length() > 0
//                    && !sourceTable.getJoinType().toLowerCase().contains("inner")) {
//                allInnerJoin = false;
//                break;
//            }

        boolean isFirstTable = true;
        for (ETLSourceTable sourceTable : sourceTableList) {
            buffer.append("\n");

            if (isFirstTable) {
                buffer.append(sourceTable.getInterfaceTable()).append(" ").append(sourceTable.getTableAlias());
                isFirstTable = false;
                continue;
            }

            String joinType = sourceTable.getJoinType().trim().toLowerCase();
            if (joinType.length() == 0 || joinType.contains("inner"))
                buffer.append(" INNER JOIN");
            else if (joinType.contains("left"))
                buffer.append(" LEFT OUTER JOIN");
            else if (joinType.contains("right"))
                buffer.append(" RIGHT OUTER JOIN");
            else if (joinType.contains("full"))
                buffer.append(" FULL OUTER JOIN");

            buffer.append(" ").append(sourceTable.getInterfaceTable()).append(" ").append(sourceTable.getTableAlias());

            buffer.append("\nON");

            buffer.append(" ").append(sourceTable.getJoinCondition());
        }

        return buffer.toString();
    }

    private List<ETLEntityAttribute> getPartitionKeys() {
        List<ETLEntityAttribute> partitionKeys = new ArrayList<ETLEntityAttribute>();

        for (ETLEntityAttribute attribute : etlTask.getEtlEntity().getEtlEntityAttributes()) {
            if (attribute.isPartitionKey())
                partitionKeys.add(attribute);
        }

        return partitionKeys;
    }

    private List<ETLEntityAttribute> getNonPartitionKeys() {
        List<ETLEntityAttribute> nonPartitionKeys = new ArrayList<ETLEntityAttribute>();

        for (ETLEntityAttribute attribute : etlTask.getEtlEntity().getEtlEntityAttributes()) {
            if (!attribute.isPartitionKey())
                nonPartitionKeys.add(attribute);
        }

        return nonPartitionKeys;
    }

    private List<ETLEntityAttribute> getPrimaryKeys() {
        List<ETLEntityAttribute> primaryKeys = new ArrayList<ETLEntityAttribute>();

        for (ETLEntityAttribute attribute : etlTask.getEtlEntity().getEtlEntityAttributes()) {
            if (attribute.isPK())
                primaryKeys.add(attribute);
        }

        return primaryKeys;
    }

    private ETLColumnMapping getMapping(ETLLoadGroup loadGroup, ETLEntityAttribute attribute) throws Exception {

        for (ETLColumnMapping mapping : loadGroup.getColumnMappings())
            if (mapping.getEntityAttribute().getColumnName().equals(attribute.getColumnName()))
                return mapping;

        ETLLoadGroup defaultGroup = null;
        ETLLoadBatch loadBatch = loadGroup.getLoadBatch();
        for (ETLLoadGroup group : loadBatch.getLoadGroupList())
            if (group.getLoadGroup() == 0) {
                defaultGroup = group;
                break;
            }

        if (defaultGroup == null)
            throw new Exception("Default load group was not found!");

        for (ETLColumnMapping mapping : defaultGroup.getColumnMappings())
            if (mapping.getEntityAttribute().getColumnName().equals(attribute.getColumnName()))
                return mapping;

        // No mapping found, this shouldn't happen.
        return null;
    }

    private String getQualifiedColumnName(int loadBatch, ETLSourceColumn sourceColumn) {
        List<ETLSourceTable> sourceTables = null;

        for (ETLLoadBatch etlLoadBatch : etlTask.getEtlEntity().getEtlLoadBatches())
            if (etlLoadBatch.getLoadBatch() == loadBatch) {
                sourceTables = etlLoadBatch.getSourceTableList();
                break;
            }

        for (ETLSourceTable sourceTable : sourceTables) {
            if (sourceTable.getSysName().equals(sourceColumn.getSysName())
                    && sourceTable.getSchemaName().equals(sourceColumn.getSchemaName())
                    && sourceTable.getTableName().equals(sourceColumn.getTableName())) {
                if (sourceTable.getTableAlias().trim().length() > 0)
                    return sourceTable.getTableAlias().trim() + "." + sourceColumn.getColumnName();
                else
                    return sourceTable.getInterfaceTable().trim() + "." + sourceColumn.getColumnName();
            }
        }

        return null;

    }
}
