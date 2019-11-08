package io.jacob.etlman.job.sql.hive;

import io.jacob.etlman.job.sql.JobSQLGenerator;
import io.jacob.etlman.job.sql.JobSQLGeneratorConfig;
import io.jacob.etlman.metastore.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static java.lang.System.exit;

/**
 * Created by xiaoy on 1/5/2017.
 */
public class HiveJobSQLGenerator extends JobSQLGenerator {

    private static Logger logger = LoggerFactory.getLogger(HiveJobSQLGenerator.class);
    private String workingTable;
    private int numGroupBlock;

    public HiveJobSQLGenerator(ETLTask etlTask) {
        super(etlTask);
    }

    @Override
    protected String genJobPreprocess() {

        // For batch level script, not job level pre-process script is needed.
        if (etlTask.getEtlEntity().isSingleSource())
            return "";

        StringBuilder buffer = new StringBuilder();

        buffer.append("\nUSE ").append(etlTask.getEtlEntity().getSchemaName()).append(";");

        buffer.append(genCreateWorkingTable(0));

//        buffer.append("\nALTER TABLE ").append(workingTable).append(
//                String.format(" DROP IF EXISTS PARTITION (%s = '%s');", JobSQLGeneratorConfig.loadDateColName, JobSQLGeneratorConfig.workDateVarName));

        preProcessScript = buffer.toString();
        return preProcessScript;
    }

    @Override
    protected String genJobPostprocess() {

        if (!etlTask.getEtlEntity().getLoadMode().equals("更新")) {
            return "";
        } else
            postProcessScript = genDataMerge(0);
        return postProcessScript;
    }

    @Override
    protected String genBatchPreprocess(ETLLoadBatch loadBatch) throws Exception {

        StringBuilder buffer = new StringBuilder();

        buffer.append("\n-- Script for batch ").append(loadBatch.getLoadBatch()).append(" begins ...");
        buffer.append("\nUSE ").append(etlTask.getEtlEntity().getSchemaName()).append(";");

        buffer.append(genCreateWorkingTable(loadBatch.getLoadBatch()));

        return buffer.toString();

    }

    @Override
    protected String genBatchPostprocess(ETLLoadBatch loadBatch) throws Exception {

        StringBuilder buffer = new StringBuilder();

        if (etlTask.getEtlEntity().getLoadMode().equals("更新"))
            buffer.append(genDataMerge(loadBatch.getLoadBatch()));
        else
            buffer.append(genDataAppend(loadBatch.getLoadBatch()));

        buffer.append(genDropWorkingTable(loadBatch.getLoadBatch()));

        buffer.append("\n-- Script for batch ").append(loadBatch.getLoadBatch()).append(" ends.");

        return buffer.toString();
    }


    private String genCreateWorkingTable(int batchNo) {
        StringBuilder buffer = new StringBuilder();

        for (ETLLoadBatch batch : etlTask.getEtlEntity().getEtlLoadBatches()) {
            if (batch.getLoadBatch() != batchNo && batchNo != 0)
                continue;
            for (ETLLoadGroup group : batch.getLoadGroupList()) {
                workingTable = etlTask.getEtlEntity().getPhyTableName() +
                        "_" + JobSQLGeneratorConfig.workDate8VarName +
                        "_" + batch.getLoadBatch() +
                        "_" + group.getLoadGroup();
                buffer.append("\nDROP TABLE IF EXISTS ").append(workingTable).append(";");
                buffer.append("\nCREATE TABLE ")
                        .append(workingTable)
                        .append(" LIKE ")
                        .append(etlTask.getEtlEntity().getPhyTableName())
                        .append(";");
                group.setWorkingTable(workingTable);
            }
        }

        return buffer.toString();

    }

    private String genDropWorkingTable(int batchNo) {
        StringBuilder buffer = new StringBuilder();

        for (ETLLoadBatch batch : etlTask.getEtlEntity().getEtlLoadBatches()) {
            if (batch.getLoadBatch() != batchNo && batchNo != 0)
                continue;
            for (ETLLoadGroup group : batch.getLoadGroupList()) {
                workingTable = etlTask.getEtlEntity().getPhyTableName() +
                        "_" + JobSQLGeneratorConfig.workDate8VarName +
                        "_" + batch.getLoadBatch() +
                        "_" + group.getLoadGroup();
                buffer.append("\nDROP TABLE IF EXISTS ").append(workingTable).append(";");
            }
        }

        return buffer.toString();


    }

    private void appendColumnList(StringBuilder buffer) {
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

            buffer.append(column.getPhyName());
            isFirstColumn = false;
        }

        for (ETLEntityAttribute column : getPartitionKeys()) {
            buffer.append("\n, ").append(column.getPhyName());
        }

    }

    void appendTableUpdate(StringBuilder buffer, List<String> newTables, String oldTable,
                           String partition, String targetTable) {
        buffer.append("\nINSERT OVERWRITE TABLE ").append(targetTable);
        buffer.append(String.format("\nPARTITION (%s = '%s'",
                JobSQLGeneratorConfig.loadDateColName, JobSQLGeneratorConfig.workDateVarName));

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

            buffer.append("coalesce(t1.")
                    .append(column.getPhyName())
                    .append(", t2.")
                    .append(column.getPhyName())
                    .append(")");
            isFirstColumn = false;
        }

        for (ETLEntityAttribute column : getPartitionKeys()) {
            buffer.append("\n, ")
                    .append("coalesce(t1.")
                    .append(column.getPhyName())
                    .append(", t2.")
                    .append(column.getPhyName())
                    .append(")");
        }

        StringBuilder newTableBuf = new StringBuilder();
        boolean firstTable = true;
        for (String newTable : newTables) {
            if (!firstTable)
                newTableBuf.append(" UNION ALL ");
            newTableBuf.append("SELECT * FROM " + newTable);
            firstTable = false;
        }

        buffer.append("\nFROM (")
                .append(newTableBuf.toString())
                .append(") t1 FULL OUTER JOIN ");

        buffer.append("(SELECT * FROM ")
                .append(oldTable)
                .append(String.format(" WHERE %s = '%s')",
                        JobSQLGeneratorConfig.loadDateColName, partition))
                .append(" t2");

        buffer.append("\nON");

        isFirstColumn = true;
        for (ETLEntityAttribute column : getPrimaryKeys()) {
            if (!isFirstColumn)
                buffer.append(" AND");
            buffer.append(" t1.")
                    .append(column.getPhyName())
                    .append(" =")
                    .append(" t2.")
                    .append(column.getPhyName());
            isFirstColumn = false;
        }

        buffer.append(";");

    }

    private String genDataMerge(int batchNo) {
        StringBuilder buffer = new StringBuilder();

        for (ETLLoadBatch batch : etlTask.getEtlEntity().getEtlLoadBatches()) {
            if (batchNo != 0 && batch.getLoadBatch() != batchNo)
                continue;

            List<String> groupWorkingTables = new ArrayList<String>();
            for (ETLLoadGroup group : batch.getLoadGroupList()) {
                groupWorkingTables.add(group.getWorkingTable());
            }

            String mergeTempTable = etlTask.getEtlEntity().getPhyTableName() + "_" + batch.getLoadBatch() + "_merge";

            buffer.append("\nDROP TABLE IF EXISTS ").append(mergeTempTable).append(";");
            buffer.append("\nCREATE TABLE ")
                    .append(mergeTempTable)
                    .append(" LIKE ")
                    .append(etlTask.getEtlEntity().getPhyTableName())
                    .append(";");

            // Calculate the new today's snapshot based on batch snapshot and existing today's snapshot.
            appendTableUpdate(buffer, groupWorkingTables, etlTask.getEtlEntity().getPhyTableName(),
                    JobSQLGeneratorConfig.workDateVarName, mergeTempTable);

            // Calculate the final today's snapshot based on the new today's snapshot and last day's snapshot.
            appendTableUpdate(buffer, Arrays.asList(mergeTempTable), etlTask.getEtlEntity().getPhyTableName(),
                    JobSQLGeneratorConfig.lastWorkDateVarName, etlTask.getEtlEntity().getPhyTableName());

            buffer.append("\nDROP TABLE IF EXISTS ").append(mergeTempTable).append(";");
        }

        return buffer.toString();

    }

    private String genDataAppend(int batchNo) {
        StringBuilder buffer = new StringBuilder();

        for (ETLLoadBatch batch : etlTask.getEtlEntity().getEtlLoadBatches()) {
            if (batchNo != 0 && batch.getLoadBatch() != batchNo)
                continue;

            for (ETLLoadGroup group : batch.getLoadGroupList()) {
                buffer.append("\nINSERT INTO TABLE ").append(etlTask.getEtlEntity().getPhyTableName());
                buffer.append(String.format("\nPARTITION (%s = '%s'", JobSQLGeneratorConfig.loadDateColName, JobSQLGeneratorConfig.workDateVarName));

                appendColumnList(buffer);

                buffer.append("\nFROM ").append(group.getWorkingTable());

                buffer.append(";");
            }
        }

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
        } else
            buffer = buffer4Groups;

        return buffer.toString();
    }

    @Override
    protected String genGroupBody(ETLLoadGroup loadGroup) throws Exception {
        StringBuilder buffer = new StringBuilder();

        buffer.append("\nINSERT OVERWRITE TABLE ").append(loadGroup.getWorkingTable());

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
            if (mapping == null) {
                buffer.delete(0, buffer.length());
                logger.error("Mapping not found for batch: " + loadGroup.getLoadBatch().getLoadBatch() + ", group: " + loadGroup.getLoadGroup() +
                        ", column: " + column.getColumnName() + " !");
                exit(-1);
            }

            if ((mapping.getExpression() == null || mapping.getExpression().trim().length() == 0)
                    && mapping.getSrcColumnList().get(0).getColumnName().equals("")) {
                logger.error(String.format("Entity: %s, batch: %d, group: %d, expression for mapping [%s] <- [%s] is not specified.",
                        etlTask.getEtlEntity().getEntityName(), loadGroup.getLoadBatch().getLoadBatch(), loadGroup.getLoadGroup(),
                        mapping.getEntityAttribute().getColumnName(), mapping.getSrcColumnList().get(0).getColumnName()));
                exit(-1);
            }

            if (mapping.getExpression().trim().length() == 0) {
                // Why can't we just use the source system, source schema and source table in the mapping object
                // to produce qualified column name?
                // Because we need to use table alias or interface table name to qualify column name instead of
                // the connected source system, source schema and source table name.
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
                    && mapping.getSrcColumnList().get(0).getColumnName().equals("")) {
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
            StringBuilder bufForTable = new StringBuilder();

            if (!isFirstTable)
                bufForTable.append("\nAND");

            int groupIdx = 0;
            for (ETLLoadGroup group : loadGroup.getLoadBatch().getLoadGroupList()) {
                if (group.getLoadGroup() == loadGroup.getLoadGroup())
                    break;
                groupIdx++;
            }

            // Filter conditions for different groups of this batch are separated by ';'.
            String[] filterConditions = sourceTable.getFilterCondition().split(";");
            if (groupIdx > filterConditions.length - 1)
                groupIdx = filterConditions.length - 1;

            bufForTable.append(" ").append("(").append(filterConditions[groupIdx].trim()).append(")");
            isFirstTable = false;

            filterLength += filterConditions[groupIdx].trim().length();

            if (filterConditions[groupIdx].trim().length() > 0)
                buffer.append(bufForTable);
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
            if (attribute.getPartitionKey() != 0)
                partitionKeys.add(attribute);
        }

        Collections.sort(partitionKeys, new Comparator<ETLEntityAttribute>() {

            @Override
            public int compare(ETLEntityAttribute o1, ETLEntityAttribute o2) {
                if (o1.getPartitionKey() < o2.getPartitionKey())
                    return -1;
                else if (o1.getPartitionKey() > o2.getPartitionKey())
                    return 1;
                else
                    return 0;
            }
        });

        return partitionKeys;
    }

    private List<ETLEntityAttribute> getNonPartitionKeys() {
        List<ETLEntityAttribute> nonPartitionKeys = new ArrayList<ETLEntityAttribute>();

        for (ETLEntityAttribute attribute : etlTask.getEtlEntity().getEtlEntityAttributes()) {
            if (attribute.getPartitionKey() == 0)
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

        if (defaultGroup == null) {
            logger.error("Default load group was not found!");
            exit(-1);
        }

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
                sourceTables.addAll(etlTask.getEtlEntity().getCommonSourceTables());
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
