package io.jacob.etlman.job.sql;

import io.jacob.etlman.metastore.ETLLoadBatch;
import io.jacob.etlman.metastore.ETLLoadGroup;
import io.jacob.etlman.metastore.ETLSourceTable;
import io.jacob.etlman.metastore.ETLTask;

/**
 * Created by xiaoy on 1/5/2017.
 */
public abstract class JobSQLGenerator {

    protected boolean hasIncSourceData;
    protected ETLTask etlTask;

    public JobSQLGenerator(ETLTask etlTask) {
        this.etlTask = etlTask;
        hasIncSourceData = hasIncrementalSource();
    }

    protected String genJobPreprocess() throws Exception {
        return "";
    }

    protected String genJobPostprocess() throws Exception {
        return "";
    }

    protected String genBatchPreprocess(ETLLoadBatch loadBatch) throws Exception {
        return "\n-- Script for batch " + loadBatch.getLoadBatch() + " begin";
    }

    protected String genBatchPostprocess(ETLLoadBatch loadBatch) throws Exception {
        return "\n-- Script for batch " + loadBatch.getLoadBatch() + " end";
    }

    protected String genGroupPreprocess(ETLLoadGroup loadGroup) throws Exception {
        return "\n-- Script for group " + loadGroup.getLoadGroup() + " begin";
    }

    protected String genGroupPostprocess(ETLLoadGroup loadGroup) throws Exception {
        return "\n-- Script for group " + loadGroup.getLoadGroup() + " end";
    }

    protected String genGroupBody(ETLLoadGroup loadGroup) throws Exception {
        return "";
    }

    protected String genGroupScript(ETLLoadGroup loadGroup) throws Exception {
        return genGroupPreprocess(loadGroup) + genGroupBody(loadGroup) + genGroupPostprocess(loadGroup);
    }

    protected String genBatchBody(ETLLoadBatch loadBatch) throws Exception {
        StringBuilder buffer = new StringBuilder();

        for (ETLLoadGroup loadGroup : loadBatch.getLoadGroupList())
            buffer.append(genGroupScript(loadGroup));

        return buffer.toString();
    }

    protected String genBatchScript(ETLLoadBatch loadBatch) throws Exception {
        return genBatchPreprocess(loadBatch) + genBatchBody(loadBatch) + genBatchPostprocess(loadBatch);
    }

    protected String genJobBody() throws Exception {
        StringBuilder buffer = new StringBuilder();
        for (ETLLoadBatch loadBatch : etlTask.getEtlEntity().getEtlLoadBatches())
            buffer.append(genBatchScript(loadBatch));

        return buffer.toString();
    }

    public String genJobScript() throws Exception {
        return genJobPreprocess() + genJobBody() + genJobPostprocess();
    }

    private boolean hasIncrementalSource() {
        // If any one of the source tables contains incremental data, a working table is needed.
        for (ETLLoadBatch loadBatch : etlTask.getEtlEntity().getEtlLoadBatches()){
            for (ETLSourceTable sourceTable : loadBatch.getSourceTableList())
                if (sourceTable.isIncExtract()){
                    return true;
                }
        }
        return false;
    }
}
