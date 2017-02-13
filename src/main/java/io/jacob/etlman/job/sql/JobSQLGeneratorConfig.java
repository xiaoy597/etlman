package io.jacob.etlman.job.sql;

/**
 * Created by xiaoy on 1/20/2017.
 */
public class JobSQLGeneratorConfig {
    public final static String loadDateColName = "data_dt";
    public final static String workDateVarName = "${DATA_DT}";
    public final static String lastWorkDateVarName = "${LAST_DATA_DT}";
}
