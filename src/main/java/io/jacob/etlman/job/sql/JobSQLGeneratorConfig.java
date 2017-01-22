package io.jacob.etlman.job.sql;

/**
 * Created by xiaoy on 1/20/2017.
 */
public class JobSQLGeneratorConfig {
    public final static String loadDateColName = "data_dt";
    public final static String workDateVarName = "${data_dt}";
    public final static String lastWorkDateVarName = "${last_data_dt}";
}
