package io.jacob.etlman;

import io.jacob.etlman.job.sql.JobSQLGenerator;
import io.jacob.etlman.job.sql.hive.HiveJobSQLGenerator;
import io.jacob.etlman.metastore.ETLEntity;
import io.jacob.etlman.metastore.ETLTask;

import java.io.FileWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created by xiaoy on 1/3/2017.
 */
public class ETLMan {

    private static Connection metaDBConn = null;
    private List<ETLTask> etlTaskList = new ArrayList<ETLTask>();

    public static Connection getMetaDBConn() {
        return metaDBConn;
    }

    private static void connectMetaDB(Properties metaDBParameters) throws Exception {

        String jdbcURL = String.format("jdbc:mysql://%s/%s?user=%s&password=%s&useUnicode=true&characterEncoding=UTF8",
                metaDBParameters.getProperty("dbHost"), metaDBParameters.getProperty("dbName"),
                metaDBParameters.getProperty("userName"), metaDBParameters.getProperty("password"));

        Class.forName("com.mysql.jdbc.Driver");
        metaDBConn = DriverManager.getConnection(jdbcURL);
    }

    private void initETLTasks() throws Exception {
        Statement statement = metaDBConn.createStatement();

        ResultSet rs = statement.executeQuery("select * from etl_tasks where sys_name = 'DW'");
        while (rs.next()) {
            ETLTask etlTask = new ETLTask();
            etlTask.setTaskName(rs.getString("task_name"));
            etlTask.setComments(rs.getString("comments"));
            etlTask.setDeveloperName(rs.getString("etl_dvlpr_name"));
            etlTask.setSchemaName(rs.getString("schema_name"));

            String tableName = rs.getString("table_name");

            ETLEntity etlEntity = new ETLEntity(etlTask.getSchemaName(), tableName);

            etlTask.setEtlEntity(etlEntity);

            etlTaskList.add(etlTask);
        }
        rs.close();
    }

    public static void main(String[] args) {

        String entityName = args[0];
        String outputFile = args[1];

        Properties metaDBConnParameters = new Properties();

        metaDBConnParameters.setProperty("dbHost", System.getenv("ETL_METADB_SERVER"));
        metaDBConnParameters.setProperty("dbName", System.getenv("ETL_METADB_DBNAME"));
        metaDBConnParameters.setProperty("userName", System.getenv("ETL_METADB_USER"));
        metaDBConnParameters.setProperty("password", System.getenv("ETL_METADB_PASSWORD"));

        try {
            connectMetaDB(metaDBConnParameters);
            ETLMan etlMan = new ETLMan();
            etlMan.initETLTasks();

            for (ETLTask task : etlMan.etlTaskList) {
                if (task.getEtlEntity().getEntityName().equals(entityName)) {
                    JobSQLGenerator sqlGenerator = new HiveJobSQLGenerator(task);

                    FileWriter fileWriter = new FileWriter(outputFile);

                    fileWriter.write(sqlGenerator.genJobScript());

                    fileWriter.close();

//                    System.out.println(sqlGenerator.genJobScript());
                }
            }

        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
