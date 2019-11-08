package io.jacob.etlman;

import io.jacob.etlman.job.sql.JobSQLGenerator;
import io.jacob.etlman.job.sql.hive.HiveJobSQLGenerator;
import io.jacob.etlman.metastore.ETLEntity;
import io.jacob.etlman.metastore.ETLTask;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import javax.sql.DataSource;
import java.io.File;
import java.io.FileWriter;
import java.sql.*;
import java.util.*;
import java.util.stream.Stream;

/**
 * Created by xiaoy on 1/3/2017.
 */
@SpringBootApplication
public class ETLMan {

    private static Connection metaDBConn = null;
    private List<ETLTask> etlTaskList = new ArrayList<ETLTask>();
    private static DataSource dataSource = null;

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

    public void initETLTasks() throws Exception {
        Statement statement = metaDBConn.createStatement();

        ResultSet rs = statement.executeQuery("select * from etl_tasks where sys_name = '数据平台'");
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

    public static void setDataSource(DataSource aDataSource) throws SQLException {
        dataSource = aDataSource;

        if (metaDBConn != null)
            metaDBConn.close();

        metaDBConn = dataSource.getConnection();
    }

    public String getSQLScriptForEntity(String entityName) throws Exception {

        String[] res = new String[]{"\n-- Job Script for " + entityName + "\n"};

        for (ETLTask task : etlTaskList) {
            if (task.getEtlEntity().getEntityName().equals(entityName)) {
                JobSQLGenerator sqlGenerator = new HiveJobSQLGenerator(task);

                Map<String, String> scripts = sqlGenerator.genJobScript();

                scripts.values()
                        .stream()
                        .reduce((x, y) -> x + "\n -- 批次分割线 -- " + y)
                        .ifPresent(s -> res[0] += s);
            }
        }

        return res[0];
    }

    public static void main(String[] args) {

        String entityName = args[0];
        String outputDir = args[1];

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

                    Map<String, String> scripts = sqlGenerator.genJobScript();

                    for (String scriptFile : scripts.keySet()) {
                        FileWriter fileWriter = new FileWriter(outputDir + File.separator + scriptFile);

                        fileWriter.write(scripts.get(scriptFile));

                        fileWriter.close();
                    }

//                    System.out.println(sqlGenerator.genJobScript());
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
