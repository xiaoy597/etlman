package io.jacob.etlman;

import io.jacob.etlman.job.sql.JobSQLGenerator;
import io.jacob.etlman.job.sql.hive.HiveJobSQLGenerator;
import io.jacob.etlman.metastore.ETLEntity;
import io.jacob.etlman.metastore.ETLTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.io.File;
import java.io.FileWriter;
import java.sql.*;
import java.util.*;
import java.util.stream.Stream;

/**
 * Created by xiaoy on 1/3/2017.
 */
@Component
public class ETLMan {

    private Connection metaDBConn = null;

    private List<ETLTask> etlTaskList = new ArrayList<ETLTask>();

    private static Logger logger = LoggerFactory.getLogger(ETLMan.class);

    @Autowired
    public ETLMan(DataSource aDataSource) throws Exception {
        metaDBConn = aDataSource.getConnection();
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

            ETLEntity etlEntity = new ETLEntity(etlTask.getSchemaName(), tableName).initialize(metaDBConn);

            etlTask.setEtlEntity(etlEntity);

            etlTaskList.add(etlTask);
        }
        rs.close();
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

}
