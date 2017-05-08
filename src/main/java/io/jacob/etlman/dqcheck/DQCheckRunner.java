package io.jacob.etlman.dqcheck;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by xiaoy on 2017/4/19.
 */
public class DQCheckRunner {
    private static Connection dqcDBConn = null;
    private static String dqcDBName = "dg_dqc";
    private static String dqcHiveDBName = "dg_dqc";
    private static String workDir = "c:\\tmp\\dqcheck";
    private static String workDate = "2017-04-16";

    private static void connDQCDB() throws Exception {
        if (dqcDBConn != null)
            return;

        String dqcDBServer = System.getenv("DQC_DB_SERVER");
        String dqcDBPort = System.getenv("DQC_DB_PORT");
        String dqcDBUser = System.getenv("DQC_DB_USER");
        String dqcDBPassword = System.getenv("DQC_DB_PASSWORD");

        Class.forName("com.mysql.jdbc.Driver");
        dqcDBConn = DriverManager.getConnection("jdbc:mysql://" + dqcDBServer + ":" + dqcDBPort + "/" + dqcDBName, dqcDBUser, dqcDBPassword);
    }

    private static List<DQCheck> getDQCheckRules() throws Exception {
        Statement statement = dqcDBConn.createStatement();

        ResultSet rs = statement.executeQuery(
                "select c.dqid, c.task_name, c.task_desc, c.dqtype_id" +
                        ",c.setting1, c.setting2, c.setting3, c.setting4, c.setting5, c.setting6, c.setting7, c.setting8" +
                        ",c.stage_cd, c.error_level_cd, c.interval_cd, c.dqtask_status_cd, c.target_table, c.owner" +
                        ",r.sqltemplate, coalesce(c.saveerrors, 100000) as saveerrors, r.dqtype_name" +
//                        ",t.tx_date" +
                        " " +
                        "from " +
                        dqcDBName + ".m07_checklist c" +
                        ", " + dqcDBName + ".m07_dqtype r" +
//                        ", " + dqcDBName + ".m07_dirrun_task t" +
                        " " +
                        "where c.dqtype_id = r.dqtype_id " +
//                        "and c.dqid = t.dqid " +
//                        "and t.dirtask_status_cd = 11 " +
                        "and r.end_dt = '2999-12-31 00:00:00' " +
                        "and c.end_dt = '2999-12-31 00:00:00' " +
                        "and c.dqtask_status_cd = 1 " +
                        "order by c.dqtype_id, c.task_name" +
                        ""
        );

        List<DQCheck> dqCheckList = new ArrayList<DQCheck>();

        SimpleDateFormat timeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        while(rs.next()){
//            System.out.println("dqid=" + rs.getString("dqid")
//                    + ", task_name=" + rs.getString("task_name")
//                    + ", dqtype_name=" + rs.getString("dqtype_name"));
//            System.out.println(">>>>>>>>>>> SQL Template >>>>>>>>>>>>>>>>>>>>");
//            System.out.println(rs.getString("sqltemplate"));
//            System.out.println("<<<<<<<<<<< SQL Template <<<<<<<<<<<<<<<<<<<<\n\n");

            DQCheck dqCheck = new DQCheck();

            dqCheck.setDqCheckId(rs.getString("dqid"));
            dqCheck.setTaskName(rs.getString("task_name"));
            dqCheck.setStartTime(timeFormat.format(new Date()));
            dqCheck.setErrLevelCd(rs.getString("error_level_cd"));
            dqCheck.setSqlTemplate(rs.getString("sqltemplate").trim());
            dqCheck.setSetting1(rs.getString("setting1"));
            dqCheck.setSetting2(rs.getString("setting2"));
            dqCheck.setSetting3(rs.getString("setting3"));
            dqCheck.setSetting4(rs.getString("setting4"));
            dqCheck.setSetting5(rs.getString("setting5"));
            dqCheck.setSetting6(rs.getString("setting6"));
            dqCheck.setSetting7(rs.getString("setting7"));
            dqCheck.setSetting8(rs.getString("setting8"));

            dqCheckList.add(dqCheck);

        }

        rs.close();

        return dqCheckList;
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 3){
            System.out.println("Usage: DQCheckRunner <dqc_db[/dqc_hivedb]> <work_date> <work_dir> <debug>");
        }else {
            String[] dbNames = args[0].split("/");
            dqcDBName = dbNames[0];
            if (dbNames.length > 1)
                dqcHiveDBName = dbNames[1];

            workDate = args[1];
            workDir = args[2];
        }

        if (args.length > 3)
            DQCheckSQLGeneratorConfig.isDebug = Boolean.valueOf(args[3]);

        connDQCDB();
        List<DQCheck> dqCheckList = getDQCheckRules();

        for (DQCheck dqCheck : dqCheckList){
            dqCheck.runCheck(dqcHiveDBName, workDate, workDir);
        }

        dqcDBConn.close();
    }
}
