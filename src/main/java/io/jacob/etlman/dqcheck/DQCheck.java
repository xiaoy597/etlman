package io.jacob.etlman.dqcheck;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by xiaoy on 2017/4/19.
 */
public class DQCheck {
    private String dqCheckId;
    private String taskName;
    private String sqlTemplate;
    private String startTime;
    private String errLevelCd;
    private String setting1;
    private String setting2;
    private String setting3;
    private String setting4;
    private String setting5;
    private String setting6;
    private String setting7;
    private String setting8;

    private String workDate;

    public String getErrLevelCd() {
        return errLevelCd;
    }

    public void setErrLevelCd(String errLevelCd) {
        this.errLevelCd = errLevelCd;
    }

    public String getDqCheckId() {
        return dqCheckId;
    }

    public void setDqCheckId(String dqCheckId) {
        this.dqCheckId = dqCheckId;
    }

    public String getTaskName() {
        return taskName;
    }

    public void setTaskName(String taskName) {
        this.taskName = taskName;
    }

    public String getSqlTemplate() {
        return sqlTemplate;
    }

    public void setSqlTemplate(String sqlTemplate) {
        this.sqlTemplate = sqlTemplate;
    }

    public String getStartTime() {
        return startTime;
    }

    public void setStartTime(String startTime) {
        this.startTime = startTime;
    }

    public String getSetting1() {
        return setting1;
    }

    public void setSetting1(String setting1) {
        if (setting1 == null)
            this.setting1 = "";
        else
            this.setting1 = setting1.trim();
    }

    public String getSetting2() {
        return setting2;
    }

    public void setSetting2(String setting2) {
        if (setting2 == null)
            this.setting2 = "";
        else
            this.setting2 = setting2.trim();
    }

    public String getSetting3() {
        return setting3;
    }

    public void setSetting3(String setting3) {
        if (setting3 == null)
            this.setting3 = "";
        else
            this.setting3 = setting3.trim();
    }

    public String getSetting4() {
        return setting4;
    }

    public void setSetting4(String setting4) {
        if (setting4 == null)
            this.setting4 = "";
        else
            this.setting4 = setting4.trim();
    }

    public String getSetting5() {
        return setting5;
    }

    public void setSetting5(String setting5) {
        if (setting5 == null)
            this.setting5 = "";
        else
            this.setting5 = setting5.trim();
    }

    public String getSetting6() {
        return setting6;
    }

    public void setSetting6(String setting6) {
        if (setting6 == null)
            this.setting6 = "";
        else
            this.setting6 = setting6.trim();
    }

    public String getSetting7() {
        return setting7;
    }

    public void setSetting7(String setting7) {
        if (setting7 == null)
            this.setting7 = "";
        else
            this.setting7 = setting7.trim();
    }

    public String getSetting8() {
        return setting8;
    }

    public void setSetting8(String setting8) {
        if (setting8 == null)
            this.setting8 = "";
        else
            this.setting8 = setting8.trim();
    }

    public void runCheck(String dqcHiveDBName, String workDate, String workDir) throws Exception {

        this.workDate = workDate;

        String sqlTemplateFileName = workDir + File.separator + taskName.replaceAll("[^a-zA-Z0-9\\.]*", "");
        String logFileName = sqlTemplateFileName + ".log";

        File sqlTemplateFile = new File(sqlTemplateFileName);
        if (sqlTemplateFile.exists())
            sqlTemplateFile.delete();

        sqlTemplateFile.createNewFile();

        PrintStream printStream = new PrintStream(new FileOutputStream(sqlTemplateFile));
        printStream.print(sqlTemplate);
        printStream.close();

        String cmd = "hive " +
                " --hivevar DQC_DB=" + dqcHiveDBName +
                " --hivevar Q_DQ_ID=" + dqCheckId +
                " --hivevar DATA_DT=" + workDate +
                " --hivevar Q_TaskStartTime=" + "'" + startTime + "'" +
                " --hivevar Q_Error_Level_Cd=" + errLevelCd +
                " --hivevar Q_DQTask_Run_Type_Cd=2" +
                " --hivevar Q_Setting1=" + processSettings(setting1) +
                " --hivevar Q_Setting2=" + processSettings(setting2) +
                " --hivevar Q_Setting3=" + processSettings(setting3) +
                " --hivevar Q_Setting4=" + processSettings(setting4) +
                " --hivevar Q_Setting5=" + processSettings(setting5) +
                " --hivevar Q_Setting6=" + processSettings(setting6) +
                " --hivevar Q_Setting7=" + processSettings(setting7) +
                " --hivevar Q_Setting8=" + processSettings(setting8) +
                " -f " + sqlTemplateFileName
                + " 1>" + logFileName + " 2>&1";

        System.out.println("Executing " + cmd + " ...");

        if (DQCheckSQLGeneratorConfig.isDebug)
            cmd = "hive -f /home/etl/etlman/etlman-1.0-SNAPSHOT/test.sql 1>/home/etl/etlman/etlman-1.0-SNAPSHOT/test.sql.log 2>&1";

        String shellFileName = workDir + File.separator + "hive-cmd.sh";
        File shellFile = new File(shellFileName);

        if (shellFile.exists())
            shellFile.delete();

        shellFile.createNewFile();
        printStream = new PrintStream(new FileOutputStream(shellFile));
        printStream.println(cmd);
        printStream.close();

        List<String> cmds = new ArrayList<String>();
        cmds.add("sh");
        cmds.add(shellFileName);
        ProcessBuilder pb =new ProcessBuilder(cmds);
        Process p = pb.start();

        System.out.println("Command [" + cmd + "] is launched ...");
        int returnCode = p.waitFor();
        System.out.println("Return value= " + returnCode);

        if (returnCode != 0){
            System.out.println("Failed!");
            transferCheckLogForLoad(logFileName);
            loadCheckLog(logFileName + ".load", dqcHiveDBName, workDir);
            return;
        }
    }

    private void transferCheckLogForLoad(String logFileName) throws Exception {
        File logFile = new File(logFileName);
        File logFileForLoad = new File(logFileName + ".load");

        if (logFileForLoad.exists())
            logFileForLoad.delete();

        logFileForLoad.createNewFile();
        PrintStream printStream = new PrintStream(new FileOutputStream(logFileForLoad));

        BufferedReader reader = new BufferedReader(new FileReader(logFile));

        String line;
        int lineNo = 0;
        while((line = reader.readLine()) != null){
            lineNo++;
            String logLine = dqCheckId
                    + "\t" + workDate
                    + "\t" + startTime
                    + "\t" + String.valueOf(lineNo)
                    + "\t" + line.replaceAll("\\t", " ");
            printStream.println(logLine);
        }

        reader.close();

        printStream.close();
    }

    private void loadCheckLog(String logFileName, String dqcHiveDBName, String workDir) throws Exception {
        String cmd = "hive -e \"load data local inpath '" + logFileName + "' into table " + dqcHiveDBName + ".m07_checkresult_log\"";

        System.out.println("Executing " + cmd);

        String shellFileName = workDir + File.separator + "hive-load-cmd.sh";
        File shellFile = new File(shellFileName);

        if (shellFile.exists())
            shellFile.delete();

        shellFile.createNewFile();
        PrintStream printStream = new PrintStream(new FileOutputStream(shellFile));
        printStream.println(cmd);
        printStream.close();

        List<String> cmds = new ArrayList<String>();
        cmds.add("sh");
        cmds.add(shellFileName);
        ProcessBuilder pb =new ProcessBuilder(cmds);
        Process p = pb.start();

        System.out.println("Command [" + cmd + "] is launched ...");
        int returnCode = p.waitFor();

        System.out.println("Return value= " + returnCode);

        if (returnCode != 0)
            System.out.println("Failed!");
        else
            System.out.println("Succeed.");
    }

    private String processSettings(String setting){
        String result = setting.replaceAll("--[^\\n]*\\n", " ").replaceAll("\\n", " ").replaceAll("\\$\\{DATA_DT\\}", workDate);
        result = result.replaceAll("\"", "\\\"").replaceAll("\\*", "\\*");
        return "\"" + result + "\"";
    }
}
