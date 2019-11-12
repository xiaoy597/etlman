package io.jacob.etlman;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.File;
import java.io.FileWriter;

@SpringBootApplication
public class ETLManApp implements CommandLineRunner {

    @Autowired
    private ETLMan etlMan;

    public static void main(String[] args) {
        SpringApplication.run(ETLManApp.class, args);
    }

    @Override
    public void run(String... args) throws Exception {

        if (args.length == 0)
            return;

        String entityName = args[0];
        String outputDir = args[1];

        etlMan.initETLTasks();

        String script = etlMan.getSQLScriptForEntity(entityName);
        FileWriter fileWriter = new FileWriter(outputDir + File.separator + entityName + ".sql");

        fileWriter.write(script);

        fileWriter.close();
    }
}
