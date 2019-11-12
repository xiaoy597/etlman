package io.jacob.etlman;


import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
public class ETLManTest {

    @Autowired
    private ETLMan etlMan = null;

    @Before
    public void init() throws Exception {
        etlMan.initETLTasks();
    }

    @Test
    public void testJobSQLGenerator_1() throws Exception {
        String sql = etlMan.getSQLScriptForEntity("基础层主表1");

        System.out.println(sql);

    }

    @Test
    public void testJobSQLGenerator_2() throws Exception {
        String sql = etlMan.getSQLScriptForEntity("基础层主表2");

        System.out.println(sql);

    }

    @Test
    public void testJobSQLGenerator_3() throws Exception {
        String sql = etlMan.getSQLScriptForEntity("基础层主表3");

        System.out.println(sql);

    }

    @Test
    public void testJobSQLGenerator_4() throws Exception {
        String sql = etlMan.getSQLScriptForEntity("法人");

        System.out.println(sql);

    }

    @After
    public void cleanup() {
    }
}
