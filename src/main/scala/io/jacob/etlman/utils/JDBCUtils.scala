package io.jacob.etlman.utils

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import java.util.Properties

/**
  * Created by lingxiao on 2016/5/10.
  */
object JDBCUtils {

  var connParameters: Properties = _

  def getConn(connParameters: Properties): Connection = {
    this.connParameters = connParameters

    try {
      Class.forName("com.mysql.jdbc.Driver")

      DriverManager.getConnection("jdbc:mysql://" + connParameters.getProperty("dbHost") + "/" + connParameters.getProperty("dbName") +
        "?useUnicode=true&characterEncoding=UTF-8",
        connParameters.getProperty("userName"), connParameters.getProperty("password"))

    } catch {
      case e: Exception =>
        e.printStackTrace()
        throw e
    }
  }

  def closeConn(rs: ResultSet, pstmt: PreparedStatement, conn: Connection): Unit = {
    if (rs != null) {
      try {
        rs.close();
      } catch {
        case e: Exception => e.printStackTrace()
      }
    }
    if (pstmt != null) {
      try {
        pstmt.close();
      } catch {
        case e: Exception => e.printStackTrace()
      }
    }
    if (conn != null) {
      try {
        conn.close();
      } catch {
        case e: Exception => e.printStackTrace()
      }
    }
  }

  //用于发送增删改语句的方法
  def execOther(ps: PreparedStatement): Int = {
    try {
      //1、使用Statement对象发送SQL语句
      ps.executeUpdate()
    } catch {
      case e: Exception =>
        e.printStackTrace()
        throw e
    }
  }


  //method4: 专门用于发送查询语句
  def execQuery(ps: PreparedStatement): ResultSet = {
    try {
      ps.executeQuery()
    } catch {
      case e: Exception =>
        e.printStackTrace()
        throw e
    }
  }
}
