/*
 *  Класс для работы с описанием данных XML устройств ВЛР
 *  Все описания собственно это таблицы данных
 */
package ruraomsk.list.ru.vlrmanager;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import ruraomsk.list.ru.strongsql.ParamSQL;

/**
 *
 * @author Yury Rusinov <ruraomsl@list.ru at Automatics E>
 */
public class VLRXMLManager {

    private ParamSQL param;
    private Connection con;
    private Statement stmt;
    public boolean connected = false;

    /**
     * Конструктор с открытием базы данных
     *
     * @param param - параметры с доступом к БД проектов
     */
    public VLRXMLManager(ParamSQL param) {
        this.param = param;
        connected = open();

    }

    /**
     * Конструктор с созданием БД и затем ее открытие
     *
     * @param param - параметры с доступом к БД проектов
     * @param newDB - true если создать
     */
    public VLRXMLManager(ParamSQL param, boolean newDB) {
        this.param = param;
        if (newDB) {
            if (create()) {
                connected = open();
            }
        } else {
            connected = open();
        }
    }

    private boolean create() {
        try {
            Class.forName(param.JDBCDriver);
            con = DriverManager.getConnection(param.url, param.user, param.password);
            stmt = con.createStatement();
            stmt.executeUpdate("drop table if exists " + param.myDB + ";");
            stmt.executeUpdate("create table " + param.myDB + " (idvlr bigint,idfile bigint,xml text);");
        } catch (ClassNotFoundException | SQLException ex) {
            System.err.println("SQL create " + ex.getMessage());
            return false;
        }
        return true;

    }

    private boolean open() {
        try {
            Class.forName(param.JDBCDriver);
            con = DriverManager.getConnection(param.url, param.user, param.password);
            stmt = con.createStatement();
        } catch (ClassNotFoundException | SQLException ex) {
            System.err.println("SQL open " + ex.getMessage());
            return false;
        }
        return true;
    }

    public void close() {
        try {
            if(con==null) return;
            con.close();
        } catch (SQLException ex) {
        }
    }

    /**
     *
     * @param idvlr - идентификатор схемы ВЛР
     * @param idfile - номер данных 1 - БПО переменные 2- СПО переменные 3- Константы СПО
     * @return
     */
    public String getXML(Integer idvlr, Integer idfile) {
        String xml = null;
        try {
            String rez = "SELECT xml FROM " + param.myDB + " WHERE  idvlr='" + idvlr.toString() + "' and idfile='" + idfile.toString()+"';";
            ResultSet rs = stmt.executeQuery(rez);
            while (rs.next()) {
                return rs.getString("xml");
            }
            return null;
        } catch (SQLException ex) {
            System.err.println("getXML "+ex.getMessage());
            return null;
        }
    }

    public boolean putXML(Integer idvlr, Integer idfile, String xml) {
        try {
            String rez = "SELECT xml FROM " + param.myDB + " WHERE  idvlr='" + idvlr.toString() + "' and idfile='" + idfile.toString()+"';";
            ResultSet rs = stmt.executeQuery(rez);
            PreparedStatement preparedStatement = con.prepareStatement("begin;");

            while (rs.next()) {
                preparedStatement = con.prepareStatement("UPDATE " + param.myDB + " SET xml=? WHERE idvlr='" + idvlr.toString()
                        + "' and idfile='" + idfile.toString() + "';");
                preparedStatement.setString(1, xml);
                preparedStatement.executeUpdate();
                preparedStatement = con.prepareStatement("commit;");
                return true;
            }
            preparedStatement = con.prepareStatement("INSERT INTO " + param.myDB + " (idvlr,idfile,xml) VALUES (" + idvlr.toString()
                    + "," + idfile.toString() + ",?);");
            preparedStatement.setString(1, xml);
            preparedStatement.executeUpdate();
            preparedStatement = con.prepareStatement("commit;");
            return true;

        } catch (SQLException ex) {
            try {
                System.err.println("putXML " + ex.getMessage());
                con.prepareStatement("rollback;");
            } catch (SQLException ex1) {
            }
            return false;
        }

    }
}
