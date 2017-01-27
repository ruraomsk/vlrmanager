/*
 *  Класс для работы с описанием данных XML устройств ВЛР
 *  Все описания собственно это таблицы данных
 */
package ruraomsk.list.ru.vlrmanager;

import com.tibbo.aggregate.common.datatable.DataRecord;
import com.tibbo.aggregate.common.datatable.DataTable;
import com.tibbo.aggregate.common.datatable.FieldFormat;
import com.tibbo.aggregate.common.datatable.TableFormat;
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
            stmt.executeUpdate("create table " + param.myDB + " (idvlr text,idfile bigint,xml text);");
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
            if (con == null) {
                return;
            }
            con.close();
        } catch (SQLException ex) {
        }
    }

    /**
     *
     * @param idvlr - идентификатор схемы ВЛР
     * @param idfile - номер данных 1 - CПО переменные 2- ППО переменные 3-
     * Константы ППО
     * @return
     */
    public String getXML(String idvlr, Integer idfile) {
        String xml = null;
        try {
            String rez = "SELECT xml FROM " + param.myDB + " WHERE  idvlr='" + idvlr.toString() + "' and idfile='" + idfile.toString() + "';";
            ResultSet rs = stmt.executeQuery(rez);
            while (rs.next()) {
                return rs.getString("xml");
            }
            return null;
        } catch (SQLException ex) {
            System.err.println("getXML " + ex.getMessage());
            return null;
        }
    }

    public boolean putXML(String idvlr, Integer idfile, String xml) {
        try {
            String rez = "SELECT xml FROM " + param.myDB + " WHERE  idvlr='" + idvlr+ "' and idfile='" + idfile.toString() + "';";
//                System.err.println(rez);
            ResultSet rs = stmt.executeQuery(rez);
            PreparedStatement preparedStatement = con.prepareStatement("begin;");

            while (rs.next()) {
                preparedStatement = con.prepareStatement("UPDATE " + param.myDB + " SET xml=? WHERE idvlr='" + idvlr
                        + "' and idfile='" + idfile.toString() + "';");
                preparedStatement.setString(1, xml);
//                System.err.println(preparedStatement.toString());
                preparedStatement.executeUpdate();
                preparedStatement = con.prepareStatement("commit;");
                return true;
            }
            preparedStatement = con.prepareStatement("INSERT INTO " + param.myDB + " (idvlr,idfile,xml) VALUES ('" + idvlr
                    + "'," + idfile.toString() + ",?);");
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

    public DataTable toTable(TableFormat resFrm) {
        try {
            DataTable result = new DataTable(resFrm);
            ResultSet rs = null;
            String rez = "SELECT * FROM " + param.myDB + ";";
            rs = stmt.executeQuery(rez);
            while (rs.next()) {
                DataRecord rec = result.addRecord();
                rec.setValue("idvlr", rs.getString("idvlr"));
                rec.setValue("idfile", rs.getInt("idfile"));
                DataTable table = VLRDataTableManager.fromXML(rs.getString("xml"));
                rec.setValue("variables", table);
            }
            return result;

        } catch (SQLException ex) {
            System.err.println("Ошибка загрузки из БД vlr " + ex.getMessage());
            return null;
        }
    }

    public boolean toDB(DataTable table) {
        try {
            stmt.executeUpdate("drop table if exists " + param.myDB + ";");
            stmt.executeUpdate("create table " + param.myDB + " (idvlr text,idfile bigint,xml text);");
            for (DataRecord rec : table) {
                putXML(rec.getString("idvlr"), rec.getInt("idfile"), VLRDataTableManager.toXML(rec.getDataTable("variables")));
            }
            return true;
        } catch (SQLException ex) {
            System.err.println("Ошибка загрузки в БД vlr " + ex.getMessage());
            return false;
        }
    }
}
