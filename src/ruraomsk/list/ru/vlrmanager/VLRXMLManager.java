/*
 *  Класс для работы с описанием данных XML устройств ВЛР
 *  Все описания собственно это таблицы данных
 */
package ruraomsk.list.ru.vlrmanager;

import com.tibbo.aggregate.common.Log;
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
            Log.CORE.error("SQL create " + ex.getMessage());
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
            Log.CORE.error("SQL open " + ex.getMessage());
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
     * Считывает из БД XML с описанием переменной
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
            if (rs == null) {
                return null;
            }
            while (rs.next()) {
                return rs.getString("xml");
            }
            return null;
        } catch (SQLException ex) {
            Log.CORE.error("getXML " + ex.getMessage());
            return null;
        }
    }

    /**
     * Записывает в базу данных схему XML описания переменных
     *
     * @param idvlr
     * @param idfile
     * @param xml
     * @return
     */
    public boolean putXML(String idvlr, Integer idfile, String xml) {
        try {
            String rez = "SELECT xml FROM " + param.myDB + " WHERE  idvlr='" + idvlr + "' and idfile='" + idfile.toString() + "';";
            ResultSet rs = stmt.executeQuery(rez);
            PreparedStatement preparedStatement = con.prepareStatement("begin;");

            while (rs.next()) {
                preparedStatement = con.prepareStatement("UPDATE " + param.myDB + " SET xml=? WHERE idvlr='" + idvlr
                        + "' and idfile='" + idfile.toString() + "';");
                preparedStatement.setString(1, xml);
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
                Log.CORE.error("putXML " + ex.getMessage());
                con.prepareStatement("rollback;");
            } catch (SQLException ex1) {
            }
            return false;
        }
    }

    /**
     * Возвращает в виде таблицы всю главную таблицу описания перемнных и
     * констант
     *
     * @param resFrm формат результирующей таблицы
     * @return
     */
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
            Log.CORE.error("Ошибка загрузки из БД vlr " + ex.getMessage());
            return null;
        }
    }

    /**
     * возвращает пустую таблицу описания всех переменных
     *
     * @return
     */
    static public DataTable emptyTable() {
        TableFormat VFT_Table = new TableFormat();
        VFT_Table.addField(FieldFormat.create("<idvlr><S><D=Идентификатор ВЛР>"));
        VFT_Table.addField(FieldFormat.create("<idfile><I><D=Номер таблицы>"));
        VFT_Table.addField(FieldFormat.create("<variables><T><D=Таблица перемненных>"));
        return new DataTable(VFT_Table);
    }

    /**
     * Записывает в БД всю таблицу описания всех переменных
     *
     * @param table
     * @return
     */
    public boolean toDB(DataTable table) {
        try {
            stmt.executeUpdate("drop table if exists " + param.myDB + ";");
            stmt.executeUpdate("create table " + param.myDB + " (idvlr text,idfile bigint,xml text);");
            for (DataRecord rec : table) {
                putXML(rec.getString("idvlr"), rec.getInt("idfile"), VLRDataTableManager.toXML(rec.getDataTable("variables")));
            }
            return true;
        } catch (SQLException ex) {
            Log.CORE.error("Ошибка загрузки в БД vlr " + ex.getMessage());
            return false;
        }
    }
}
