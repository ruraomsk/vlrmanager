/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ruraomsk.list.ru.vlrmanager;

import com.tibbo.aggregate.common.context.ContextException;
import com.tibbo.aggregate.common.datatable.DataTable;
import com.tibbo.aggregate.common.datatable.EncodingUtils;
import java.io.IOException;
import javax.xml.parsers.ParserConfigurationException;
import ruraomsk.list.ru.strongsql.ParamSQL;

/**
 *
 * @author Yury Rusinov <ruraomsl@list.ru at Automatics E>
 */
public class testXML {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws ParserConfigurationException, IOException, ContextException {
        ParamSQL param=new ParamSQL();
        param.myDB="vlrbase";
        param.JDBCDriver="org.postgresql.Driver";
        param.url="jdbc:postgresql://192.168.1.70:5432/testbase";
        param.user="postgres";
        param.password="162747";

        VLRXMLManager vlrbase=new VLRXMLManager(param, true);
        DataTable table=VLRDataTableManager.emptyTable();
        String xml=EncodingUtils.encodeToXML(table);
        vlrbase.putXML(1, 1, xml);
        vlrbase.putXML(1, 2, xml);
        vlrbase.putXML(1, 3, xml);
        vlrbase.putXML(1, 1, xml);
        
    }
    
}
