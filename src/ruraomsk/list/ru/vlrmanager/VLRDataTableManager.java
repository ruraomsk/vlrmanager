/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ruraomsk.list.ru.vlrmanager;

import com.tibbo.aggregate.common.Log;
import com.tibbo.aggregate.common.context.ContextException;
import com.tibbo.aggregate.common.datatable.*;
import static com.tibbo.aggregate.common.datatable.FieldFormat.*;
import com.tibbo.aggregate.common.datatable.validator.ValidatorHelper;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import javax.xml.parsers.ParserConfigurationException;
import org.w3c.dom.DOMException;
import org.xml.sax.SAXException;

/**
 *
 * @author Yury Rusinov <ruraomsl@list.ru at Automatics E>
 */
public class VLRDataTableManager {

    /**
     * Возвращает пустую таблицу описания переменных и констант
     *
     * @return
     */
    static public final DataTable emptyTable() {
        TableFormat VFT_Table = new TableFormat();

        FieldFormat ff = FieldFormat.create("<id><I><D=Номер переменной>");
        VFT_Table.addField(ff);

        ff = FieldFormat.create("<name><S><D=Имя переменной>");
        ff.getValidators().add(ValidatorHelper.NAME_LENGTH_VALIDATOR);
        ff.getValidators().add(ValidatorHelper.NAME_SYNTAX_VALIDATOR);
        VFT_Table.addField(ff);

        ff = FieldFormat.create("<description><S><D=Описание>");
        VFT_Table.addField(ff);

        ff = FieldFormat.create("<type><I><D=Тип переменной>");
        ff.setSelectionValues(typeSelectionValues());

        VFT_Table.addField(ff);

        VFT_Table.addField(create("<value><S><A=0><D=Значение>"));
        VFT_Table.addField(create("<send><B><A=false><D=Флаг Send>"));
        VFT_Table.addField(create("<arch><B><A=false><D=Флаг Arch>"));
        VFT_Table.addField(create("<eprom><B><A=false><D=Флаг Eprom>"));
        VFT_Table.addField(create("<readonly><B><A=false><D=Только чтение>"));

        return new DataTable(VFT_Table);

    }

    private static Map typeSelectionValues() {
        Map types = new LinkedHashMap();
        types.put(0, "Boolean");
        types.put(1, "Integer");
        types.put(2, "Float");
        types.put(3, "Long");
        types.put(4, "One byte");
        return types;
    }

    /**
     * Загружает переменные
     *
     * @param buffer
     * @return
     */
    public static DataTable loadVariables(byte[] buffer) {
        DataTable result = emptyTable();
        String str = null;
        try {
            str = new String(buffer, "Cp1251");
        } catch (UnsupportedEncodingException ex) {
            Log.CORE.error("Ошибка loadVariables " + ex.getMessage());
            return null;
        }
        StringTokenizer st = new StringTokenizer(str, "\t\n\r");
        if(str.substring(0, 1).equals("№")){
            for (int i = 0; i < 5; i++) {
                st.nextToken();
            }
        }
        while (st.hasMoreElements()) {
            Integer id = Integer.parseInt(st.nextToken());
            String name = st.nextToken();
            Integer type = Integer.parseInt(st.nextToken());
            Integer mask = Integer.parseInt(st.nextToken());
            String description = st.nextToken();
            DataRecord rec = result.addRecord();
            rec.setValue("id", id);
            rec.setValue("name", name);
            rec.setValue("description", description);
            switch (type) {
                case 1:
                    type = 2;
                    break;
                case 2:
                    type = 1;
                    break;
                case 3:
                    type = 0;
                    break;
                case 6:
                    type = 4;
                    break;
                case 7:
                    type = 3;
                    break;
                default:
                    type = 99;
            }
            rec.setValue("type", type);
            rec.setValue("send", (mask & 0x80) != 0);
            rec.setValue("arch", (mask & 0x40) != 0);
            rec.setValue("eprom", (mask & 0x20) != 0);

        }
        return result;
    }

    /**
     * Загружает в таблицу константы ПЗУ
     *
     * @param buffer
     * @return
     */
    public static DataTable loadConstants(byte[] buffer) {
        DataTable result = emptyTable();
        String str = null;
        Integer id = 0;
        try {
            str = new String(buffer, "Cp1251");
            
        } catch (UnsupportedEncodingException ex) {
            Log.CORE.error("Ошибка loadConstants " + ex.getMessage());
            return null;
        }
        StringTokenizer st = new StringTokenizer(str, "\t\n\r");
        for (int i = 0; i < 7; i++) {
            st.nextToken();
        }
        while (st.hasMoreElements()) {
            st.nextToken();
            String s = st.nextToken();

            String name = s.substring(0, s.indexOf(" "));
            String eeprom = s.substring(s.indexOf(" ") + 2);

            String description = st.nextToken();
            String stype = st.nextToken();
            String svalue = st.nextToken();
            Integer type;
            Object value;
            String modulename = st.nextToken();
            s = st.nextToken();
            s = s.substring(0, s.indexOf(" "));
            Integer block = Integer.parseInt(s);
            if ("(ПЗУ)".equals(eeprom)) {

                DataRecord rec = result.addRecord();
                rec.setValue("id", id++);
                rec.setValue("name", name + block.toString());
                rec.setValue("description", description);
                switch (stype) {
                    case "Вещ.":
                        type = 2;
                        value = Float.parseFloat(svalue);
                        break;
                    case "Цел.":
                        type = 1;
                        value = Integer.parseInt(svalue);
                        break;
                    case "Лог.":
                        type = 0;
                        value = Integer.parseInt(svalue) == 1;
                        break;
                    case "Байт.":
                        type=4;
                        value=(byte)Integer.parseInt(svalue)&0xff;
                        break;
                    default:
                        type = 99;
                        value = 0;
                }
                rec.setValue("type", type);
                rec.setValue("value", value.toString());
                rec.setValue("readonly", true);
            }
        }
        return result;
    }

    /**
     * Конвертирует в XML DataTable
     *
     * @param table
     * @return
     */
    static public String toXML(DataTable table) {
        try {
            return EncodingUtils.encodeToXML(table);
        } catch (ParserConfigurationException | IOException | ContextException | DOMException ex) {
            Log.CORE.error("toXML " + ex.getMessage());
            return null;
        }
    }

    /**
     * Создает таблицу из XML
     *
     * @param xml
     * @return
     */
    static public DataTable fromXML(String xml) {
        try {
            if (xml == null) {
                return new DataTable(TableFormat.EMPTY_FORMAT);
            }
            return EncodingUtils.decodeFromXML(xml);
        } catch (ParserConfigurationException | IOException | ContextException | DOMException | IllegalArgumentException | SAXException ex) {
            Log.CORE.error("fromXML " + ex.getMessage());
            return null;
        }
    }

    /**
     * Загружает в строку zip файл dataXXX
     *
     * @param zipFile
     * @param flag True переменные false константы ПЗУ
     * @return null если были ошибки
     */
    static public byte[] loadZipFile(String zipFile, boolean flag) {
        try (ZipInputStream zipfile = new ZipInputStream(new FileInputStream(zipFile))) {
            ZipEntry zipentry;
            while ((zipentry = zipfile.getNextEntry()) != null) {
                byte[] buffer = new byte[(int) (zipentry.getSize() & 0xffff)];
                int pos = 0;
                while (pos < buffer.length) {
                    int len = buffer.length - pos;
                    len = Math.min(1000, len);
                    len = zipfile.read(buffer, pos, len);
                    pos += len;
                }
                if (zipentry.getName().equalsIgnoreCase("params.inf") && flag) {
                    return buffer;
                }
                if (zipentry.getName().equalsIgnoreCase("const.txt") && !flag) {
                    return buffer;
                }
            }
            return null;
        } catch (FileNotFoundException ex) {
            Log.CORE.error("Файл не найден " + zipFile);
            return null;
        } catch (IOException ex) {
            Log.CORE.error("Ошибка файла " + zipFile + " " + ex.getMessage());
            return null;
        }
    }
}
