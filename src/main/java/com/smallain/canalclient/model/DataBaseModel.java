package com.smallain.canalclient.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DataBaseModel {
    public String database;
    public String table;
    public String type;
    public String ts;
    public Map data;

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String databaseinfo) {
        this.database = databaseinfo;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String tableinfo) {
        this.table = tableinfo;
    }


    public String getType() {
        return type;
    }

    public void setType(String typeinfo) {
        this.type = typeinfo;
    }

    public String getTs() {
        return ts;
    }

    public void setTs(String tsinfo) {
        this.ts = tsinfo;
    }

    public Map getData() {
        return data;
    }

    public void setData(Map datainfo) {
        this.data = datainfo;
    }

    public DataBaseModel() {
        super();
    }

}
