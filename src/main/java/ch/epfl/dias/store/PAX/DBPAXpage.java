package ch.epfl.dias.store.PAX;

import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;

public class DBPAXpage {

    public Object[] fields;
    public DataType[] types;
    public boolean eof;

    public DBPAXpage(Object[] fields, DataType[] types) {
        this.fields = fields;
        this.types = types;
        this.eof = false;
    }

    public DBPAXpage() {
        this.eof = true;
    }


}
