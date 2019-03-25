package ch.epfl.dias.store.column;

import ch.epfl.dias.store.DataType;

public class DBColumn {

	public Object[] fields;
	public DataType[] types;
	public boolean eof;

	public DBColumn(Object[] fields, DataType[] types) {
		this.fields = fields;
		this.types = types;
		this.eof = false;
	}

	public DBColumn() {this.eof = true;}


	public Integer[] getAsInteger() {
		Integer intarr[] = new Integer[fields.length];
		for (int i = 0; i < fields.length; i++){
			Object obj =  fields[i];
			if (obj instanceof String) {
				intarr[i] = Integer.valueOf((String) obj);
			}else{
				intarr[i] = (int) (double)obj;
			}
		}
		return intarr;
	}

	public Double[] getAsDouble() {
		Double intarr[] = new Double[fields.length];
		for (int i = 0; i < fields.length; i++){
			Object obj =  fields[i];
			if (obj instanceof String) {
				intarr[i] = Double.valueOf((String) obj);
			}else{
				intarr[i] = (double)obj;
			}
		}
		return intarr;
	}
}
