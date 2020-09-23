package io.rtdi.bigdata.snowflakeloader.table.column;

import java.sql.Timestamp;
import java.time.Instant;
import java.sql.Date;
import java.util.List;

import org.apache.avro.Schema.Field;

import io.rtdi.bigdata.connector.connectorframework.exceptions.ConnectorCallerException;
import io.rtdi.bigdata.connector.pipeline.foundation.avro.JexlGenericData.JexlRecord;
import io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes.AvroByte;
import io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes.AvroDate;
import io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes.AvroTimestamp;
import io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes.AvroType;
import io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes.AvroVarchar;
import io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes.IAvroDatatype;
import io.rtdi.bigdata.connector.pipeline.foundation.avrodatatypes.LogicalTypeWithLength;
import io.rtdi.bigdata.connector.pipeline.foundation.exceptions.PipelineCallerException;
import io.rtdi.bigdata.connector.pipeline.foundation.recordbuilders.AvroField;

public class Column {

	private Field field;
	private String columnname;
	private String hanadatatype;
	private IAvroDatatype avrotype;
	private List<String> path;

	public Column(Field f, List<String> path) {
		this.field = f;
		String name = AvroField.getOriginalName(field);
		if (path != null && path.size() != 0) {
			StringBuffer b = new StringBuffer();
			for ( String p : path) {
				b.append(p);
				b.append('_');
			}
			b.append(name);
			this.columnname = b.toString(); 
		} else {
			this.columnname = name;
		}
		avrotype = AvroType.getAvroDataType(f.schema());
		this.hanadatatype = getSnowflakeDatatype(avrotype);
		this.path = path;
	}

	public Column(String name, String hanadatatype) {
		this.columnname = name;
		this.hanadatatype = hanadatatype;
	}

	public String getColumnName() {
		return columnname;
	}

	public String getDataTypeString() {
		return hanadatatype;
	}

	private static String getSnowflakeDatatype(IAvroDatatype avrotype) {
		switch (avrotype.getAvroType()) {
		case AVROANYPRIMITIVE:
			return "VARCHAR(5000)";
		case AVROARRAY:
			return "VARCHAR(5000)";
		case AVROSTRING:
			return "VARCHAR(5000)";
		case AVROBYTE:
		case AVROSHORT:
			return "SMALLINT";
		case AVROBOOLEAN:
			return "BOOLEAN";
		case AVROBYTES:
			return "BINARY";
		case AVROCLOB:
		case AVRONCLOB:
			return "VARCHAR";
		case AVRODATE:
			return "DATE";
		case AVROFLOAT:
			return "FLOAT";
		case AVROINT:
			return "INT";
		case AVROLONG:
			return "BIGINT";
		case AVRONVARCHAR: {
			AvroVarchar t = AvroVarchar.create(((LogicalTypeWithLength) avrotype).getLength());
			return t.toString();
		}
		case AVROSTGEOMETRY:
		case AVROSTPOINT:
			return "GEOMETRY";
		case AVROTIMEMICROS:
		case AVROTIMEMILLIS:
			return "TIME";
		case AVROTIMESTAMPMICROS:
		case AVROTIMESTAMPMILLIS:
			return "TIMESTAMP_NTZ";
		case AVROURI:
			return "VARCHAR(5000)";
		case AVROUUID:
			return "VARCHAR(36)";
		case AVROVARCHAR:
			return avrotype.toString();
		case AVRODOUBLE:
			return "DOUBLE";
		case AVRODECIMAL:
			return avrotype.toString();
		case AVROFIXED:
			return "VARBINARY";
		case AVROENUM:
		case AVROMAP:
		case AVRORECORD:
		default:
			return avrotype.toString();
		}
	}

	public Field getField() {
		return field;
	}

	public Object getValue(JexlRecord record) throws ConnectorCallerException {
		if (path != null && path.size() != 0) {
			for ( String p : path) {
				record = (JexlRecord) record.get(p);
			}
		}
		if (record == null) {
			return null;
		} else {
			return getSnowflakeNativeValue(record.get(field.name()));
		}
	}

	protected Object getSnowflakeNativeValue(Object value) throws ConnectorCallerException {
		if (value == null) {
			return null;
		}
		try {
			switch (avrotype.getAvroType()) {
			case AVROANYPRIMITIVE:
				break;
			case AVROARRAY:
				break;
			case AVROBOOLEAN:
				break;
			case AVROBYTE:
				return ((AvroByte) avrotype).convertToJava(value).shortValue();
			case AVROBYTES:
				break;
			case AVROCLOB:
				break;
			case AVRODATE: {
				Instant date = ((AvroDate) avrotype).convertToJava(value);
				return new Date(date.toEpochMilli());
			}
			case AVRODECIMAL:
				break;
			case AVRODOUBLE:
				break;
			case AVROENUM:
				break;
			case AVROFIXED:
				break;
			case AVROFLOAT:
				break;
			case AVROINT:
				break;
			case AVROLONG:
				break;
			case AVROMAP:
				break;
			case AVRONCLOB:
				break;
			case AVRONVARCHAR:
				break;
			case AVRORECORD:
				break;
			case AVROSHORT:
				break;
			case AVROSTGEOMETRY:
				break;
			case AVROSTPOINT:
				break;
			case AVROSTRING:
				break;
			case AVROTIMEMICROS:
				break;
			case AVROTIMEMILLIS:
				break;
			case AVROTIMESTAMPMICROS:
				break;
			case AVROTIMESTAMPMILLIS: {
				Instant utc = ((AvroTimestamp) avrotype).convertToJava(value);
				Timestamp ts = Timestamp.from(utc);
				return ts;
			}
			case AVROURI:
				break;
			case AVROUUID:
				break;
			case AVROVARCHAR:
				break;
			default:
				break;
			}
			return avrotype.convertToJava(value);
		} catch (PipelineCallerException e) {
			throw new ConnectorCallerException(e.getMessage(), e.getCause(), e.getHint(), e.getCausingObject());
		}
	}
	
	@Override
	public String toString() {
		return columnname;
	}

}
