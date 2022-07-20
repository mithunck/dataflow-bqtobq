package com.dataflow_bqtobq.model;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import com.google.api.services.bigquery.model.TableRow;

public class SourceData {
	@DefaultCoder(AvroCoder.class)
	public static class SourceDataValue{
		String firstName;
		String lastName;
		
		public String getFirstName() {
			return firstName;
		}

		public void setFirstName(String firstName) {
			this.firstName = firstName;
		}

		public String getLastName() {
			return lastName;
		}

		public void setLastName(String lastName) {
			this.lastName = lastName;
		}

		public static SourceDataValue fromSourceTable(TableRow row) {
			SourceDataValue data = new SourceDataValue();
			data.setFirstName((String) row.get("FIRST_NAME"));
			data.setLastName((String) row.get("LAST_NAME"));
			return data;
			
		}
	}

}
