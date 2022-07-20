package com.dataflow_bqtobq.pipeline;

import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

import com.dataflow_bqtobq.model.SourceData;
import com.dataflow_bqtobq.model.SourceData.SourceDataValue;
import com.google.api.services.bigquery.model.TableRow;

import java.util.UUID;

public class BqToBqPipeline {
	
	private static final String outputTable="spry-effect-356214:sample_dataset.target_table";
	

	private static void bigQueryTransform() {
		DataflowPipelineOptions pipelineOptions = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
		pipelineOptions.setJobName("copyData");
		pipelineOptions.setProject("spry-effect-356214");
		pipelineOptions.setRegion("us-central1");
		pipelineOptions.setRunner(DataflowRunner.class);
		pipelineOptions.setTemplateLocation("gs://dataflow-bqtobq-ws/template");
		pipelineOptions.setStagingLocation("gs://dataflow-bqtobq-ws/staging");
		pipelineOptions.setTempLocation("gs://dataflow-bqtobq-ws/temp");
		pipelineOptions.setGcpTempLocation("gs://dataflow-bqtobq-ws/temp");
		
		Pipeline pipeline = Pipeline.create(pipelineOptions);
		
		String sourceProject="spry-effect-356214";
		String sourceDataSet="sample_dataset";
		String sourceTable="source_table";
		
		
		PCollection sourceTableRecords = pipeline.apply("Read from BigQuery query", BigQueryIO.readTableRows().fromQuery(
				String.format("select * from `%s.%s.%s`",sourceProject,sourceDataSet,sourceTable)).usingStandardSql()).apply("TableRows to SourceDta",
						MapElements.into(TypeDescriptor.of(SourceData.SourceDataValue.class))
						.via(SourceData.SourceDataValue::fromSourceTable));
		
		PCollection sourceRows= (PCollection)sourceTableRecords.apply(ParDo.of(new transformSourceTable()));
		
		sourceRows.apply("Insert into target table", BigQueryIO.writeTableRows().to(outputTable)
				.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
				.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));
		
		PipelineResult result = pipeline.run();
	    try {
	        result.getState();
	        result.waitUntilFinish();
	    } catch (UnsupportedOperationException e) {
	       // do nothing
	    } catch (Exception e) {
	        e.printStackTrace();
	    }
	}
	
	private static class transformSourceTable extends DoFn<SourceDataValue, TableRow>{
		@ProcessElement
		public void process(ProcessContext processContext) {
			SourceDataValue record= processContext.element();
			TableRow tableRow = new TableRow();
			tableRow.set("FIRST_NAME", record.getFirstName().toLowerCase());
			tableRow.set("LAST_NAME", record.getLastName().toLowerCase());
			tableRow.set("NUMBER", UUID.randomUUID());
			processContext.output(tableRow);
			
		}
		
	}
	
	public static void main(String[] args) {
		bigQueryTransform();
	}
}

