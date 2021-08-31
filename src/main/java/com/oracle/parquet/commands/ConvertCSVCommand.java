/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.oracle.parquet.commands;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import org.apache.parquet.cli.csv.AvroCSVReader;
import org.apache.parquet.cli.csv.CSVProperties;
import org.apache.parquet.cli.csv.AvroCSV;
import org.apache.parquet.cli.util.Schemas;
import org.apache.avro.Schema;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Set;

import static org.apache.avro.generic.GenericData.Record;

@Parameters(commandDescription = "Create a file from CSV data")
public class ConvertCSVCommand extends BaseCommandConvert {

	public ConvertCSVCommand(Logger console) {
		super(console);
	}

	@Parameter(names = "--delimiter", description = "Delimiter character")
	String delimiter = ",";

	@Parameter(names = "--escape", description = "Escape character")
	String escape = "\\";

	@Parameter(names = "--quote", description = "Quote character")
	String quote = "\"";

	@Parameter(names = "--no-header", description = "Don't use first line as CSV header")
	boolean noHeader = false;

	@Parameter(names = "--skip-lines", description = "Lines to skip before CSV start")
	int linesToSkip = 0;

	@Parameter(names = "--charset", description = "Character set name", hidden = true)
	String charsetName = Charset.defaultCharset().displayName();

	@Parameter(names = "--header", description = "Line to use as a header. Must match the CSV settings.")
	String header;

	@Parameter(names = "--require", description = "Do not allow null values for the given field")
	List<String> requiredFields;

	@Parameter(names = { "-s", "--schema" }, description = "The file containing the Avro schema.")
	String avroSchemaFile;

	@Override
	public int run() throws IOException {
		Preconditions.checkArgument(
            sourceFiles != null && !sourceFiles.isEmpty(),
            "Missing file name");
        Preconditions.checkArgument(sourceFiles.size() == 1,
            "Only one file can be given");

		String source = sourceFiles.get(0);

		if(dateInString==null) dateInString = true;

		String recordName="csvConvert";

		if(outputPath == null){
			recordName = FilenameUtils.getBaseName(source);
			outputPath = FilenameUtils.removeExtension(source); 
		}
        else {
			recordName = FilenameUtils.getBaseName(outputPath);
			outputPath = FilenameUtils.removeExtension(outputPath); 
		}            
		console.debug(outputPath);

		CSVProperties props = new CSVProperties.Builder()
				.delimiter(delimiter)
				.escape(escape)
				.quote(quote)
				.header(header)
				.hasHeader(!noHeader)
				.linesToSkip(linesToSkip)
				.charset(charsetName)
				.build();


		Schema csvSchema;
		console.debug("Generating CSV Schema");
		if (avroSchemaFile != null) {
			csvSchema = Schemas.fromAvsc(open(avroSchemaFile));
		} else {
			Set<String> required = ImmutableSet.of();
			if (requiredFields != null) {
				required = ImmutableSet.copyOf(requiredFields);
			}

			String filename = new File(source).getName();
			if (filename.contains(".")) {
				recordName = filename.substring(0, filename.indexOf("."));
			} else {
				recordName = filename;
			}

			csvSchema = AvroCSV.inferNullableSchema(
					recordName, open(source), props, required);
		}

		console.debug("CSV Schema Generated");
		configureWriters(csvSchema);

		long count = 1;
		try (AvroCSVReader<Record> reader = new AvroCSVReader<>(
				open(source), props, csvSchema, Record.class, true)) 
		{
			console.debug("CSV Reader Initialised successfully");
			try{
				for (Record record : reader){
					writeRecord(record);

					if(count%10000==0)
						console.debug(count+":");
					count++;
				}
			}
			catch (Exception e) {
				throw new RuntimeException("Failed on record " + count, e);
			}
		}
		console.debug("All rows Succesfully read and converted into parquet");
		closeWriters();
		console.debug("Task Completed");
		return 0;
	}

	@Override
	public List<String> getExamples() {
		return Lists.newArrayList(
				"# Create a Parquet file from a CSV file",
				"sample.csv",
				"# Create a Parquet file from a CSV file specifying outputfile",
				"path/to/sample.csv -o path/to/output.parquet"
		);
	}
}
