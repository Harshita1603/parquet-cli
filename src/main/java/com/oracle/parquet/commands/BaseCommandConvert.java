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
import com.google.common.collect.Lists;

import java.text.ParseException;

import org.apache.parquet.cli.BaseCommand;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.commons.io.FilenameUtils;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.cli.util.Codecs;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.text.SimpleDateFormat;
import java.util.HashMap;

import static org.apache.avro.generic.GenericData.Record;
import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_1_0;
import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_2_0;

@Parameters(commandDescription="Common Class to convert files to parquet")
public class BaseCommandConvert extends BaseCommand {

	public BaseCommandConvert(Logger console) {
        super(console);
    }

	@Parameter(description="<file path>")
	List<String> sourceFiles;

	@Parameter(
			names={"-o", "--output"},
			description="Output file path")
	String outputPath = null;

	@Parameter(
			names={"-2", "--format-version-2", "--writer-version-2"},
			description="Use Parquet format version 2")
    boolean v2 = false;
    
    @Parameter(names = {"--monthly-split"},
			description = "Column Name containing date to split the database")
    String dateColString;

    @Parameter(names = {"--date-in-string"},
            description = "if Date is in string format (default false for db , true for csv)")
    Boolean dateInString;

    @Parameter(names = {"--date-format"},
            description = "Date format for the monthly split column")
    String sdateFormat = "yyyy-MM-dd'T'HH:mm:ss";

    @Parameter(names = {"--alphabetical-split"},
			description = "Column Name containing date to split the database")
    String alphaColString;

    @Parameter(names = {"--column-split"},
			description = "Column based on which the table is to be split")
    String splitColString;

	@Parameter(names = {"--compression-codec"},
			description = "A compression codec name.")
	String compressionCodecName = "SNAPPY";

	@Parameter(names="--row-group-size", description="Target row group size")
	int rowGroupSize = ParquetWriter.DEFAULT_BLOCK_SIZE;

	@Parameter(names="--page-size", description="Target page size")
	int pageSize = ParquetWriter.DEFAULT_PAGE_SIZE;

	@Parameter(names="--dictionary-size", description="Max dictionary page size")
	int dictionaryPageSize = ParquetWriter.DEFAULT_PAGE_SIZE;

	@Parameter(
			names={"--overwrite"},
			description="Remove any data already in the target view or dataset")
    boolean overwrite = false;
    
    int writeOption=1;
    List <ParquetWriter<Record>> writerList = new ArrayList<>();
    List<String> fileNames = new ArrayList<>(); 
    Map <String,Integer> writerMap  =  new HashMap<String,Integer>(); 
    

    @Override
    public int run() throws IOException {
        return 0;
    }

	@Override
	public List<String> getExamples() {
		return Lists.newArrayList(
            "not needed"
		);
    }

    private Schema genericSchema;
    
    public ParquetWriter<Record> getParquetWriter(String outPath, Schema schema) throws IOException{
        CompressionCodecName codec = Codecs.parquetCodec(compressionCodecName);
        ParquetWriter<Record> writer = AvroParquetWriter
					.<Record>builder(qualifiedPath(outPath+".parquet"))
					.withWriterVersion(v2 ? PARQUET_2_0 : PARQUET_1_0)
					.withWriteMode(overwrite ?
							ParquetFileWriter.Mode.OVERWRITE : ParquetFileWriter.Mode.CREATE)
					.withCompressionCodec(codec)
					.withDictionaryEncoding(true)
					.withDictionaryPageSize(dictionaryPageSize)
                    .withPageSize(pageSize)
					.withRowGroupSize(rowGroupSize)
					.withDataModel(GenericData.get())
					.withConf(getConf())
                    .withSchema(schema)
                    .build();
        fileNames.add(outPath+".parquet");
        return writer;
    }

    public void configureWriters (Schema schema) throws IOException {
		console.debug("Configuring Writers" );

        genericSchema = schema;
        int ct=0;
        if(alphaColString != null) ct++;
        if(dateColString != null) ct++;
        if(splitColString != null) ct++;

        if(ct>1) 
            throw new RuntimeException("Can't split according to both row and column");

        if(alphaColString!=null){
            writeOption = 2;
            if(schema.getField(alphaColString)==null) 
                throw new RuntimeException("Alphabet SplitwiseColumn not present");
            writerList.add(getParquetWriter(outputPath +"/null", schema));

        } else if(dateColString!=null){
            writeOption = 3;
            if(schema.getField(dateColString)==null)
                throw new RuntimeException("Date SplitwiseColumn not present");

            for (int i = 0; i < 12; i++) {
                writerList.add(getParquetWriter(outputPath + '/'+Integer.toString(i), schema));
            }
            writerList.add(getParquetWriter(outputPath +"/null", schema));
        } else if(splitColString != null){
            writeOption = 4;
            if(schema.getField(splitColString)==null) 
                throw new RuntimeException("Splitting Column not present");
            writerList.add(getParquetWriter(outputPath +"/null", schema));
        } 
        else {
            writerList.add(getParquetWriter(outputPath, schema));
            return ;
        }
		console.debug("Writers Configured , write option :" + writeOption );
    }

    public void writeSimple (Record record) throws IOException {
        writerList.get(0).write(record);
    }

    public void writeAlpha(Record record ) throws IOException,RuntimeException{
        Object obj = record.get(alphaColString);
        int pos = 0;
        if(obj!=null&&obj.toString().isEmpty()==false) {
            Character ch = obj.toString().charAt(0);
            if(writerMap.containsKey(ch+"")){
                pos = writerMap.get(ch+"");
            }
            else {
                writerList.add(getParquetWriter(outputPath + '/'+ch, genericSchema));
                pos = writerList.size()-1;
                writerMap.put(ch+"",pos);
            }
        }

        writerList.get(pos).write(record);
    }

    public void writeDate(Record record) throws IOException,ParseException {
        Object obj = record.get(dateColString);
        int pos = 12;
        Calendar cal = Calendar.getInstance();
        cal.setTimeZone(TimeZone.getTimeZone("UTC"));
        if(obj!=null&&obj.toString().isEmpty()==false) {
            String s = obj.toString();
            if(dateInString){
                cal.setTime(new SimpleDateFormat(sdateFormat).parse(s));
            }
            else {
                cal.setTimeInMillis(Long.parseLong(s));
            }
            pos = cal.get(Calendar.MONTH);
        }
        writerList.get(pos).write(record);
    }
    public void writeSplitCol(Record record ) throws IOException,RuntimeException{
        Object obj = record.get(splitColString);
        int pos = 0;
        if(obj!=null&&obj.toString().isEmpty()==false) {
            String ch = obj.toString();
            if(writerMap.containsKey(ch+"")){
                pos = writerMap.get(ch+"");
            }
            else {
                writerList.add(getParquetWriter(outputPath + '/'+ch, genericSchema));
                pos = writerList.size()-1;
                writerMap.put(ch+"",pos);
            }
        }

        writerList.get(pos).write(record);
    }

    public void writeRecord(Record record) throws IOException,ParseException {
        switch (writeOption){
            case 1: writeSimple(record);break;
            case 2: writeAlpha(record);break;
            case 3: writeDate(record);break;
            case 4: writeSplitCol(record);break;
        }
    }

    public void closeWriters () throws IOException {
		console.debug("Closing all writers , Deleting unnesary files");
        int i=0;
        for(ParquetWriter<Record> writer : writerList){
            boolean shouldDelete = (writer.getDataSize()==0);
            writer.close();
            
            if(shouldDelete){
                File file = new File(fileNames.get(i)); 
                file.delete();
                String temp = FilenameUtils.getFullPath(fileNames.get(i)) + "."+FilenameUtils.getName(fileNames.get(i))+".crc";
                file = new File(temp);
                file.delete();
            }
            i++;
        }
    }
}
