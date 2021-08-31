package com.oracle.parquet.commands;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.DriverManager;
import java.sql.SQLException;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.oracle.parquet.jdbc.ResultSetSchemaGenerator;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import java.util.List;


import static com.oracle.parquet.jdbc.JdbcUtils.extractResult;

@Parameters(commandDescription = "Convert from sql to parquet")
public class DBSQLCommand extends BaseCommandConvert {

	@Parameter(names={"-d", "--db-url"},
			description="Connection URL for the database",
			required = true)
	String dbUrl;

	@Parameter(names={"-u", "--username"},
			description="Database Username",
			required = true)
	String userName;

	@Parameter(names={"-p", "--password"},
			description="Database Password",
			password =  true,
			required = true)
	String passwd;

	public DBSQLCommand(Logger console) {
		super(console);
	}
	String recordName = "query";

	public void executeStatement(Connection conn,String content){
		console.debug("Executing Statement at the db");
		try {
			Statement statement = conn.createStatement();
			statement.setFetchSize(100000);
            ResultSet resultSet = statement.executeQuery(content);

			console.debug("SQL Statement Executed Successfully");
            if (!resultSet.isBeforeFirst() ) {
                console.info("Query did not return any data");
                return;
            }

			ResultSetMetaData rsmd = resultSet.getMetaData();
			int colNumber = rsmd.getColumnCount();

			console.debug("Generating Schema");
			Schema jdbcSchema = new ResultSetSchemaGenerator().generateSchema(rsmd, recordName);
			console.debug("Schema Generated Successfully");

			configureWriters(jdbcSchema);

			int count = 1;

			while(resultSet.next()){
				Record record = new Record(jdbcSchema);
				try{
					for (int i = 1; i <=colNumber; i++) {
						record.put(rsmd.getColumnLabel(i),extractResult(rsmd.getColumnType(i), resultSet, i));
					}
					writeRecord(record);
					if(count%10000==0)
						console.debug(count+":");
					count++;
				}
				catch (RuntimeException e) {
					throw new RuntimeException("Failed on record " + count, e);
				}
			}
			console.debug("All rows Succesfully read and converted into parquet");
			closeWriters();
			console.debug("Task Completed");
		}
		catch (Exception e){
			throw new RuntimeException("Unable convert SQL query to parquet",e);
		}
	}

	@Override
	public int run() throws IOException{
		Preconditions.checkArgument(
				sourceFiles != null && !sourceFiles.isEmpty(),
				"Missing file name");
		Preconditions.checkArgument(sourceFiles.size() == 1,
				"Only one file can be given");

		final String source = sourceFiles.get(0);

		if(dateInString==null) dateInString = false;

		if(outputPath == null){
			recordName = FilenameUtils.getBaseName(source);
			outputPath = FilenameUtils.removeExtension(source);
		}
        else {
			recordName = FilenameUtils.getBaseName(outputPath);
			outputPath = FilenameUtils.removeExtension(outputPath);
		}
		console.debug(outputPath);

		String content = "";
		try{
			content = new String(Files.readAllBytes(Paths.get(source)), StandardCharsets.UTF_8);
		}
		catch (Exception e){
			throw new RuntimeException("Unable to access SQL File",e);
		}

		Connection conn = null;
		try {
			conn = DriverManager.getConnection(dbUrl, userName, passwd);
			executeStatement(conn, content);
		} catch (SQLException ex) {
			System.out.println("SQLException: " + ex.getMessage());
			System.out.println("SQLState: " + ex.getSQLState());
			System.out.println("VendorError: " + ex.getErrorCode());
		} catch(Exception e){
			throw new RuntimeException("SQL Error",e);
		}

		return 0;
	}

	@Override
	public List<String> getExamples() {
		return Lists.newArrayList(
				"# Convert the output of the query in the file \"input.sql\" in \"outputfile.parquet\":",
				"input.sql -u username -p -d \"jdbc:mysql://remotemysql.com:3306/uPWKeSgHt8\" -o outputfile.parquet"
		);
	}
}

