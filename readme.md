<h1>PARQUET </h1>
<p>
  <span style="color: rgb(0,0,0);">
    <a href="https://parquet.apache.org/documentation/latest/">Parquet </a>is a columnar storage format implying that data is stored column-wise instead of row-wise.</span>
</p>
<p>
  <br/>
</p>
<p>Parquet is built to support very efficient compression and encoding schemes. Multiple projects have demonstrated the performance impact of applying the right compression and encoding scheme to the data. Parquet allows compression schemes to be specified on a per-column level and is future-proofed to allow adding more encodings as they are invented and implemented.</p>
<p>
  <span style="color: rgb(51,51,51);font-size: 20.0px;letter-spacing: -0.008em;">Features :</span>
</p>
<ul>
  <li>
    <strong>
      <strong>
        <span style="color: rgb(51,51,51);">Column Chunks: </span>
      </strong>
    </strong>Composed of pages written back to back. The pages share a common header and readers can skip over page they are not interested in. The data for the page follows the header and can be compressed and/or encoded. The compression and encoding is specified in the page metadata.</li>
  <li>
    <p>
      <strong>
        <strong>Error Recovery: </strong>
      </strong>If the file metadata is corrupt, the file is lost. If the column metadata is corrupt, that column chunk is lost (but column chunks for this column in other row groups are okay). If a page header is corrupt, the remaining pages in that chunk are lost. If the data within a page is corrupt, that page is lost. The file will be more resilient to corruption with smaller row groups.</p>
  </li>
  <li>
    <p>
      <strong>Separating metadata and column data: </strong>The format is explicitly designed to separate the metadata from the data. This allows splitting columns into multiple files, as well as having a single metadata file reference multiple parquet files.</p>
  </li>
  <li>
    <strong>
      <strong>Simple data types: </strong>
    </strong>The types supported by the file format are intended to be as minimal as possible, with a focus on how the types effect on disk storage. For example, 16-bit ints are not explicitly supported in the storage format since they are covered by 32-bit ints with an efficient encoding. This reduces the complexity of implementing readers and writers for the format.</li>
</ul>
<h2>
  <span style="color: rgb(0,0,0);">Advantages :</span>
</h2>
<ul>
  <li>
    <span style="color: rgb(0,0,0);">Better compression ratio as the data (column) has the same datatype</span>
  </li>
  <li>
    <span style="color: rgb(0,0,0);">Faster read when a smaller fraction of columns is to be read</span>
  </li>
  <li>
    <span style="color: rgb(0,0,0);">Allows choosing different encoding for different rows</span> <span style="color: rgb(0,0,0);"> <br/> </span>
  </li>
  <li>
    <span style="color: rgb(0,0,0);">Faster Reads because metadata is stored on various levels not just on the top level.</span>
  </li>
  <li>
    <span style="color: rgb(0,0,0);">Better error recovery</span>
  </li>
</ul>
<p>
  <span style="color: rgb(0,0,0);">The above reasons combined give the benefits of reduced storage, network, and processing costs while also reducing processing time. </span>
</p>
<h2>When to use and how:</h2>
<h3>When</h3>
<ul>
  <li>If the data is low entropy </li>
  <li>If the data can be converted into a form such that blocks of data are low entropy.</li>
</ul>
<h3>How</h3>
<ul>
  <li>Sort the data into a form such that blocks of data having low entropy are given</li>
  <li>eg If we have a Billings table of a large chain of electronics stores. Then sorting the data by storeId's would give blocks of data of low entropy as a store may have repeating customerID's ,  billingEmployeeID's and so on. This will result in better compression due to similar values.</li>
</ul>
<h1>AVRO </h1>
<p>Apache <a href="https://avro.apache.org/docs/1.10.0/">Avro</a> is a data serialization system.</p>
<h2 class="h3">Schemas</h2>
<p>Avro relies on <em>
    <a href="https://avro.apache.org/docs/1.10.0/#schemas">schemas</a>
  </em>. When Avro data is read, the schema used when writing it is always present. This permits each datum to be written with no per-value overheads, making serialization both fast and small. This also facilitates use with dynamic, scripting languages, since data, together with its schema, is fully self-describing.</p>
<p>When Avro data is stored in a file, its schema is stored with it, so that files may be processed later by any program. If the program reading the data expects a different schema this can be easily resolved, since both schemas are present.</p>
<p>When Avro is used in RPC, the client and server exchange schemas in the connection handshake. (This can be optimized so that, for most calls, no schemas are actually transmitted.) Since both client and server both have the other's full schema, correspondence between same named fields, missing fields, extra fields, etc. can all be easily resolved.</p>
<p>Avro schemas are defined with <a href="https://www.json.org/">JSON</a> . This facilitates implementation in languages that already have JSON libraries.</p>
<p>
  <a href="https://avro.apache.org/docs/1.10.0/spec.html#schema_record">Schema Record Definition</a>
</p>
<h1>AIM</h1>
<p>The utility aims to provide a simple interface to convert CSV and JSON files to parquet format. Apart from this one can connect to the database and extract the results of a query into the parquet file format.</p>
<h2>Requirements :</h2>
<ul>
  <li>Windows: hadoop.dll, hdfs.dll (<a href="https://github.com/cdarlint/winutils">link</a>)</li>
  <li>DevDepencies: maven </li>
  <li>Maven Dependencies : (<ac:link>
      <ri:attachment ri:filename="pom.xml"/>
      <ac:plain-text-link-body><![CDATA[file]]></ac:plain-text-link-body>
    </ac:link>)</li>
</ul>
<h2>How to get started :</h2>
<h3>Build</h3>
<ol>
  <li>install apache-maven if u don't have it</li>
  <li>cd project base directory</li>
  <li>mvn package</li>
</ol>
<p>It gives a shaded jar with all the dependencies. This jar can be run from anywhere</p>
<h3>Run</h3>
<p>java -jar parquetcli.jar</p>
<h2>Use cases : </h2>
<ul>
  <li>
    <p>Convert a CSV file to a parquet file.</p>
  </li>
  <li>
    <p>Save the result of a query from a database into the parquet file format</p>
  </li>
  <li>
    <p>Split data into a dataset based on the month  by specifying the date containing column and format of data (if not in ISO format)</p>
  </li>
  <li>
    <p>Split data into a dataset alphabetically by specifying the string containing column and format of data</p>
  </li>
</ul>
<h2>Input Format :</h2>
<h3>General Command</h3>
<p>Usage:  parquet  [options]  [command]  [command  options] <br/> <br/>    Options: <br/> <br/>        -v,  --verbose,  --debug <br/>    Print  extra  debugging  information <br/> <br/>    Commands: <br/> <br/>        help <br/>    Retrieves  details  on  the  functions  of  other  commands <br/>        meta <br/>    Print  a  Parquet  file's  metadata <br/>        pages <br/>    Print  page  summaries  for  a  Parquet  file <br/>        dictionary <br/>    Print  dictionaries  for  a  Parquet  column <br/>        check-stats <br/>    Check  Parquet  files  for  corrupt  page  and  column  stats  (PARQUET-251) <br/>        csv-schema <br/>    Build  a  schema  from  a  CSV  data  sample <br/>        convert-csv <br/>    Create  a  file  from  CSV  data <br/>        head <br/>    Print  the  first  N  records  from  a  file <br/>        column-index <br/>    Prints  the  column  and  offset  indexes  of  a  Parquet  file <br/>        convert-sql <br/>    Convert  from  sql  to  parquet <br/> <br/>    Examples: <br/> <br/>        #  print  information  for  create <br/>        parquet  help  create <br/> <br/>    See  'parquet help command'  for more information on a  specific command.</p>
<h3>help convert-csv</h3>
<p>Usage: parquet [general options] convert-csv {file path} [command options] <br/> <br/>  Description: <br/> <br/>    Create a file from CSV data <br/> <br/>  Command options: <br/> <br/>    --no-header <br/>        Don't use first line as CSV header <br/>    --delimiter <br/>        Delimiter character <br/>    -s, --schema <br/>        The file containing the Avro schema. <br/>    --escape <br/>        Escape character <br/>    --skip-lines <br/>        Lines to skip before CSV start <br/>    --column-split <br/>        Column based on which the table is to be split <br/>    --compression-codec <br/>        A compression codec name. <br/>    --date-format <br/>        Date format for the monthly split column <br/>    -o, --output <br/>        Output file path <br/>    --alphabetical-split <br/>        Column Name containing date to split the database <br/>    --header <br/>        Line to use as a header. Must match the CSV settings. <br/>    --require <br/>        Do not allow null values for the given field <br/>    -2, --format-version-2, --writer-version-2 <br/>        Use Parquet format version 2 <br/>    --date-in-string <br/>        if Date is in string format (default false for db , true for csv) <br/>    --quote <br/>        Quote character <br/>    --monthly-split <br/>        Column Name containing date to split the database <br/>    --row-group-size <br/>        Target row group size <br/>    --overwrite <br/>        Remove any data already in the target view or dataset <br/>    --page-size <br/>        Target page size <br/>    --dictionary-size <br/>        Max dictionary page size <br/> <br/>  Examples: <br/> <br/>    # Create a Parquet file from a CSV file <br/>    parquet convert-csv sample.csv <br/> <br/>    # Create a Parquet file from a CSV file specifying outputfile <br/>    parquet convert-csv path/to/sample.csv -o path/to/output.parquet</p>
<h3>help convert-sql</h3>
<p>
  <br/>Usage: parquet [general options] convert-sql {file path} [command options] <br/> <br/>  Description: <br/> <br/>    Convert from sql to parquet <br/> <br/>  Command options: <br/> <br/>  * -u, --username <br/>        Database Username <br/>  * -d, --db-url <br/>        Connection URL for the database <br/>    --column-split <br/>        Column based on which the table is to be split <br/>    --compression-codec <br/>        A compression codec name. <br/>    --date-format <br/>        Date format for the monthly split column <br/>    -o, --output <br/>        Output file path <br/>    --alphabetical-split <br/>        Column Name containing date to split the database <br/>  * -p, --password <br/>        Database Password <br/>    -2, --format-version-2, --writer-version-2 <br/>        Use Parquet format version 2 <br/>    --date-in-string <br/>        if Date is in string format (default false for db , true for csv) <br/>    --monthly-split <br/>        Column Name containing date to split the database <br/>    --row-group-size <br/>        Target row group size <br/>    --overwrite <br/>        Remove any data already in the target view or dataset <br/>    --page-size <br/>        Target page size <br/>    --dictionary-size <br/>        Max dictionary page size <br/> <br/>  * = required <br/> <br/>  Examples: <br/> <br/>    # Convert the output of the query in the file "input.sql" in "outputfile.parquet": <br/>    parquet convert-sql input.sql -u username -p -d "jdbc:mysql://remotemysql.com:3306/uPWKeSgHt8" -o outputfile.parquet</p>
<h3>Date Time Format</h3>
<p>See how to create date-time format string <a href="https://docs.oracle.com/javase/8/docs/api/java/text/SimpleDateFormat.html">here</a>. </p>
<h2>Implementation( ):</h2>
<h3>STEP 1 (Generate an AVRO Schema)(Separate for different file types) </h3>
<ul>
  <li>
    <p>CSV: Generate an Avro schema using the headers and the first column. (maybe inaccurate)</p>
  </li>
  <li>
    <p>SQL:  Generate the Avro schema using resultset metadata. (high accuracy).</p>
  </li>
</ul>
<h3>STEP 2 (Create ParquetWriters)(Common)</h3>
<ul>
  <li>
    <p>Use input parameters to decide the required number of parquet writers</p>
  </li>
  <li>
    <p>Create all the parquet writers required for the given task using various parameters</p>
  </li>
</ul>
<h3>STEP 3 (Read and Convert to AVRO record)(Separate for different file types)</h3>
<ul>
  <li>
    <p>CSV: Use AvroCSVReader (out of the box) to read a CSV row and convert it into a record </p>
  </li>
  <li>
    <p>SQL: Convert one row from the resultset into an AVRO record.</p>
  </li>
</ul>
<h3>STEP 4 (Write the record using the writers)(common)</h3>
<ul>
  <li>
    <p>Write the record by selecting a parquet writer created earlier.</p>
  </li>
  <li>
    <p>Select a parquet writer based on the value of differentiator field specified in the input </p>
  </li>
</ul>
<h3>STEP 5 (common)</h3>
<ul>
  <li>
    <p>If all the rows have not been converted go to step 3</p>
  </li>
  <li>
    <p>Else close all the parquet writers and delete the files in which no data was written</p>
  </li>
</ul>
<h2>
  <span style="letter-spacing: -0.008em;">Limitations And Possible Errors:</span>
</h2>
<h3>Converts only 1 SQL Query </h3>
<p>The utility converts only 1 SQL Statement which has to be a query statement (can include anything like joins etc).</p>
<h3>CSV Schema generated may be incorrect</h3>
<p>The CSV schema is made using the first row only, therefore if the first row is incorrect then the conversion may not be possible.</p>
<h3>Memory limit may exceed</h3>
<p>Each parquet writer requires a max of 128 MB by default. Each file requires a separate parquet writer. If the data is to be split into many files and the data is large, then there may be a memory limit error.</p>
<p>It can be bypassed by reducing the row group size by using the flag --row-group-size.</p>
<p>Solution (not yet implemented) : If the data is sorted by the column according to which splitting is to be done, then the splitting can be achieved using a single parquet writer removing the above error.</p>
<h3>Date Time Format</h3>
<p>The default date-time format is <span style="color: rgb(152,195,121);">yyyy-MM-dd'T'HH:mm:ss</span>
  <span style="color: rgb(152,195,121);">
    <span style="color: rgb(95,102,114);">. The format is not required for a SQL query but is required for CSV data.</span>
  </span>
</p>
<h3>Possible Errors </h3>
<ul>
  <li>
    <p>The column based on which splitting is to be done may not be present in the table.</p>
  </li>
  <li>
    <p>The first row of the CSV may not be representative of the rest of the rows.</p>
  </li>
  <li>
    <p>The DB connection string may be incorrect.</p>
  </li>
  <li>
    <p>The SQL may not be a query or may return null.</p>
  </li>
</ul>
<p>
  <br/>
</p>
<p>
  <br/>
</p>
