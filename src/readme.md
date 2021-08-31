Url : jdbc:mysql://remotemysql.com:3306/uPWKeSgHt8
User : uPWKeSgHt8
Password : VwSGk00hzd

cd /home/piyush/myprojects/parquet-cli ; /usr/lib/jvm/java-14-openjdk-amd64/bin/java -XX:+ShowCodeDetailsInExceptionMessages -Dfile.encoding=UTF-8 @/tmp/cp_5tiypa3eqxtux8togya5iiupx.argfile com.oracle.parquet.App convert-sql  letsgo.sql -u uPWKeSgHt8 -p VwSGk00hzd -d "jdbc:mysql://remotemysql.com:3306/uPWKeSgHt8" -o outputfile.parquet