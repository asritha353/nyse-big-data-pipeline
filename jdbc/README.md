# PostgreSQL JDBC driver

Sqoop 1.4.7 requires a modern PostgreSQL JDBC driver to connect to PostgreSQL
16 with SCRAM authentication. Download version 42.7.7 from Maven Central:

```powershell
Invoke-WebRequest `
  -Uri "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.7/postgresql-42.7.7.jar" `
  -OutFile ".\jdbc\postgresql-42.7.7.jar"

Copy-Item `
  ".\jdbc\postgresql-42.7.7.jar" `
  ".\sqoop-custom\postgresql-42.7.7.jar"
```

JAR files are downloaded dependencies and are intentionally excluded from Git.
