This package provides an implementation of the Spark Data Sources API
for MemSQL. For example usage, see the main function in
MemSQLRelation.scala.

Compile the package with `sbt package`. Running the demo app can be
done by tweaking paths in the run.sh shell script and running it.
Note `sbt package` produces a standalone jar which does not contain
dependencies. Run `sbt assembly` to produce a fat jar which includes transitive
dependencies.

The files JDBCRDD.scala, JDBCRelation.scala, and DriverQuirks are
sourced from version 1.3.0 of the main Spark repository. They have
been added to the memsql package and the private access modifiers have
been stripped.
