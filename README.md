[![Build & test with Maven](https://github.com/brndnmtthws/facebook-hive-udfs/actions/workflows/build-and-test.yml/badge.svg)](https://github.com/brndnmtthws/facebook-hive-udfs/actions/workflows/build-and-test.yml)

facebook-hive-udfs
==================

Facebook's Hive UDFs

# WHAT IT IS

A computer guy at Facebook dumped a bunch of UDFs/UDAFs here:

https://issues.apache.org/jira/browse/HIVE-1545

However, the code does not build and is missing many parts.

This is a partial copy of that code, except it builds and may (or may not) work. Use at your own risk. To build it:

```
mvn package
```

This will produce a jar in `target/` which you can add to your Hive classpath.

Alternatively, you can use the published jar included with [this repo's packages](https://github.com/brndnmtthws/facebook-hive-udfs/packages).

You can add this repository as a maven source with:

```xml
<project>
...
  <repositories>
    <repository>
      <id>github</id>
      <name>facebook-hive-udfs</name>
      <url>https://maven.pkg.github.com/brndnmtthws/facebook-hive-udfs</url>
      <snapshots>
        <enabled>true</enabled>
      </snapshots>
    </repository>
  </repositories>
...
</project>
```

And then include it in your `pom.xml`:

```xml
<dependency>
  <groupId>com.airbnb</groupId>
  <artifactId>facebook-udfs</artifactId>
  <version>1.1.5</version>
</dependency>
```

# HOW DO USE IT?

Like any other UDF, silly!

Here's a sample:

```
CREATE TEMPORARY FUNCTION md5 AS 'com.facebook.hive.udf.UDFMD5';
SELECT md5(password) from users limit 1;
```

**cool!!**
