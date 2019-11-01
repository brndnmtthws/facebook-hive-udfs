[![Build Status](https://travis-ci.org/brndnmtthws/facebook-hive-udfs.svg?branch=master)](https://travis-ci.org/brndnmtthws/facebook-hive-udfs) [![Dependabot Status](https://api.dependabot.com/badges/status?host=github&repo=brndnmtthws/facebook-hive-udfs)](https://dependabot.com)


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

# HOW DO USE IT?

Like any other UDF, silly!

Here's a sample:

```
CREATE TEMPORARY FUNCTION md5 AS 'com.facebook.hive.udf.UDFMD5';
SELECT md5(password) from users limit 1;
```

**cool!!**
