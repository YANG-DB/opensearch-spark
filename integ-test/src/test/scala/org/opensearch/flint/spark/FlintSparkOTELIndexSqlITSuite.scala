/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.json4s.native.JsonMethods.parse
import org.json4s.native.Serialization
import org.json4s.{Formats, NoTypeHints}
import org.opensearch.flint.core.FlintOptions
import org.opensearch.flint.core.storage.FlintOpenSearchClient
import org.opensearch.flint.spark.covering.FlintSparkCoveringIndex.getFlintIndexName
import org.opensearch.flint.spark.skipping.FlintSparkSkippingIndex.getSkippingIndexName
import org.scalatest.matchers.must.Matchers.defined
import org.scalatest.matchers.should.Matchers.{convertToAnyShouldWrapper, the}

import scala.Option.empty
import scala.collection.JavaConverters.mapAsJavaMapConverter

class FlintSparkOTELIndexSqlITSuite extends FlintSparkSuite {

  /** Test table and index name */
  private val testTable = "spark_catalog.default.covering_sql_test"
  private val testIndex = "name_and_age"
  private val testFlintIndex = getFlintIndexName(testIndex, testTable)

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  override def afterEach(): Unit = {
    super.afterEach()

    // Delete all test indices
    flint.deleteIndex(testFlintIndex)
  }

  test("create cloud-trail table index with auto refresh") {
    val frame = sql(s"""
               |CREATE TABLE default.aws_cloudtrail (
               |  Records ARRAY<STRUCT<
               |    awsRegion: STRING,
               |    eventCategory: STRING,
               |    eventID: STRING,
               |    eventName: STRING,
               |    eventSource: STRING,
               |    eventTime: STRING,
               |    eventType: STRING,
               |    eventVersion: STRING,
               |    managementEvent: STRING,
               |    readOnly: BOOLEAN,
               |    recipientAccountId: STRING,
               |    requestID: STRING,
               |    requestParameters: STRUCT<
               |      assumeRolePolicyDocument: STRING,
               |      description: STRING,
               |      roleName: STRING
               |    >,
               |    responseElements: STRUCT<
               |      role: STRUCT<
               |        arn: STRING,
               |        assumeRolePolicyDocument: STRING,
               |        createDate: STRING,
               |        path: STRING,
               |        roleId: STRING,
               |        roleName: STRING
               |      >
               |    >,
               |    sessionCredentialFromConsole: STRING,
               |    sourceIPAddress: STRING,
               |    tlsDetails: STRUCT<
               |      cipherSuite: STRING,
               |      clientProvidedHostHeader: STRING,
               |      tlsVersion: STRING
               |    >,
               |    userAgent: STRING,
               |    userIdentity: STRUCT<
               |      accessKeyId: STRING,
               |      accountId: STRING,
               |      arn: STRING,
               |      principalId: STRING,
               |      sessionContext: STRUCT<
               |        attributes: STRUCT<
               |          creationDate: STRING,
               |          mfaAuthenticated: STRING
               |        >
               |      >,
               |      type: STRING,
               |      userName: STRING
               |    >
               |  >>
               |) USING json """.stripMargin)
    frame.show()

    val jsonString: String =
      """{"Records": [
        |  {
        |  "eventVersion": "1.08",
        |  "userIdentity": {
        |    "type": "IAMUser",
        |    "principalId": "AIDA6ON6E4XEGITEXAMPLE",
        |    "arn": "arn:aws:iam::777777777777:user/Saanvi",
        |    "accountId": "777777777777",
        |    "accessKeyId": "AKIAIOSFODNN7EXAMPLE",
        |    "userName": "Saanvi",
        |    "sessionContext": {
        |      "sessionIssuer": {},
        |      "webIdFederationData": {},
        |      "attributes": {
        |        "creationDate": "2023-07-19T21:11:57Z",
        |        "mfaAuthenticated": "false"
        |      }
        |    }
        |  },
        |  "eventTime": "2023-07-19T21:29:12Z",
        |  "eventSource": "iam.amazonaws.com",
        |  "eventName": "CreateRole",
        |  "awsRegion": "us-east-1",
        |  "sourceIPAddress": "192.0.2.0",
        |  "userAgent": "aws-cli/2.13.5 Python/3.11.4 Linux/4.14.255-314-253.539.amzn2.x86_64 exec-env/CloudShell exe/x86_64.amzn.2 prompt/off command/iam.create-role",
        |  "requestParameters": {
        |    "roleName": "TestRole",
        |    "description": "Allows EC2 instances to call AWS services on your behalf.",
        |    "assumeRolePolicyDocument": "{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Action\":[\"sts:AssumeRole\"],\"Principal\":{\"Service\":[\"ec2.amazonaws.com\"]}}]}"
        |  },
        |  "responseElements": {
        |    "role": {
        |      "assumeRolePolicyDocument": "%7B%22Version%22%3A%222012-10-17%22%2C%22Statement%22%3A%5B%7B%22Effect%22%3A%22Allow%22%2C%22Action%22%3A%5B%22sts%3AAssumeRole%22%5D%2C%22Principal%22%3A%7B%22Service%22%3A%5B%22ec2.amazonaws.com%22%5D%7D%7D%5D%7D",
        |      "arn": "arn:aws:iam::777777777777:role/TestRole",
        |      "roleId": "AROA6ON6E4XEFFEXAMPLE",
        |      "createDate": "Jul 19, 2023 9:29:12 PM",
        |      "roleName": "TestRole",
        |      "path": "/"
        |    }
        |  },
        |  "requestID": "ff38f36e-ebd3-425b-9939-EXAMPLE1bbe",
        |  "eventID": "9da77cd0-493f-4c89-8852-EXAMPLEa887c",
        |  "readOnly": false,
        |  "eventType": "AwsApiCall",
        |  "managementEvent": "true",
        |  "recipientAccountId": "777777777777",
        |  "eventCategory": "Management",
        |  "tlsDetails": {
        |    "tlsVersion": "TLSv1.2",
        |    "cipherSuite": "ECDHE-RSA-AES128-GCM-SHA256",
        |    "clientProvidedHostHeader": "iam.amazonaws.com"
        |  },
        |  "sessionCredentialFromConsole": "true"
        |}]}""".stripMargin
    val jsonRDD: RDD[String] = spark.sparkContext.parallelize(Seq(jsonString))
    val df = spark.read.json(jsonRDD)
    // Print DataFrame schema to check
    df.printSchema()
    // Create table from DataFrame
//    df.write.mode("overwrite").saveAsTable("aws_cloudtrail")
//    val showDDL = spark.sql("SHOW CREATE TABLE aws_cloudtrail")
//    showDDL.show(false)
    df.write.insertInto("aws_cloudtrail")
    val result = sql(s"""
         |SELECT
         |        exploded_records.eventVersion AS `aws.cloudtrail.eventVersion`,
         |        exploded_records.userIdentity.type AS `aws.cloudtrail.userIdentity.type`,
         |        exploded_records.userIdentity.principalId AS `aws.cloudtrail.userIdentity.principalId`,
         |        exploded_records.userIdentity.arn AS `aws.cloudtrail.userIdentity.arn`,
         |        exploded_records.userIdentity.accountId AS `aws.cloudtrail.userIdentity.accountId`,
         |        exploded_records.userIdentity.accessKeyId AS `aws.cloudtrail.userIdentity.accessKeyId`,
         |        exploded_records.userIdentity.userName AS `aws.cloudtrail.userIdentity.userName`,
         |        exploded_records.userIdentity.sessionContext.attributes.creationDate AS `aws.cloudtrail.userIdentity.sessionContext.attributes.creationDate`,
         |        exploded_records.userIdentity.sessionContext.attributes.mfaAuthenticated AS `aws.cloudtrail.userIdentity.sessionContext.attributes.mfaAuthenticated`,
         |        exploded_records.eventTime AS `aws.cloudtrail.eventTime`,
         |        exploded_records.eventSource AS `aws.cloudtrail.eventSource`,
         |        exploded_records.eventName AS `aws.cloudtrail.eventName`,
         |        exploded_records.awsRegion AS `aws.cloudtrail.awsRegion`,
         |        exploded_records.sourceIPAddress AS `aws.cloudtrail.sourceIPAddress`,
         |        exploded_records.userAgent AS `aws.cloudtrail.userAgent`,
         |        exploded_records.requestParameters.roleName AS `aws.cloudtrail.requestParameters.roleName`,
         |        exploded_records.requestParameters.description AS `aws.cloudtrail.requestParameters.description`,
         |        exploded_records.requestParameters.assumeRolePolicyDocument AS `aws.cloudtrail.requestParameters.assumeRolePolicyDocument`,
         |        exploded_records.responseElements.role.assumeRolePolicyDocument AS `aws.cloudtrail.responseElements.role.assumeRolePolicyDocument`,
         |        exploded_records.responseElements.role.arn AS `aws.cloudtrail.responseElements.role.arn`,
         |        exploded_records.responseElements.role.roleId AS `aws.cloudtrail.responseElements.role.roleId`,
         |        exploded_records.responseElements.role.createDate AS `aws.cloudtrail.responseElements.role.createDate`,
         |        exploded_records.responseElements.role.roleName AS `aws.cloudtrail.responseElements.role.roleName`,
         |        exploded_records.responseElements.role.path AS `aws.cloudtrail.responseElements.role.path`,
         |        exploded_records.requestID AS `aws.cloudtrail.requestID`,
         |        exploded_records.eventID AS `aws.cloudtrail.eventID`,
         |        exploded_records.readOnly AS `aws.cloudtrail.readOnly`,
         |        exploded_records.eventType AS `aws.cloudtrail.eventType`,
         |        exploded_records.managementEvent AS `aws.cloudtrail.managementEvent`,
         |        exploded_records.recipientAccountId AS `aws.cloudtrail.recipientAccountId`,
         |        exploded_records.eventCategory AS `aws.cloudtrail.eventCategory`,
         |        exploded_records.tlsDetails.tlsVersion AS `aws.cloudtrail.tlsDetails.tlsVersion`,
         |        exploded_records.tlsDetails.cipherSuite AS `aws.cloudtrail.tlsDetails.cipherSuite`,
         |        exploded_records.tlsDetails.clientProvidedHostHeader AS `aws.cloudtrail.tlsDetails.clientProvidedHostHeader`,
         |        exploded_records.sessionCredentialFromConsole AS `aws.cloudtrail.sessionCredentialFromConsole`
         |    FROM (
         |             SELECT explode(Records) as exploded_records
         |             FROM aws_cloudtrail
         |         ) t
         |""".stripMargin)
    result.show()
  }

  test("create otel_traces table index with auto refresh") {
    val frame = sql(s"""CREATE TABLE default.otel_traces (
         |  resourceSpans ARRAY<STRUCT<
         |      resource: STRUCT<
         |        attributes: ARRAY<STRUCT<key: STRING, value: STRUCT<stringValue: STRING>>>>,
         |  scopeSpans: ARRAY<STRUCT<
         |    scope: STRUCT<name: STRING, version: STRING>,
         |    spans: ARRAY<STRUCT<
         |      attributes: ARRAY<STRUCT<key: STRING, value: STRUCT<intValue: STRING, stringValue: STRING>>>,
         |      endTimeUnixNano: STRING,
         |      kind: BIGINT,
         |      name: STRING,
         |      parentSpanId: STRING,
         |      spanId: STRING,
         |      startTimeUnixNano: STRING,
         |      traceId: STRING>>>>>>)
         |  USING json""".stripMargin)
    frame.show()

    val jsonString1: String =
      s"""{
         |  "resourceSpans": [
         |    {
         |      "resource": {
         |        "attributes": [
         |          {
         |            "key": "telemetry.sdk.version",
         |            "value": {
         |              "stringValue": "1.3.0"
         |            }
         |          },
         |          {
         |            "key": "telemetry.sdk.name",
         |            "value": {
         |              "stringValue": "opentelemetry"
         |            }
         |          },
         |          {
         |            "key": "telemetry.sdk.language",
         |            "value": {
         |              "stringValue": "erlang"
         |            }
         |          },
         |          {
         |            "key": "service.name",
         |            "value": {
         |              "stringValue": "featureflagservice"
         |            }
         |          },
         |          {
         |            "key": "service.instance.id",
         |            "value": {
         |              "stringValue": "featureflagservice@e083872efcb9"
         |            }
         |          },
         |          {
         |            "key": "process.runtime.version",
         |            "value": {
         |              "stringValue": "11.2.2.13"
         |            }
         |          },
         |          {
         |            "key": "process.runtime.name",
         |            "value": {
         |              "stringValue": "BEAM"
         |            }
         |          },
         |          {
         |            "key": "process.runtime.description",
         |            "value": {
         |              "stringValue": "Erlang/OTP 23 erts-11.2.2.13"
         |            }
         |          },
         |          {
         |            "key": "process.executable.name",
         |            "value": {
         |              "stringValue": "featureflagservice"
         |            }
         |          }
         |        ]
         |      },
         |      "scopeSpans": [
         |        {
         |          "scope": {
         |            "name": "opentelemetry_phoenix",
         |            "version": "1.1.1"
         |          },
         |          "spans": [
         |            {
         |              "traceId": "bc342fb3fbfa54c2188595b89b0b1cd8",
         |              "spanId": "9b355ca40dd98f5e",
         |              "parentSpanId": "",
         |              "name": "/",
         |              "kind": 2,
         |              "startTimeUnixNano": "1698098982169733148",
         |              "endTimeUnixNano": "1698098982206571283",
         |              "attributes": [
         |                {
         |                  "key": "phoenix.plug",
         |                  "value": {
         |                    "stringValue": "Elixir.FeatureflagserviceWeb.PageController"
         |                  }
         |                },
         |                {
         |                  "key": "phoenix.action",
         |                  "value": {
         |                    "stringValue": "index"
         |                  }
         |                },
         |                {
         |                  "key": "net.transport",
         |                  "value": {
         |                    "stringValue": "IP.TCP"
         |                  }
         |                },
         |                {
         |                  "key": "net.sock.peer.addr",
         |                  "value": {
         |                    "stringValue": "127.0.0.1"
         |                  }
         |                },
         |                {
         |                  "key": "net.sock.host.addr",
         |                  "value": {
         |                    "stringValue": "127.0.0.1"
         |                  }
         |                },
         |                {
         |                  "key": "net.peer.port",
         |                  "value": {
         |                    "intValue": "45796"
         |                  }
         |                },
         |                {
         |                  "key": "net.host.port",
         |                  "value": {
         |                    "intValue": "8081"
         |                  }
         |                },
         |                {
         |                  "key": "net.host.name",
         |                  "value": {
         |                    "stringValue": "localhost"
         |                  }
         |                },
         |                {
         |                  "key": "http.user_agent",
         |                  "value": {
         |                    "stringValue": "curl/7.64.0"
         |                  }
         |                },
         |                {
         |                  "key": "http.target",
         |                  "value": {
         |                    "stringValue": "/"
         |                  }
         |                },
         |                {
         |                  "key": "http.status_code",
         |                  "value": {
         |                    "intValue": "200"
         |                  }
         |                },
         |                {
         |                  "key": "http.scheme",
         |                  "value": {
         |                    "stringValue": "http"
         |                  }
         |                },
         |                {
         |                  "key": "http.route",
         |                  "value": {
         |                    "stringValue": "/"
         |                  }
         |                },
         |                {
         |                  "key": "http.method",
         |                  "value": {
         |                    "stringValue": "GET"
         |                  }
         |                },
         |                {
         |                  "key": "http.flavor",
         |                  "value": {
         |                    "stringValue": "1.1"
         |                  }
         |                },
         |                {
         |                  "key": "http.client_ip",
         |                  "value": {
         |                    "stringValue": "127.0.0.1"
         |                  }
         |                }
         |              ],
         |              "status": {}
         |            }
         |          ]
         |        }
         |      ]
         |    }
         |  ]
         |}""".stripMargin
    val jsonRDD1: RDD[String] = spark.sparkContext.parallelize(Seq(jsonString1))
    val df1 = spark.read.json(jsonRDD1)
    df1.write.insertInto("default.otel_traces")

    val jsonString2: String =
      s"""{
         |  "resourceSpans": [
         |    {
         |      "resource": {
         |        "attributes": [
         |          {
         |            "key": "telemetry.sdk.version",
         |            "value": {
         |              "stringValue": "1.3.0"
         |            }
         |          },
         |          {
         |            "key": "telemetry.sdk.name",
         |            "value": {
         |              "stringValue": "opentelemetry"
         |            }
         |          },
         |          {
         |            "key": "telemetry.sdk.language",
         |            "value": {
         |              "stringValue": "erlang"
         |            }
         |          },
         |          {
         |            "key": "service.name",
         |            "value": {
         |              "stringValue": "featureflagservice"
         |            }
         |          },
         |          {
         |            "key": "service.instance.id",
         |            "value": {
         |              "stringValue": "featureflagservice@e083872efcb9"
         |            }
         |          },
         |          {
         |            "key": "process.runtime.version",
         |            "value": {
         |              "stringValue": "11.2.2.13"
         |            }
         |          },
         |          {
         |            "key": "process.runtime.name",
         |            "value": {
         |              "stringValue": "BEAM"
         |            }
         |          },
         |          {
         |            "key": "process.runtime.description",
         |            "value": {
         |              "stringValue": "Erlang/OTP 23 erts-11.2.2.13"
         |            }
         |          },
         |          {
         |            "key": "process.executable.name",
         |            "value": {
         |              "stringValue": "featureflagservice"
         |            }
         |          }
         |        ]
         |      },
         |      "scopeSpans": [
         |        {
         |          "scope": {
         |            "name": "opentelemetry_ecto",
         |            "version": "1.1.1"
         |          },
         |          "spans": [
         |            {
         |              "traceId": "bc342fb3fbfa54c2188595b89b0b1cd8",
         |              "spanId": "87acd6659b425f80",
         |              "parentSpanId": "9b355ca40dd98f5e",
         |              "name": "featureflagservice.repo.query:featureflags",
         |              "kind": 3,
         |              "startTimeUnixNano": "1698098982170068232",
         |              "endTimeUnixNano": "1698098982202276205",
         |              "attributes": [
         |                {
         |                  "key": "total_time_microseconds",
         |                  "value": {
         |                    "intValue": "31286"
         |                  }
         |                },
         |                {
         |                  "key": "source",
         |                  "value": {
         |                    "stringValue": "featureflags"
         |                  }
         |                },
         |                {
         |                  "key": "queue_time_microseconds",
         |                  "value": {
         |                    "intValue": "13579"
         |                  }
         |                },
         |                {
         |                  "key": "query_time_microseconds",
         |                  "value": {
         |                    "intValue": "17698"
         |                  }
         |                },
         |                {
         |                  "key": "idle_time_microseconds",
         |                  "value": {
         |                    "intValue": "307054"
         |                  }
         |                },
         |                {
         |                  "key": "decode_time_microseconds",
         |                  "value": {
         |                    "intValue": "8"
         |                  }
         |                },
         |                {
         |                  "key": "db.url",
         |                  "value": {
         |                    "stringValue": "ecto://ffs_postgres"
         |                  }
         |                },
         |                {
         |                  "key": "db.type",
         |                  "value": {
         |                    "stringValue": "sql"
         |                  }
         |                },
         |                {
         |                  "key": "db.statement",
         |                  "value": {
         |                    "stringValue": " AS f0"
         |                  }
         |                },
         |                {
         |                  "key": "db.name",
         |                  "value": {
         |                    "stringValue": "ffs"
         |                  }
         |                },
         |                {
         |                  "key": "db.instance",
         |                  "value": {
         |                    "stringValue": "ffs"
         |                  }
         |                }
         |              ],
         |              "status": {}
         |            }
         |          ]
         |        }
         |      ]
         |    }
         |  ]
         |}""".stripMargin
    val jsonRDD2: RDD[String] = spark.sparkContext.parallelize(Seq(jsonString2))
    val df2 = spark.read.json(jsonRDD2)
    df2.write.insertInto("default.otel_traces")

    // Print DataFrame schema to check
//    df.printSchema()
    // Create table from DataFrame
//    df.write.mode("overwrite").saveAsTable("default.otel_table")
//    val showDDL = spark.sql("SHOW CREATE TABLE default.otel_table")
//    showDDL.show(false)
    val result = sql(s"""
         | SELECT * from default.otel_traces
         |""".stripMargin)
    result.show()

    val stats = sql(
      s"""
         |ANALYZE TABLE default.otel_traces COMPUTE STATISTICS;
         |
         |""".stripMargin)
    stats.show()
  }

  test("create covering index with auto refresh") {
    sql(s"""
         | CREATE INDEX $testIndex ON $testTable
         | (name, age)
         | WITH (auto_refresh = true)
         |""".stripMargin)

    // Wait for streaming job complete current micro batch
    val job = spark.streams.active.find(_.name == testFlintIndex)
    job shouldBe defined
    failAfter(streamingTimeout) {
      job.get.processAllAvailable()
    }

    val indexData = flint.queryIndex(testFlintIndex)
    indexData.count() shouldBe 2
  }

  test("create covering index with streaming job options") {
    withTempDir { checkpointDir =>
      sql(s"""
             | CREATE INDEX $testIndex ON $testTable ( name )
             | WITH (
             |   auto_refresh = true,
             |   refresh_interval = '5 Seconds',
             |   checkpoint_location = '${checkpointDir.getAbsolutePath}'
             | )
             | """.stripMargin)

      val index = flint.describeIndex(testFlintIndex)
      index shouldBe defined
      index.get.options.autoRefresh() shouldBe true
      index.get.options.refreshInterval() shouldBe Some("5 Seconds")
      index.get.options.checkpointLocation() shouldBe Some(
        checkpointDir.getAbsolutePath
      )
    }
  }

  test("create covering index with index settings") {
    sql(s"""
           | CREATE INDEX $testIndex ON $testTable ( name )
           | WITH (
           |   index_settings = '{"number_of_shards": 2, "number_of_replicas": 3}'
           | )
           |""".stripMargin)

    // Check if the index setting option is set to OS index setting
    val flintClient =
      new FlintOpenSearchClient(new FlintOptions(openSearchOptions.asJava))

    implicit val formats: Formats = Serialization.formats(NoTypeHints)
    val settings =
      parse(flintClient.getIndexMetadata(testFlintIndex).indexSettings.get)
    (settings \ "index.number_of_shards").extract[String] shouldBe "2"
    (settings \ "index.number_of_replicas").extract[String] shouldBe "3"
  }

  test("create covering index with invalid option") {
    the[IllegalArgumentException] thrownBy
      sql(s"""
             | CREATE INDEX $testIndex ON $testTable
             | (name, age)
             | WITH (autoRefresh = true)
             | """.stripMargin)
  }

  test("create covering index with manual refresh") {
    sql(s"""
           | CREATE INDEX $testIndex ON $testTable
           | (name, age)
           |""".stripMargin)

    val indexData = flint.queryIndex(testFlintIndex)

    flint.describeIndex(testFlintIndex) shouldBe defined
    indexData.count() shouldBe 0

    sql(s"REFRESH INDEX $testIndex ON $testTable")
    indexData.count() shouldBe 2
  }

  test("create covering index on table without database name") {
    sql(s"CREATE INDEX $testIndex ON covering_sql_test (name)")

    flint.describeIndex(testFlintIndex) shouldBe defined
  }

  test("create covering index on table in other database") {
    sql("CREATE SCHEMA sample")
    sql("USE sample")

    // Create index without database name specified
    sql("CREATE TABLE test1 (name STRING) USING CSV")
    sql(s"CREATE INDEX $testIndex ON sample.test1 (name)")

    // Create index with database name specified
    sql("CREATE TABLE test2 (name STRING) USING CSV")
    sql(s"CREATE INDEX $testIndex ON sample.test2 (name)")

    try {
      flint.describeIndex(
        s"flint_spark_catalog_sample_test1_${testIndex}_index"
      ) shouldBe defined
      flint.describeIndex(
        s"flint_spark_catalog_sample_test2_${testIndex}_index"
      ) shouldBe defined
    } finally {
      sql("DROP DATABASE sample CASCADE")
    }
  }

  test("create covering index on table in other database than current") {
    sql("CREATE SCHEMA sample")
    sql("USE sample")

    // Specify database "default" in table name instead of current "sample" database
    sql(s"CREATE INDEX $testIndex ON $testTable (name)")

    try {
      flint.describeIndex(testFlintIndex) shouldBe defined
    } finally {
      sql("DROP DATABASE sample CASCADE")
    }
  }

  test("create covering index if not exists") {
    sql(s"""
           | CREATE INDEX IF NOT EXISTS $testIndex
           | ON $testTable (name, age)
           |""".stripMargin)
    flint.describeIndex(testFlintIndex) shouldBe defined

    // Expect error without IF NOT EXISTS, otherwise success
    assertThrows[IllegalStateException] {
      sql(s"""
             | CREATE INDEX $testIndex
             | ON $testTable (name, age)
             |""".stripMargin)
    }
    sql(s"""
           | CREATE INDEX IF NOT EXISTS $testIndex
           | ON $testTable (name, age)
           |""".stripMargin)
  }

  test("show all covering index on the source table") {
    flint
      .coveringIndex()
      .name(testIndex)
      .onTable(testTable)
      .addIndexColumns("name", "age")
      .create()

    // Create another covering index
    flint
      .coveringIndex()
      .name("idx_address")
      .onTable(testTable)
      .addIndexColumns("address")
      .create()

    // Create a skipping index which is expected to be filtered
    flint
      .skippingIndex()
      .onTable(testTable)
      .addPartitions("year", "month")
      .create()

    val result = sql(s"SHOW INDEX ON $testTable")
    checkAnswer(result, Seq(Row(testIndex), Row("idx_address")))

    flint.deleteIndex(getFlintIndexName("idx_address", testTable))
    flint.deleteIndex(getSkippingIndexName(testTable))
  }

  test("describe covering index") {
    flint
      .coveringIndex()
      .name(testIndex)
      .onTable(testTable)
      .addIndexColumns("name", "age")
      .create()

    val result = sql(s"DESC INDEX $testIndex ON $testTable")
    checkAnswer(
      result,
      Seq(Row("name", "string", "indexed"), Row("age", "int", "indexed"))
    )
  }

  test("drop covering index") {
    flint
      .coveringIndex()
      .name(testIndex)
      .onTable(testTable)
      .addIndexColumns("name", "age")
      .create()

    sql(s"DROP INDEX $testIndex ON $testTable")

    flint.describeIndex(testFlintIndex) shouldBe empty
  }
}
