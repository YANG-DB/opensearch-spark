/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.IOUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row}
import org.json4s.native.JsonMethods.parse
import org.json4s.native.Serialization
import org.json4s.{Formats, NoTypeHints}
import org.opensearch.flint.core.FlintOptions
import org.opensearch.flint.core.storage.FlintOpenSearchClient
import org.opensearch.flint.spark.covering.FlintSparkCoveringIndex.getFlintIndexName
import org.opensearch.flint.spark.skipping.FlintSparkSkippingIndex.getSkippingIndexName
import org.scalatest.matchers.must.Matchers.defined
import org.scalatest.matchers.should.Matchers.{convertToAnyShouldWrapper, the}

import java.io.ByteArrayInputStream
import scala.collection.JavaConverters._
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

  test("create cloud-front table index with auto refresh") {
    val frame = sql(s"""CREATE TABLE cloudfront (
                       |    log_date STRING,
                       |    log_time DATE,
                       |    x_edge_location STRING,
                       |    sc_bytes BIGINT,
                       |    c_ip STRING,
                       |    cs_method STRING,
                       |    cs_host STRING,
                       |    cs_uri_stem STRING,
                       |    sc_status INT,
                       |    cs_referer STRING,
                       |    cs_user_agent STRING,
                       |    cs_uri_query STRING,
                       |    cs_cookie STRING,
                       |    x_edge_result_type STRING,
                       |    x_edge_request_id STRING,
                       |    x_host_header STRING,
                       |    cs_protocol STRING,
                       |    cs_bytes BIGINT,
                       |    time_taken float,
                       |    x_forwarded_for STRING,
                       |    ssl_protocol STRING,
                       |    ssl_cipher STRING,
                       |    x_edge_response_result_type STRING,
                       |    cs_protocol_version STRING,
                       |    fle_status STRING,
                       |    fle_encrypted_fields STRING,
                       |    c_port INT,
                       |    time_to_first_byte float,
                       |    x_edge_detailed_result_type STRING,
                       |    sc_content_type STRING,
                       |    sc_content_len STRING,
                       |    sc_range_start STRING,
                       |    sc_range_end STRING
                       |)
                       |USING csv
                       |OPTIONS (
                       |  sep='\t'
                       |);
                       |""".stripMargin)
    frame.show()

    val csv: String =
      """2023-02-22	03:22:37	HKG62-C2	675	13.248.48.9	GET	d2wusnbjo8x1w7.cloudfront.net	/	304	-	Mozilla/5.0%20(Macintosh;%20Intel%20Mac%20OS%20X%2010_15_7)%20AppleWebKit/537.36%20(KHTML,%20like%20Gecko)%20Chrome/110.0.0.0%20Safari/537.36	-	-	Miss	Y5rIQOuGsI2vJN4hR3qLB55Cn4aoogvzPEnHhm5-0NiTtWDfTU5-vw==	d2wusnbjo8x1w7.cloudfront.net	https	536	0.623	-	TLSv1.3	TLS_AES_128_GCM_SHA256	Miss	HTTP/2.0	-	-	12812	0.623	Miss	-	-	-	-
        |2023-02-22	03:22:38	HKG62-C2	675	13.248.48.9	GET	d2wusnbjo8x1w7.cloudfront.net	/static/css/main.3c74189a.css	304	-	Mozilla/5.0%20(Macintosh;%20Intel%20Mac%20OS%20X%2010_15_7)%20AppleWebKit/537.36%20(KHTML,%20like%20Gecko)%20Chrome/110.0.0.0%20Safari/537.36	-	-	Miss	IPGkM0N8_4AU6ok71zDa4twWLigSM7Ib33IwRsBHm1hDSmIvWoNjBA==	d2wusnbjo8x1w7.cloudfront.net	https	118	0.656	-	TLSv1.3	TLS_AES_128_GCM_SHA256	Miss	HTTP/2.0	-	-	12812	0.656	Miss	-	-	-	-
        |2023-02-22	03:22:42	HKG62-C2	677	13.248.48.9	GET	d2wusnbjo8x1w7.cloudfront.net	/manifest.json	304	-	Mozilla/5.0%20(Macintosh;%20Intel%20Mac%20OS%20X%2010_15_7)%20AppleWebKit/537.36%20(KHTML,%20like%20Gecko)%20Chrome/110.0.0.0%20Safari/537.36	-	-	Miss	5sXuyCQs0mSgb2mN-KUcHW6z6LDQd12JBT0eE5E6RSJxwUZsxzT-kg==	d2wusnbjo8x1w7.cloudfront.net	https	410	0.582	-	TLSv1.3	TLS_AES_128_GCM_SHA256	Miss	HTTP/2.0	-	-	27007	0.582	Miss	-	-	-	-
        |2023-02-22	03:22:39	HKG62-C2	501279	13.248.48.9	GET	d2wusnbjo8x1w7.cloudfront.net	/static/js/main.1fce72cf.js	200	-	Mozilla/5.0%20(Macintosh;%20Intel%20Mac%20OS%20X%2010_15_7)%20AppleWebKit/537.36%20(KHTML,%20like%20Gecko)%20Chrome/110.0.0.0%20Safari/537.36	-	-	Miss	zdHQDWHvw3LXHsJ9il4jbrYX4XVaRejgVdcnuSNq4WmocHqM4wATkw==	d2wusnbjo8x1w7.cloudfront.net	https	70	1.606	-	TLSv1.3	TLS_AES_128_GCM_SHA256	Miss	HTTP/2.0	-	-	12812	0.840	Miss	application/javascript	-	-	-
        |2023-02-22	03:22:40	HKG62-C2	675	13.248.48.9	GET	d2wusnbjo8x1w7.cloudfront.net	/locales/en/home.json	304	-	Mozilla/5.0%20(Macintosh;%20Intel%20Mac%20OS%20X%2010_15_7)%20AppleWebKit/537.36%20(KHTML,%20like%20Gecko)%20Chrome/110.0.0.0%20Safari/537.36	v=v1.3.0	-	Miss	5FtX3W-8LR38cf05aTommPpuDAteiyix_LSSXF6T8bPJWa7eyKASpQ==	d2wusnbjo8x1w7.cloudfront.net	https	87	0.588	-	TLSv1.3	TLS_AES_128_GCM_SHA256	Miss	HTTP/2.0	-	-	12812	0.588	Miss	-	-	-	-
        |2023-02-22	03:22:40	HKG62-C2	675	13.248.48.9	GET	d2wusnbjo8x1w7.cloudfront.net	/locales/en/ekslog.json	304	-	Mozilla/5.0%20(Macintosh;%20Intel%20Mac%20OS%20X%2010_15_7)%20AppleWebKit/537.36%20(KHTML,%20like%20Gecko)%20Chrome/110.0.0.0%20Safari/537.36	v=v1.3.0	-	Miss	S4LYsYHTPEFsp2XCNcKGKtOyBYnoZObnkCXszEz_llNN1W9fN3Cskg==	d2wusnbjo8x1w7.cloudfront.net	https	78	0.592	-	TLSv1.3	TLS_AES_128_GCM_SHA256	Miss	HTTP/2.0	-	-	12812	0.592	Miss	-	-	-	-
        |2023-02-22	03:22:40	HKG62-C2	674	13.248.48.9	GET	d2wusnbjo8x1w7.cloudfront.net	/locales/en/cluster.json	304	-	Mozilla/5.0%20(Macintosh;%20Intel%20Mac%20OS%20X%2010_15_7)%20AppleWebKit/537.36%20(KHTML,%20like%20Gecko)%20Chrome/110.0.0.0%20Safari/537.36	v=v1.3.0	-	Miss	hVdTDiAs15WMiKmFOe-Wq0VmAiEU5QulF_qhbY4rPxOP0HbwpVcpFA==	d2wusnbjo8x1w7.cloudfront.net	https	78	0.592	-	TLSv1.3	TLS_AES_128_GCM_SHA256	Miss	HTTP/2.0	-	-	12812	0.592	Miss	-	-	-	-
        |2023-02-22	03:22:40	HKG62-C2	677	13.248.48.9	GET	d2wusnbjo8x1w7.cloudfront.net	/locales/en/servicelog.json	304	-	Mozilla/5.0%20(Macintosh;%20Intel%20Mac%20OS%20X%2010_15_7)%20AppleWebKit/537.36%20(KHTML,%20like%20Gecko)%20Chrome/110.0.0.0%20Safari/537.36	v=v1.3.0	-	Miss	hrLzG-wJDS3HffJNotXAkXtbQDQz1hy-PLG8YDLJnzv1KUFwIFG6pg==	d2wusnbjo8x1w7.cloudfront.net	https	103	0.594	-	TLSv1.3	TLS_AES_128_GCM_SHA256	Miss	HTTP/2.0	-	-	12812	0.594	Miss	-	-	-	-
        |2023-02-22	03:22:40	HKG62-C2	675	13.248.48.9	GET	d2wusnbjo8x1w7.cloudfront.net	/locales/en/resource.json	304	-	Mozilla/5.0%20(Macintosh;%20Intel%20Mac%20OS%20X%2010_15_7)%20AppleWebKit/537.36%20(KHTML,%20like%20Gecko)%20Chrome/110.0.0.0%20Safari/537.36	v=v1.3.0	-	Miss	WbaJxVX2Z5YMn-6ZItu60PYZxpYdTWDQR10mUdHc1zrebw8gkCmQAg==	d2wusnbjo8x1w7.cloudfront.net	https	79	0.596	-	TLSv1.3	TLS_AES_128_GCM_SHA256	Miss	HTTP/2.0	-	-	12812	0.596	Miss	-	-	-	-
        |2023-02-22	03:22:40	HKG62-C2	676	13.248.48.9	GET	d2wusnbjo8x1w7.cloudfront.net	/locales/en/common.json	304	-	Mozilla/5.0%20(Macintosh;%20Intel%20Mac%20OS%20X%2010_15_7)%20AppleWebKit/537.36%20(KHTML,%20like%20Gecko)%20Chrome/110.0.0.0%20Safari/537.36	v=v1.3.0	-	Miss	33FdRw1kaC_tOXHZXOiRvgTZpltIvUx9zIV9TwTl102zXIZOX0D02g==	d2wusnbjo8x1w7.cloudfront.net	https	78	0.600	-	TLSv1.3	TLS_AES_128_GCM_SHA256	Miss	HTTP/2.0	-	-	12812	0.600	Miss	-	-	-	-
        |2023-02-22	03:22:40	HKG62-C2	674	13.248.48.9	GET	d2wusnbjo8x1w7.cloudfront.net	/locales/en/applog.json	304	-	Mozilla/5.0%20(Macintosh;%20Intel%20Mac%20OS%20X%2010_15_7)%20AppleWebKit/537.36%20(KHTML,%20like%20Gecko)%20Chrome/110.0.0.0%20Safari/537.36	v=v1.3.0	-	Miss	aJ0T43g1SbxTcDBaapoFhxRDyevexOcx817F5Xm8vD1Fp19d_xUE4Q==	d2wusnbjo8x1w7.cloudfront.net	https	78	0.603	-	TLSv1.3	TLS_AES_128_GCM_SHA256	Miss	HTTP/2.0	-	-	12812	0.603	Miss	-	-	-	-
        |2023-02-22	03:22:40	HKG62-C2	675	13.248.48.9	GET	d2wusnbjo8x1w7.cloudfront.net	/locales/en/info.json	304	-	Mozilla/5.0%20(Macintosh;%20Intel%20Mac%20OS%20X%2010_15_7)%20AppleWebKit/537.36%20(KHTML,%20like%20Gecko)%20Chrome/110.0.0.0%20Safari/537.36	v=v1.3.0	-	Miss	L8evKthXCL7JfGkF5OPmzU-Ugwqua8Ye1PDCMW3kgAzTrea3lUfqvQ==	d2wusnbjo8x1w7.cloudfront.net	https	76	0.643	-	TLSv1.3	TLS_AES_128_GCM_SHA256	Miss	HTTP/2.0	-	-	12812	0.643	Miss	-	-	-	-
        |2023-02-22	03:22:41	HKG62-C2	703	13.248.48.9	GET	d2wusnbjo8x1w7.cloudfront.net	/locales/en-US/home.json	200	-	Mozilla/5.0%20(Macintosh;%20Intel%20Mac%20OS%20X%2010_15_7)%20AppleWebKit/537.36%20(KHTML,%20like%20Gecko)%20Chrome/110.0.0.0%20Safari/537.36	v=v1.3.0	-	Error	Bh9Z-n6bzLqlzwHGFHflVMnpjlsOC34oxMELel058PF36cTNvoCEig==	d2wusnbjo8x1w7.cloudfront.net	https	51	1.167	-	TLSv1.3	TLS_AES_128_GCM_SHA256	Error	HTTP/2.0	-	-	12812	1.167	Error	-	-	-	-
        |2023-02-22	03:22:41	HKG62-C2	703	13.248.48.9	GET	d2wusnbjo8x1w7.cloudfront.net	/locales/en-US/info.json	200	-	Mozilla/5.0%20(Macintosh;%20Intel%20Mac%20OS%20X%2010_15_7)%20AppleWebKit/537.36%20(KHTML,%20like%20Gecko)%20Chrome/110.0.0.0%20Safari/537.36	v=v1.3.0	-	Error	Qse_oGkdO2t-QWOXmo4BzPH-Tz-Tb2Y9e5xTekFQNxMOK5uMMHapWA==	d2wusnbjo8x1w7.cloudfront.net	https	51	0.284	-	TLSv1.3	TLS_AES_128_GCM_SHA256	Error	HTTP/2.0	-	-	12812	0.284	Error	-	-	-	-
        |2023-02-22	03:22:41	HKG62-C2	703	13.248.48.9	GET	d2wusnbjo8x1w7.cloudfront.net	/locales/en-US/applog.json	200	-	Mozilla/5.0%20(Macintosh;%20Intel%20Mac%20OS%20X%2010_15_7)%20AppleWebKit/537.36%20(KHTML,%20like%20Gecko)%20Chrome/110.0.0.0%20Safari/537.36	v=v1.3.0	-	Error	0oaXqSCWbKSAqoTMhRdShPMjVhmgW-Ma3HiiL_ZNkeXbrBkAxpSD8g==	d2wusnbjo8x1w7.cloudfront.net	https	52	0.309	-	TLSv1.3	TLS_AES_128_GCM_SHA256	Error	HTTP/2.0	-	-	12812	0.309	Error	-	-	-	-
        |2023-02-22	03:22:41	HKG62-C2	704	13.248.48.9	GET	d2wusnbjo8x1w7.cloudfront.net	/favicon.ico	200	-	Mozilla/5.0%20(Macintosh;%20Intel%20Mac%20OS%20X%2010_15_7)%20AppleWebKit/537.36%20(KHTML,%20like%20Gecko)%20Chrome/110.0.0.0%20Safari/537.36	-	-	Error	F2AB-kOnXsGXN_Qiynpg6-ZJeRyG5nweVAnOzM0vYhQBu4DZ3lVcyg==	d2wusnbjo8x1w7.cloudfront.net	https	91	1.169	-	TLSv1.3	TLS_AES_128_GCM_SHA256	Error	HTTP/2.0	-	-	12812	1.169	Error	-	-	-	-
        |2023-02-22	03:22:41	HKG62-C2	702	13.248.48.9	GET	d2wusnbjo8x1w7.cloudfront.net	/locales/en-US/common.json	200	-	Mozilla/5.0%20(Macintosh;%20Intel%20Mac%20OS%20X%2010_15_7)%20AppleWebKit/537.36%20(KHTML,%20like%20Gecko)%20Chrome/110.0.0.0%20Safari/537.36	v=v1.3.0	-	Error	oe_Y4moQc9VzUp-uj4BiiwgLiiFxGxA6pXmYFIlJICGf7r5c0wWyiQ==	d2wusnbjo8x1w7.cloudfront.net	https	52	1.169	-	TLSv1.3	TLS_AES_128_GCM_SHA256	Error	HTTP/2.0	-	-	12812	1.169	Error	-	-	-	-
        |2023-02-22	03:22:41	HKG62-C2	702	13.248.48.9	GET	d2wusnbjo8x1w7.cloudfront.net	/locales/en-US/servicelog.json	200	-	Mozilla/5.0%20(Macintosh;%20Intel%20Mac%20OS%20X%2010_15_7)%20AppleWebKit/537.36%20(KHTML,%20like%20Gecko)%20Chrome/110.0.0.0%20Safari/537.36	v=v1.3.0	-	Error	z6NJEfND977OSJndhHHPdhceerueb-qyGiF05FnnDtfUc3u-uAXCtg==	d2wusnbjo8x1w7.cloudfront.net	https	55	0.578	-	TLSv1.3	TLS_AES_128_GCM_SHA256	Error	HTTP/2.0	-	-	12812	0.578	Error	-	-	-	-
        |2023-02-22	03:22:41	HKG62-C2	702	13.248.48.9	GET	d2wusnbjo8x1w7.cloudfront.net	/locales/en-US/cluster.json	200	-	Mozilla/5.0%20(Macintosh;%20Intel%20Mac%20OS%20X%2010_15_7)%20AppleWebKit/537.36%20(KHTML,%20like%20Gecko)%20Chrome/110.0.0.0%20Safari/537.36	v=v1.3.0	-	Error	N1oemJ35KRzJUdU6lhbs7qFx50cK43zbclP5ujNw2Tvp61DQVxyWeQ==	d2wusnbjo8x1w7.cloudfront.net	https	53	0.578	-	TLSv1.3	TLS_AES_128_GCM_SHA256	Error	HTTP/2.0	-	-	12812	0.578	Error	-	-	-	-
        |2023-02-22	03:22:41	HKG62-C2	703	13.248.48.9	GET	d2wusnbjo8x1w7.cloudfront.net	/locales/en-US/ekslog.json	200	-	Mozilla/5.0%20(Macintosh;%20Intel%20Mac%20OS%20X%2010_15_7)%20AppleWebKit/537.36%20(KHTML,%20like%20Gecko)%20Chrome/110.0.0.0%20Safari/537.36	v=v1.3.0	-	Error	GLadXidtuMq3JYG7H7ERW8FpaxmAPruf2BHEnWoZgKPnbmg_-ESQJQ==	d2wusnbjo8x1w7.cloudfront.net	https	52	0.577	-	TLSv1.3	TLS_AES_128_GCM_SHA256	Error	HTTP/2.0	-	-	12812	0.577	Error	-	-	-	-
        |2023-02-22	03:22:41	HKG62-C2	704	13.248.48.9	GET	d2wusnbjo8x1w7.cloudfront.net	/locales/en-US/resource.json	200	-	Mozilla/5.0%20(Macintosh;%20Intel%20Mac%20OS%20X%2010_15_7)%20AppleWebKit/537.36%20(KHTML,%20like%20Gecko)%20Chrome/110.0.0.0%20Safari/537.36	v=v1.3.0	-	Error	1LXP9gNavZVGCROWZMcn_7kyU3hc0bubNrNCoWZWoSXZtGWRLde0yA==	d2wusnbjo8x1w7.cloudfront.net	https	53	0.585	-	TLSv1.3	TLS_AES_128_GCM_SHA256	Error	HTTP/2.0	-	-	12812	0.585	Error	-	-	-	-
        |2023-02-22	03:22:42	HKG62-C2	676	13.248.48.9	GET	d2wusnbjo8x1w7.cloudfront.net	/static/js/156.e12ab3ef.chunk.js	304	-	Mozilla/5.0%20(Macintosh;%20Intel%20Mac%20OS%20X%2010_15_7)%20AppleWebKit/537.36%20(KHTML,%20like%20Gecko)%20Chrome/110.0.0.0%20Safari/537.36	-	-	Miss	KsRbYPXREqKsTbiTadJECL7oBJu-QeMAQ8VtZp2M46FejSGFnZZokg==	d2wusnbjo8x1w7.cloudfront.net	https	100	0.586	-	TLSv1.3	TLS_AES_128_GCM_SHA256	Miss	HTTP/2.0	-	-	12812	0.586	Miss	-	-	-	-
        |2023-02-22	03:22:42	HKG62-C2	676	13.248.48.9	GET	d2wusnbjo8x1w7.cloudfront.net	/static/js/704.0fc9620b.chunk.js	304	-	Mozilla/5.0%20(Macintosh;%20Intel%20Mac%20OS%20X%2010_15_7)%20AppleWebKit/537.36%20(KHTML,%20like%20Gecko)%20Chrome/110.0.0.0%20Safari/537.36	-	-	Miss	S6Rm9LTX-Gh9sdBmmj89KKx36jkq8bSTzw6EgI4uWRjdwlEppfrQbA==	d2wusnbjo8x1w7.cloudfront.net	https	101	0.596	-	TLSv1.3	TLS_AES_128_GCM_SHA256	Miss	HTTP/2.0	-	-	12812	0.596	Miss	-	-	-	-
        |2023-02-22	03:22:42	HKG62-C2	675	13.248.48.9	GET	d2wusnbjo8x1w7.cloudfront.net	/static/js/592.57113085.chunk.js	304	-	Mozilla/5.0%20(Macintosh;%20Intel%20Mac%20OS%20X%2010_15_7)%20AppleWebKit/537.36%20(KHTML,%20like%20Gecko)%20Chrome/110.0.0.0%20Safari/537.36	-	-	Miss	ns8wu8ZtrFn5pPpDhVTFRVHdpQoaQ6P_iAfQeH0G8RpFs8fNAfxtJw==	d2wusnbjo8x1w7.cloudfront.net	https	100	0.644	-	TLSv1.3	TLS_AES_128_GCM_SHA256	Miss	HTTP/2.0	-	-	12812	0.644	Miss	-	-	-	-
        |2023-02-22	03:22:55	HKG62-C2	675	13.248.48.9	GET	d2wusnbjo8x1w7.cloudfront.net	/static/media/amazon_cloudtrail.26d9ae95b52f16f31bfbba95bfb0f69a.svg	304	-	Mozilla/5.0%20(Macintosh;%20Intel%20Mac%20OS%20X%2010_15_7)%20AppleWebKit/537.36%20(KHTML,%20like%20Gecko)%20Chrome/110.0.0.0%20Safari/537.36	-	-	Miss	67pVf-Byl4ASBYE4Af9F5Mj-PHj7yXGm_2laTGicFb3qlZ7E5Jf1WA==	d2wusnbjo8x1w7.cloudfront.net	https	125	0.586	-	TLSv1.3	TLS_AES_128_GCM_SHA256	Miss	HTTP/2.0	-	-	12812	0.586	Miss	-	-	-	-
        |2023-02-22	03:22:55	HKG62-C2	674	13.248.48.9	GET	d2wusnbjo8x1w7.cloudfront.net	/static/media/amazon_cloudfront.99760e5d4b4a5670c95d0ccb1df941b6.svg	304	-	Mozilla/5.0%20(Macintosh;%20Intel%20Mac%20OS%20X%2010_15_7)%20AppleWebKit/537.36%20(KHTML,%20like%20Gecko)%20Chrome/110.0.0.0%20Safari/537.36	-	-	Miss	wLuqos8-liLES101O2eGt7xKv1H9ojtNTOZQKkk8u90sZ68Q6kZ2mA==	d2wusnbjo8x1w7.cloudfront.net	https	101	0.582	-	TLSv1.3	TLS_AES_128_GCM_SHA256	Miss	HTTP/2.0	-	-	12812	0.582	Miss	-	-	-	-
        |2023-02-22	03:22:55	HKG62-C2	677	13.248.48.9	GET	d2wusnbjo8x1w7.cloudfront.net	/static/media/amazon_rds.360aca36ea40c7ea903b6b862f87c786.svg	304	-	Mozilla/5.0%20(Macintosh;%20Intel%20Mac%20OS%20X%2010_15_7)%20AppleWebKit/537.36%20(KHTML,%20like%20Gecko)%20Chrome/110.0.0.0%20Safari/537.36	-	-	Miss	EJCQrFBvytDkK3qAKcmO3PzY6IzKr59CEU39fHswI0flUv_F-16ybg==	d2wusnbjo8x1w7.cloudfront.net	https	121	0.586	-	TLSv1.3	TLS_AES_128_GCM_SHA256	Miss	HTTP/2.0	-	-	12812	0.586	Miss	-	-	-	-
        |2023-02-22	03:22:55	HKG62-C2	675	13.248.48.9	GET	d2wusnbjo8x1w7.cloudfront.net	/static/media/amazon_lambda.ec352ff412ff847df8989db268d08cf5.svg	304	-	Mozilla/5.0%20(Macintosh;%20Intel%20Mac%20OS%20X%2010_15_7)%20AppleWebKit/537.36%20(KHTML,%20like%20Gecko)%20Chrome/110.0.0.0%20Safari/537.36	-	-	Miss	7svNdqnaACxMHKDLi0IfU2dlgNs-pxsQ_9NxISB-09ovd9aSwGyzVA==	d2wusnbjo8x1w7.cloudfront.net	https	100	0.594	-	TLSv1.3	TLS_AES_128_GCM_SHA256	Miss	HTTP/2.0	-	-	12812	0.594	Miss	-	-	-	-
        |2023-02-22	03:22:55	HKG62-C2	675	13.248.48.9	GET	d2wusnbjo8x1w7.cloudfront.net	/static/media/amazon_s3.4c12b77f2ed5eba3d1753f7bceffa085.svg	304	-	Mozilla/5.0%20(Macintosh;%20Intel%20Mac%20OS%20X%2010_15_7)%20AppleWebKit/537.36%20(KHTML,%20like%20Gecko)%20Chrome/110.0.0.0%20Safari/537.36	-	-	Miss	7xm_GS5BG9-diZcNsUkRxjbDPhrbf8vssGSCFNuHnT4RWSJpsCn3_g==	d2wusnbjo8x1w7.cloudfront.net	https	96	0.777	-	TLSv1.3	TLS_AES_128_GCM_SHA256	Miss	HTTP/2.0	-	-	12812	0.777	Miss	-	-	-	-
        |2023-02-22	03:22:56	HKG62-C2	676	13.248.48.9	GET	d2wusnbjo8x1w7.cloudfront.net	/static/media/amazon_elb.f0b256fc7a7d0104df7de23bb37fa4e7.svg	304	-	Mozilla/5.0%20(Macintosh;%20Intel%20Mac%20OS%20X%2010_15_7)%20AppleWebKit/537.36%20(KHTML,%20like%20Gecko)%20Chrome/110.0.0.0%20Safari/537.36	-	-	Miss	mJtoIBCXJ_Q6LUitILccKDU3wq9BpIb1JfZQJwMVXLv3jiyA-ch-EA==	d2wusnbjo8x1w7.cloudfront.net	https	97	0.591	-	TLSv1.3	TLS_AES_128_GCM_SHA256	Miss	HTTP/2.0	-	-	12812	0.591	Miss	-	-	-	-
        |2023-02-22	03:22:56	HKG62-C2	677	13.248.48.9	GET	d2wusnbjo8x1w7.cloudfront.net	/static/media/amazon_config.3e3f8e67758d5bb8184803f79395f023.svg	304	-	Mozilla/5.0%20(Macintosh;%20Intel%20Mac%20OS%20X%2010_15_7)%20AppleWebKit/537.36%20(KHTML,%20like%20Gecko)%20Chrome/110.0.0.0%20Safari/537.36	-	-	Miss	bnZL88SZ53njHDf42zxY-VPweyZLa7whVCDaGLUjOC2308nCqS2RPQ==	d2wusnbjo8x1w7.cloudfront.net	https	100	0.592	-	TLSv1.3	TLS_AES_128_GCM_SHA256	Miss	HTTP/2.0	-	-	12812	0.592	Miss	-	-	-	-
        |2023-02-22	03:22:56	HKG62-C2	677	13.248.48.9	GET	d2wusnbjo8x1w7.cloudfront.net	/static/media/amazon_vpclogs.a6361309d9b003946aa1ffddbe09b159.svg	304	-	Mozilla/5.0%20(Macintosh;%20Intel%20Mac%20OS%20X%2010_15_7)%20AppleWebKit/537.36%20(KHTML,%20like%20Gecko)%20Chrome/110.0.0.0%20Safari/537.36	-	-	Miss	XpMKw9WCFEJvYYmx3n6qvBt0yHPLnRZt1yfz4G7OkbITF7jPmVguQg==	d2wusnbjo8x1w7.cloudfront.net	https	100	0.592	-	TLSv1.3	TLS_AES_128_GCM_SHA256	Miss	HTTP/2.0	-	-	12812	0.592	Miss	-	-	-	-
        |2023-02-22	03:22:56	HKG62-C2	675	13.248.48.9	GET	d2wusnbjo8x1w7.cloudfront.net	/static/media/amazon_waf.29acdfca0afd099f6731667747bfcbdd.svg	304	-	Mozilla/5.0%20(Macintosh;%20Intel%20Mac%20OS%20X%2010_15_7)%20AppleWebKit/537.36%20(KHTML,%20like%20Gecko)%20Chrome/110.0.0.0%20Safari/537.36	-	-	Miss	A2Fs75-2iJOohgpxAKuxYe7ozq2Z3lyYWwedEna8UXBHvBE-JrGNwQ==	d2wusnbjo8x1w7.cloudfront.net	https	98	0.594	-	TLSv1.3	TLS_AES_128_GCM_SHA256	Miss	HTTP/2.0	-	-	12812	0.594	Miss	-	-	-	-
        |2023-02-22	03:22:56	HKG62-C2	114697	13.248.48.9	GET	d2wusnbjo8x1w7.cloudfront.net	/static/media/s3LogArch.c4018f3dddce0f432077.png	200	-	Mozilla/5.0%20(Macintosh;%20Intel%20Mac%20OS%20X%2010_15_7)%20AppleWebKit/537.36%20(KHTML,%20like%20Gecko)%20Chrome/110.0.0.0%20Safari/537.36	-	-	Miss	o6zoQmYGTLRJhB6ezOguxrK1OF5grNTc_cOM94Pnie92K6Mwq2icsg==	d2wusnbjo8x1w7.cloudfront.net	https	58	0.928	-	TLSv1.3	TLS_AES_128_GCM_SHA256	Miss	HTTP/2.0	-	-	12812	0.659	Miss	image/png	113818	-	-
        |2023-02-22	03:22:59	HKG62-C2	140898	13.248.48.9	GET	d2wusnbjo8x1w7.cloudfront.net	/static/media/cloudFrontArch.0fcf57a6a2537f8709f0.png	200	-	Mozilla/5.0%20(Macintosh;%20Intel%20Mac%20OS%20X%2010_15_7)%20AppleWebKit/537.36%20(KHTML,%20like%20Gecko)%20Chrome/110.0.0.0%20Safari/537.36	-	-	Miss	vVbdBCQusqcbToWbjQSK7Q_gmehboF3kAk60Q10JHxf8dd5iD0QJWg==	d2wusnbjo8x1w7.cloudfront.net	https	62	0.987	-	TLSv1.3	TLS_AES_128_GCM_SHA256	Miss	HTTP/2.0	-	-	12812	0.718	Miss	image/png	140008	-	-
        |2023-02-22	03:23:12	HKG62-C2	161604	13.248.48.9	GET	d2wusnbjo8x1w7.cloudfront.net	/static/media/lambdaArch.783de1739ebe58abc2a9.png	200	-	Mozilla/5.0%20(Macintosh;%20Intel%20Mac%20OS%20X%2010_15_7)%20AppleWebKit/537.36%20(KHTML,%20like%20Gecko)%20Chrome/110.0.0.0%20Safari/537.36	-	-	Miss	5ai8KK3TCwN2IN-Df95LWkK7Hzvo_k7e-boJOxcLcczb2Xe8TzDy1g==	d2wusnbjo8x1w7.cloudfront.net	https	59	1.079	-	TLSv1.3	TLS_AES_128_GCM_SHA256	Miss	HTTP/2.0	-	-	12812	0.676	Miss	image/png	160670	-	-
        |2023-02-22	03:23:12	HKG62-C2	167457	13.248.48.9	GET	d2wusnbjo8x1w7.cloudfront.net	/static/media/cloudtrailArch.b76f6988d706c2824458.png	200	-	Mozilla/5.0%20(Macintosh;%20Intel%20Mac%20OS%20X%2010_15_7)%20AppleWebKit/537.36%20(KHTML,%20like%20Gecko)%20Chrome/110.0.0.0%20Safari/537.36	-	-	Miss	goCDY4q7yiC4T-I4Sv2D-sgPW3PVd1VNXqkVDkS4A-dHVEVd6iow3g==	d2wusnbjo8x1w7.cloudfront.net	https	62	1.079	-	TLSv1.3	TLS_AES_128_GCM_SHA256	Miss	HTTP/2.0	-	-	12812	0.673	Miss	image/png	166523	-	-
        |2023-02-22	03:23:14	HKG62-C2	162423	13.248.48.9	GET	d2wusnbjo8x1w7.cloudfront.net	/static/media/elbArch.dbcdcea16ace81a05c28.png	200	-	Mozilla/5.0%20(Macintosh;%20Intel%20Mac%20OS%20X%2010_15_7)%20AppleWebKit/537.36%20(KHTML,%20like%20Gecko)%20Chrome/110.0.0.0%20Safari/537.36	-	-	Miss	T2z6641ScR1RQwziiZT9s8zqrlo7YD3iaLg5WVowGzs1NkcTPdrnog==	d2wusnbjo8x1w7.cloudfront.net	https	56	1.050	-	TLSv1.3	TLS_AES_128_GCM_SHA256	Miss	HTTP/2.0	-	-	12812	0.648	Miss	image/png	161498	-	-
        |2023-02-22	03:23:15	HKG62-C2	161053	13.248.48.9	GET	d2wusnbjo8x1w7.cloudfront.net	/static/media/wafArch.9cdccd95c4eb308461a2.png	200	-	Mozilla/5.0%20(Macintosh;%20Intel%20Mac%20OS%20X%2010_15_7)%20AppleWebKit/537.36%20(KHTML,%20like%20Gecko)%20Chrome/110.0.0.0%20Safari/537.36	-	-	Miss	WxSdG2R6oqaVNJQkue2oCkI6nU3ZXKb04KZQDgb7ZFUkX-RmAv59XA==	d2wusnbjo8x1w7.cloudfront.net	https	57	1.074	-	TLSv1.3	TLS_AES_128_GCM_SHA256	Miss	HTTP/2.0	-	-	12812	0.669	Miss	image/png	160127	-	-
        |2023-02-22	03:24:22	HKG62-C2	150993	13.248.48.9	GET	d2wusnbjo8x1w7.cloudfront.net	/static/media/rdsArch.aa17197fc8ed28ace19f.png	200	-	Mozilla/5.0%20(Macintosh;%20Intel%20Mac%20OS%20X%2010_15_7)%20AppleWebKit/537.36%20(KHTML,%20like%20Gecko)%20Chrome/110.0.0.0%20Safari/537.36	-	-	Miss	yLiCzSmstFWGLeb9NjslBLkBOpN6stNwWakqN4wKZNHAm9VTFzE2zw==	d2wusnbjo8x1w7.cloudfront.net	https	56	1.030	-	TLSv1.3	TLS_AES_128_GCM_SHA256	Miss	HTTP/2.0	-	-	12812	0.626	Miss	image/png	150075	-	-
        |"""


    // Write CSV string to HDFS or local file system
    val path = new Path("./temp.csv")
    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    val outputStream = fs.create(path)
    IOUtils.copyBytes(new ByteArrayInputStream(csv.getBytes), outputStream, conf)

    val df = spark.read.option("header", false).option("delimiter", "\t").option("inferSchema", true).csv("./temp.csv")

    // Create table from DataFrame
//    df.write.mode("overwrite").saveAsTable("aws_cloudtrail")
//    val showDDL = spark.sql("SHOW CREATE TABLE aws_cloudtrail")
//    showDDL.show(false)
    df.write.insertInto("cloudfront")
    val simpleQuery = sql("select * from cloudfront")
    simpleQuery.collect()
    simpleQuery.show()
    
    val result = sql(s"""SELECT
                        |    to_timestamp(concat(log_date, ' ', log_time), 'yyyy-MM-dd HH:mm:ss') AS `@timestamp`,
                        |    c_ip AS `aws.cloudfront.c-ip`,
                        |    NULL AS `aws.cloudfront.geo_location`,
                        |    NULL AS `aws.cloudfront.geo_iso_code`,
                        |    NULL AS `aws.cloudfront.geo_country`,
                        |    NULL AS `aws.cloudfront.geo_city`,
                        |    NULL AS `aws.cloudfront.ua_browser`,
                        |    NULL AS `aws.cloudfront.ua_browser_version`,
                        |    NULL AS `aws.cloudfront.ua_os`,
                        |    NULL AS `aws.cloudfront.ua_os_version`,
                        |    NULL AS `aws.cloudfront.ua_device`,
                        |    NULL AS `aws.cloudfront.ua_category`,
                        |    c_port AS `aws.cloudfront.c-port`,
                        |    cs_cookie AS `aws.cloudfront.cs-cookie`,
                        |    cs_host AS `aws.cloudfront.cs-host`,
                        |    cs_referer AS `aws.cloudfront.cs-referer`,
                        |    cs_user_agent AS `aws.cloudfront.cs-user-agent`,
                        |    cs_bytes AS `aws.cloudfront.cs-bytes`,
                        |    cs_method AS `aws.cloudfront.cs-method`,
                        |    cs_protocol AS `aws.cloudfront.cs-protocol`,
                        |    cs_protocol_version AS `aws.cloudfront.cs-protocol-version`,
                        |    cs_uri_query AS `aws.cloudfront.cs-uri-query`,
                        |    cs_uri_stem AS `aws.cloudfront.cs-uri-stem`,
                        |    fle_encrypted_fields AS `aws.cloudfront.fle-encrypted-fields`,
                        |    fle_status AS `aws.cloudfront.fle-status`,
                        |    sc_bytes AS `aws.cloudfront.sc-bytes`,
                        |    sc_content_len AS `aws.cloudfront.sc-content-len`,
                        |    sc_content_type AS `aws.cloudfront.sc-content-type`,
                        |    sc_range_end AS `aws.cloudfront.sc-range-end`,
                        |    sc_range_start AS `aws.cloudfront.sc-range-start`,
                        |    sc_status AS `aws.cloudfront.sc-status`,
                        |    ssl_cipher AS `aws.cloudfront.ssl-cipher`,
                        |    ssl_protocol AS `aws.cloudfront.ssl-protocol`,
                        |    time_taken AS `aws.cloudfront.time-taken`,
                        |    time_to_first_byte AS `aws.cloudfront.time-to-first-byte`,
                        |    x_edge_detailed_result_type AS `aws.cloudfront.x-edge-detailed-result-type`,
                        |    x_edge_location AS `aws.cloudfront.x-edge-location`,
                        |    x_edge_request_id AS `aws.cloudfront.x-edge-request-id`,
                        |    x_edge_result_type AS `aws.cloudfront.x-edge-result-type`,
                        |    x_edge_response_result_type AS `aws.cloudfront.x-edge-response-result-type`,
                        |    x_forwarded_for AS `aws.cloudfront.x-forwarded-for`,
                        |    x_host_header AS `aws.cloudfront.x-host-header` 
                        |    FROM cloudfront""".stripMargin)
    result.show()
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
