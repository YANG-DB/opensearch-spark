# Testing locally With Spark

This document is intended to review the local docker-compose based environment in-which the Flint/PPL - spark plugins
can be testes and explored.

## Overview

The following components are part of this testing environment

### Livy

Apache Livy is a service that enables easy interaction with a Spark cluster over a REST interface. It enables easy
submission of Spark jobs or snippets of Spark code, synchronous or asynchronous result retrieval, as well as Spark
Context management, all via a simple REST interface or an RPC client library.
Live provides a comprehensive [REST API](https://livy.apache.org/docs/latest/rest-api.html) to interact with spark
cluster in a simplified way.

## Test Tutorial

First we need to create a livy session

```
curl --location --request POST 'http://localhost:8998/sessions' \
--header 'Content-Type: application/json' \
--data-raw '{
    "kind": "sql",
    "proxyUser": "a_user"
}'
```

This call will respond with a session Id in the following manner:

```json5
{
  "id": 0,
  "name": null,
  "appId": null,
  "owner": null,
  "proxyUser": null,
  "state": "starting",
  "kind": "sql",
  "appInfo": {
    "driverLogUrl": null,
    "sparkUiUrl": null
  },
  "log": [
    "stdout: ",
    "\nstderr: "
  ]
}
```

Once a session is created, we can submit a SQL query statement the following way:

```
curl --location --request POST 'http://localhost:8998/sessions/0/statements' \
--header 'Content-Type: application/json' \
--data-raw '{
    "code": "CREATE TABLE test_table (id INT, name STRING) USING parquet"
}'
```

This call responds with the next ack

```json5
{
  "id": 0,
  "code": "spark.sql(\"CREATE TABLE test_table (id INT, name STRING)\")",
  "state": "waiting",
  "output": null,
  "progress": 0.0,
  "started": 0,
  "completed": 0
}
```

Next we can Insert some data into that table:

```
curl --location --request POST 'http://localhost:8998/sessions/0/statements' \
--header 'Content-Type: application/json' \
--data-raw '{
    "code": "INSERT INTO test_table VALUES (1, \"John\"), (2, \"Doe\")"
}'
```

Now lets query the table using SQL:

```
curl --location --request POST 'http://localhost:8998/sessions/0/statements' \
--header 'Content-Type: application/json' \
--data-raw '{
    "code": "SELECT * FROM test_table"
}'
```

We can now see the Livy session created with the execution running:

![Livy UI session Image]()

To get the response of this statement use the next API:
`curl --location --request GET http://localhost:8998/sessions/0/statements/2 `

This would respond with the next results

```json5
{
  "id": 2,
  "code": "SELECT * FROM test_table",
  "state": "available",
  "output": {
    "status": "ok",
    "execution_count": 2,
    "data": {
      "application/json": {
        "schema": {
          "type": "struct",
          "fields": [
            {
              "name": "id",
              "type": "integer",
              "nullable": true,
              "metadata": {}
            },
            {
              "name": "name",
              "type": "string",
              "nullable": true,
              "metadata": {}
            }
          ]
        },
        "data": []
      }
    }
  },
  "progress": 1.0,
  "started": 1696467890662,
  "completed": 1696467890978
}
```