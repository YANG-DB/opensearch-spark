## PPL JSON Functions

### `JSON`

**Description**

`json(value)` Evaluates whether a string can be parsed as JSON format. Returns the string value if valid, null otherwise.

**Argument type:** STRING

**Return type:** STRING/NULL

A STRING expression of a valid JSON object format.

Example:

    os> source=people | eval `valid_json()` = json('[1,2,3,{"f1":1,"f2":[5,6]},4]') | fields valid_json
    fetched rows / total rows = 1/1
    +---------------------------------+
    | valid_json                      |
    +---------------------------------+
    | [1,2,3,{"f1":1,"f2":[5,6]},4]   |
    +---------------------------------+

    os> source=people | eval `invalid_json()` = json('{"invalid": "json"') | fields invalid_json
    fetched rows / total rows = 1/1
    +----------------+
    | invalid_json   |
    +----------------+
    | null           |
    +----------------+


### `JSON_OBJECT`

**Description**

`json_object(<key>, <value>[, <key>, <value>]...)` returns a JSON object from members of key-value pairs.

**Argument type:**
- A \<key\> must be STRING.
- A \<value\> can be any data types.

**Return type:** JSON_OBJECT (Spark StructType)

A StructType expression of a valid JSON object.

Example:

    os> source=people | eval result = json_object('key', 123.45) | fields result
    fetched rows / total rows = 1/1
    +------------------+
    | result           |
    +------------------+
    | {"key":123.45}   |
    +------------------+

    os> source=people | eval result = json_object('outer', json_object('inner', 123.45)) | fields result
    fetched rows / total rows = 1/1
    +------------------------------+
    | result                       |
    +------------------------------+
    | {"outer":{"inner":123.45}}   |
    +------------------------------+


### `JSON_ARRAY`

**Description**

`json_array(<value>...)` Creates a JSON ARRAY using a list of values.

**Argument type:**
- A \<value\> can be any kind of value such as string, number, or boolean.

**Return type:** ARRAY (Spark ArrayType)

An array of any supported data type for a valid JSON array.

Example:

    os> source=people | eval `json_array` = json_array(1, 2, 0, -1, 1.1, -0.11)
    fetched rows / total rows = 1/1
    +------------------------------+
    | json_array                   |
    +------------------------------+
    | [1.0,2.0,0.0,-1.0,1.1,-0.11] |
    +------------------------------+

    os> source=people | eval `json_array_object` = json_object("array", json_array(1, 2, 0, -1, 1.1, -0.11))
    fetched rows / total rows = 1/1
    +----------------------------------------+
    | json_array_object                      |
    +----------------------------------------+
    | {"array":[1.0,2.0,0.0,-1.0,1.1,-0.11]} |
    +----------------------------------------+

**Limitation**

The list of parameters of `json_array` should all be the same type.
`json_array('this', 'is', 1.1, -0.11, true, false)` throws exception.

### `TO_JSON_STRING`

**Description**

`to_json_string(jsonObject)` Returns a JSON string with a given json object value.

**Argument type:** JSON_OBJECT (Spark StructType/ArrayType)

**Return type:** STRING

Example:

    os> source=people | eval `json_string` = to_json_string(json_array(1, 2, 0, -1, 1.1, -0.11)) | fields json_string
    fetched rows / total rows = 1/1
    +--------------------------------+
    | json_string                    |
    +--------------------------------+
    | [1.0,2.0,0.0,-1.0,1.1,-0.11]   |
    +--------------------------------+

    os> source=people | eval `json_string` = to_json_string(json_object('key', 123.45)) | fields json_string
    fetched rows / total rows = 1/1
    +-----------------+
    | json_string     |
    +-----------------+
    | {'key', 123.45} |
    +-----------------+


### `JSON_ARRAY_LENGTH`

**Description**

`json_array_length(jsonArrayString)` Returns the number of elements in the outermost JSON array string.

**Argument type:** STRING

A STRING expression of a valid JSON array format.

**Return type:** INTEGER

`NULL` is returned in case of any other valid JSON string, `NULL` or an invalid JSON.

Example:

    os> source=people | eval `lenght1` = json_array_length('[1,2,3,4]'), `lenght2` = json_array_length('[1,2,3,{"f1":1,"f2":[5,6]},4]'), `not_array` = json_array_length('{"key": 1}')
    fetched rows / total rows = 1/1
    +-----------+-----------+-------------+
    | lenght1   | lenght2   | not_array   |
    +-----------+-----------+-------------+
    | 4         | 5         | null        |
    +-----------+-----------+-------------+


### `JSON_EXTRACT`

**Description**

`json_extract(jsonStr, path)` Extracts json object from a json string based on json path specified. Return null if the input json string is invalid.

**Argument type:** STRING, STRING

**Return type:** STRING

A STRING expression of a valid JSON object format.

`NULL` is returned in case of an invalid JSON.

Example:

    os> source=people | eval `json_extract('{"a":"b"}', '$.a')` = json_extract('{"a":"b"}', '$a')
    fetched rows / total rows = 1/1
    +----------------------------------+
    | json_extract('{"a":"b"}', 'a')   |
    +----------------------------------+
    | b                                |
    +----------------------------------+

    os> source=people | eval `json_extract('{"a":[{"b":1},{"b":2}]}', '$.a[1].b')` = json_extract('{"a":[{"b":1},{"b":2}]}', '$.a[1].b')
    fetched rows / total rows = 1/1
    +-----------------------------------------------------------+
    | json_extract('{"a":[{"b":1.0},{"b":2.0}]}', '$.a[1].b')   |
    +-----------------------------------------------------------+
    | 2.0                                                       |
    +-----------------------------------------------------------+

    os> source=people | eval `json_extract('{"a":[{"b":1},{"b":2}]}', '$.a[*].b')` = json_extract('{"a":[{"b":1},{"b":2}]}', '$.a[*].b')
    fetched rows / total rows = 1/1
    +-----------------------------------------------------------+
    | json_extract('{"a":[{"b":1.0},{"b":2.0}]}', '$.a[*].b')   |
    +-----------------------------------------------------------+
    | [1.0,2.0]                                                 |
    +-----------------------------------------------------------+

    os> source=people | eval `invalid_json` = json_extract('{"invalid": "json"')
    fetched rows / total rows = 1/1
    +----------------+
    | invalid_json   |
    +----------------+
    | null           |
    +----------------+


### `JSON_DELETE`

**Description**

`json_delete(json, [keys list])` Deletes json elements from a json object based on json specific keys. Return the updated object after keys deletion .

**Argument type:** JSON, List<STRING>

**Return type:** JSON

A JSON object format.

Example:

    os> source=people | eval deleted = json_delete({"a":"valueA", "b":"valueB"}, array("a")) 
    fetched rows / total rows = 1/1
    +----------------------------------+
    | deleted                          |
    +----------------------------------+
    | {"a": "valueA" }                 |
    +----------------------------------+

    os> source=people | eval deleted = json_delete({"a":[{"b":1},{"b":2},{"c":3}]}, array("a.b"))
    fetched rows / total rows = 1/1
    +-----------------------------------------------------------+
    | deleted                                                   |
    +-----------------------------------------------------------+
    | {"a":[{"c":3}] }                                          |
    +-----------------------------------------------------------+

    os> source=people | eval `no_action` =  json_delete({"a":[{"b":1},{"b":2},{"c":3}]}, array("b.b"))
    fetched rows / total rows = 1/1
    +-----------------------------------+
    | no_action                         |
    +-----------------------------------+
    | {"a":[{"b":1},{"b":2},{"c":3}]}   |
    +-----------------------------------+

### `JSON_APPEND`

**Description**

`json_append(json, [path_key, list of values to add ])`  appends values to end of an array within the json elements. Return the updated json object after appending .

**Argument type:** JSON, List<STRING>

**Return type:** JSON

A JSON object format.

**Note**
Append adds the value to the end of the existing array with the following cases:
  - path is an object value - append is ignored and the value is returned
  - path is an existing array not empty - the value are added to the array's tail
  - path not found - the value are added to the root of the json tree
  - path is an existing array is empty - create a new array with the given value

Example:

    os> source=people | eval append = json_append(`{"a":["valueA", "valueB"]}`, array('a', 'valueC', 'valueD')) 
    fetched rows / total rows = 1/1
    +-------------------------------------------------+
    | append                                          |
    +-------------------------------------------------+
    | {"a":["valueA", "valueB", "valueC", "valueD"]}  |
    +-------------------------------------------------+

    os> source=people | eval append = json_append(`{"a":[]}`, array('a', 'valueC')) 
    fetched rows / total rows = 1/1
    +-----------------------------------------------+
    | append                                        |
    +-----------------------------------------------+
    | {"a":["valueC"]}                              |
    +-----------------------------------------------+

    os> source=people | eval append = json_append(`{"root":{ "a":["valueA", "valueB"]}}`, array('root', 'valueC') 
    fetched rows / total rows = 1/1
    +-----------------------------------------------+
    | append                                        |
    +-----------------------------------------------+
    |{"root": {"a":["valueA", "valueB"]}}           |
    +-----------------------------------------------+



### `JSON_EXTEND`

**Description**

`json_extend(json, [path_value_pairs list])` extend appends multiple (array) values to an existing array json elements. Return the updated object after extending.

**Argument type:** JSON, List<[(STRING, List<STRING>)]>

**Return type:** JSON

A JSON object format.

**Note**
Extend arrays as individual values separates the `json_extend` functionality from the `json_append` - which is a similar function that appends the `<value>` as a single element.

Example:

    os> source=people | eval extend = json_extend(`{"a":["valueA", "valueB"]}`,['a', ["valueC","valueD"]]) 
    fetched rows / total rows = 1/1
    +-------------------------------------------------+
    | extend                                          |
    +-------------------------------------------------+
    | {"a":["valueA", "valueB", "valueC", "valueD"]}  |
    +-------------------------------------------------+

    os> source=people | eval extend = json_extend(`{"a":["valueA", "valueB"]}`,['a',[{"b":["valueC","valueD"]}]]) 
    fetched rows / total rows = 1/1
    +-------------------------------------------------------------+
    | extend                                                      |
    +-------------------------------------------------------------+
    | {"a":["valueA", "valueB", {"b":"valueC"}, {"b":"valueD"}]}  |
    +-------------------------------------------------------------+

### `JSON_KEYS`

**Description**

`json_keys(jsonStr)` Returns all the keys of the outermost JSON object as an array.

**Argument type:** STRING

A STRING expression of a valid JSON object format.

**Return type:** ARRAY[STRING]

`NULL` is returned in case of any other valid JSON string, or an empty string, or an invalid JSON.

Example:

    os> source=people | eval `keys` = json_keys('{"f1":"abc","f2":{"f3":"a","f4":"b"}}')
    fetched rows / total rows = 1/1
    +------------+
    | keus       |
    +------------+
    | [f1, f2]   |
    +------------+

    os> source=people | eval `keys` = json_keys('[1,2,3,{"f1":1,"f2":[5,6]},4]')
    fetched rows / total rows = 1/1
    +--------+
    | keys   |
    +--------+
    | null   |
    +--------+

### `JSON_VALID`

**Description**

`json_valid(jsonStr)` Evaluates whether a JSON string uses valid JSON syntax and returns TRUE or FALSE.

**Argument type:** STRING

**Return type:** BOOLEAN

Example:

    os> source=people | eval `valid_json` = json_valid('[1,2,3,4]'), `invalid_json` = json_valid('{"invalid": "json"') | feilds `valid_json`, `invalid_json`
    fetched rows / total rows = 1/1
    +--------------+----------------+
    | valid_json   | invalid_json   |
    +--------------+----------------+
    | True         | False          |
    +--------------+----------------+

    os> source=accounts | where json_valid('[1,2,3,4]') and isnull(email) | fields account_number, email
    fetched rows / total rows = 1/1
    +------------------+---------+
    | account_number   | email   |
    |------------------+---------|
    | 13               | null    |
    +------------------+---------+

### `FORALL`

**Description**

`forall(json_array, lambda)` Evaluates whether a lambda predicate holds for all elements in the json_array.

**Argument type:** ARRAY, LAMBDA

**Return type:** BOOLEAN

Returns `TRUE` if all elements in the array satisfy the lambda predicate, otherwise `FALSE`.

Example:

    os> source=people | eval array = json_array(1, -1, 2), result = forall(array, x -> x > 0) | fields result
    fetched rows / total rows = 1/1
    +-----------+
    | result    |
    +-----------+
    | false     |
    +-----------+

    os> source=people | eval array = json_array(1, 3, 2), result = forall(array, x -> x > 0) | fields result
    fetched rows / total rows = 1/1
    +-----------+
    | result    |
    +-----------+
    | true      |
    +-----------+

**Note:** The lambda expression can access the nested fields of the array elements. This applies to all lambda functions introduced in this document.

Consider constructing the following array:

    array = [
        {"a":1, "b":1},
        {"a":-1, "b":2}
    ]

and perform lambda functions against the nested fields `a` or `b`. See the examples:

    os> source=people | eval array = json_array(json_object("a", 1, "b", 1), json_object("a" , -1, "b", 2)), result = forall(array, x -> x.a > 0) | fields result
    fetched rows / total rows = 1/1
    +-----------+
    | result    |
    +-----------+
    | false     |
    +-----------+

    os> source=people | eval array = json_array(json_object("a", 1, "b", 1), json_object("a" , -1, "b", 2)), result = forall(array, x -> x.b > 0) | fields result
    fetched rows / total rows = 1/1
    +-----------+
    | result    |
    +-----------+
    | true      |
    +-----------+

### `EXISTS`

**Description**

`exists(json_array, lambda)` Evaluates whether a lambda predicate holds for one or more elements in the json_array.

**Argument type:** ARRAY, LAMBDA

**Return type:** BOOLEAN

Returns `TRUE` if at least one element in the array satisfies the lambda predicate, otherwise `FALSE`.

Example:

    os> source=people | eval array = json_array(1, -1, 2), result = exists(array, x -> x > 0) | fields result
    fetched rows / total rows = 1/1
    +-----------+
    | result    |
    +-----------+
    | true      |
    +-----------+

    os> source=people | eval array = json_array(-1, -3, -2), result = exists(array, x -> x > 0) | fields result
    fetched rows / total rows = 1/1
    +-----------+
    | result    |
    +-----------+
    | false     |
    +-----------+


### `FILTER`

**Description**

`filter(json_array, lambda)`  Filters the input json_array using the given lambda function.

**Argument type:** ARRAY, LAMBDA

**Return type:** ARRAY

An ARRAY that contains all elements in the input json_array that satisfy the lambda predicate.

Example:

    os> source=people | eval array = json_array(1, -1, 2), result = filter(array, x -> x > 0) | fields result
    fetched rows / total rows = 1/1
    +-----------+
    | result    |
    +-----------+
    | [1, 2]    |
    +-----------+

    os> source=people | eval array = json_array(-1, -3, -2), result = filter(array, x -> x > 0) | fields result
    fetched rows / total rows = 1/1
    +-----------+
    | result    |
    +-----------+
    | []        |
    +-----------+

### `TRANSFORM`

**Description**

`transform(json_array, lambda)` Transform elements in a json_array using the lambda transform function. The second argument implies the index of the element if using binary lambda function. This is similar to a `map` in functional programming.

**Argument type:** ARRAY, LAMBDA

**Return type:** ARRAY

An ARRAY that contains the result of applying the lambda transform function to each element in the input array.

Example:

    os> source=people | eval array = json_array(1, 2, 3), result = transform(array, x -> x + 1) | fields result
    fetched rows / total rows = 1/1
    +--------------+
    | result       |
    +--------------+
    | [2, 3, 4]    |
    +--------------+

    os> source=people | eval array = json_array(1, 2, 3), result = transform(array, (x, i) -> x + i) | fields result
    fetched rows / total rows = 1/1
    +--------------+
    | result       |
    +--------------+
    | [1, 3, 5]    |
    +--------------+

### `REDUCE`

**Description**

`reduce(json_array, start, merge_lambda, finish_lambda)` Applies a binary merge lambda function to a start value and all elements in the json_array, and reduces this to a single state. The final state is converted into the final result by applying a finish lambda function.

**Argument type:** ARRAY, ANY, LAMBDA, LAMBDA

**Return type:** ANY

The final result of applying the lambda functions to the start value and the input json_array.

Example:

    os> source=people | eval array = json_array(1, 2, 3), result = reduce(array, 0, (acc, x) -> acc + x) | fields result
    fetched rows / total rows = 1/1
    +-----------+
    | result    |
    +-----------+
    | 6         |
    +-----------+

    os> source=people | eval array = json_array(1, 2, 3), result = reduce(array, 10, (acc, x) -> acc + x) | fields result
    fetched rows / total rows = 1/1
    +-----------+
    | result    |
    +-----------+
    | 16        |
    +-----------+

    os> source=people | eval array = json_array(1, 2, 3), result = reduce(array, 0, (acc, x) -> acc + x, acc -> acc * 10) | fields result
    fetched rows / total rows = 1/1
    +-----------+
    | result    |
    +-----------+
    | 60        |
    +-----------+
