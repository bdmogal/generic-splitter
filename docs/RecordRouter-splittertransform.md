# Record Router

Description
-----------
This transform routes a record to an appropriate port based on the evaluation of a simple function on the value of one
of its fields. It is implemented as a Splitter Transform.

Use case
--------
Often times, you have data feeds coming in where the value of a field in the feed typically determines the processing
that must happen on that record.
E.g. Consider a supply chain field containing inventory information, which contains a field called *supplier_id*.
Your supply chain optimization pipeline must process records differently, based on their *supplier_id*. In this case,
you can use the Record Router to set up a pipeline that routes records to different processing branches based on their
*supplier_id*.

Properties
----------
**Route Specification Mode**: The mode in which you would like to provide the routing specification. The basic mode
allows you to specify multiple simple routing rules, where each rule operates on a single field in the input schema.
For basic, the Routing Field and Port Specification are required. The jexl mode allows you to specify complex
routing rules, which can operate on multiple input fields in a single rule. In the jexl mode, you can use JEXL
expressions to specify the routing configuration. Also specify the JEXL Port Configuration for the jexl mode.
Defaults to basic.

**Routing Field**: Specifies the field in the input schema on which the rules in the _Port Specification_ should be
applied, to determine the port where the record should be routed to.

**Port Specification**: Specifies the rules to determine the port where the record should be routed to. Rules are
applied on the value of the routing field. The port specification is expressed as a comma-separated list of rules,
where each rule has the format [port-name]:[function-name]([parameter-name]). [port-name] is the name of the port to
route the record to if the rule is satisfied. [function-name] can be one of equals, not_equals, contains, not_contains,
in, not_in. [parameter-name] is the parameter based on which the selected function evaluates the value of the routing
field.

**JEXL Port Specification**: Specifies a '#' separated list of ports, and the JEXL expression to route the record to
the port in the format [port-name]:[jexl-expression]. All the input fields are available as variables in the JEXL
expression. Additionally, utility methods from common Java classes such as Math, Guava Strings, Apache Commons Lang
StringUtils, Bytes and Arrays are also available for use in the port specification rules. To avoid conflicts with
delimiters, it is recommended to URL-encode the JEXL expressions.

**Default handling**: Determines the way to handle records whose value for the field to match on doesn't match an of
the rules defined in the port configuration. Defaulting records can either be skipped ("Skip"), sent to a specific port
("Send to default port"), or sent to the error port. Defaults to "Send to default port".

**Default Port**: Determines the port to which records that do not match any of the rules in the port specification
are routed. This is only used if default handling is set to "Send to default port". Defaults to 'Default'.

**Null Port**: Determines the port to which records with null values for the field to split on are sent. This is only
used if default handling is set to "Send to null port". Defaults to 'Null'.
