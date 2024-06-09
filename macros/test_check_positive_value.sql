{% test test_check_positive_value(model, column_name) %}
SELECT
*
FROM
{{ model }}
WHERE
{{ column_name}} < 1

{% endtest %}