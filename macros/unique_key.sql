{% macro create_unique_constraint(table_name, constraint_name, columns) %}
    {% set column_list = columns | join(', ') %}
    ALTER TABLE {{ table_name }}
    ADD CONSTRAINT {{ constraint_name }} UNIQUE ({{ column_list }});
{% endmacro %}
