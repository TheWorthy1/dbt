{% macro create_primary_key_constraint(table_name, constraint_name, columns) %}
    {% set column_list = columns | join(', ') %}
    ALTER TABLE {{ table_name }}
    ADD CONSTRAINT {{ constraint_name }} PRIMARY KEY ({{ column_list }});
{% endmacro %}