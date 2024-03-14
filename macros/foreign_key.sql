{% macro create_foreign_key_constraint(table_name, constraint_name, columns, referenced_table, referenced_columns) %}
    {% set column_list = columns | join(', ') %}
    {% set referenced_column_list = referenced_columns | join(', ') %}
    ALTER TABLE {{ table_name }}
    ADD CONSTRAINT {{ constraint_name }}
    FOREIGN KEY ({{ column_list }})
    REFERENCES {{ referenced_table }} ({{ referenced_column_list }});
{% endmacro %}