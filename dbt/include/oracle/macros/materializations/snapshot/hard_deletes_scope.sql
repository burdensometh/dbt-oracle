{#
 Copyright (c) 2026, Oracle and/or its affiliates.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
#}

{#
    Authoritative scope for hard-delete detection.

    When a source only contains a subset of the full dataset on each run
    (for example only a few tenants are pulled per day), records missing from
    the source must not be treated as deleted unless they belong to the subset
    this run explicitly claims to cover.

    Config:
      hard_deletes_scope_column  : column (e.g. tenant_id) whose values
                                   identify the authoritative subset
      hard_deletes_scope_values  : list of values present in this run's
                                   source. Only open snapshot rows whose
                                   scope column has one of these values take
                                   part in hard-delete comparison.

    Rows outside the declared scope are never invalidated / marked deleted,
    even if they are absent from the source. A source row carrying a scope
    value that was not declared makes the run fail before any change is made
    to the snapshot table.
#}

{% macro get_hard_deletes_scope() %}
    {{ return(adapter.dispatch('get_hard_deletes_scope', 'oracle')()) }}
{% endmacro %}

{% macro oracle__get_hard_deletes_scope() %}
    {% set scope_column = config.get('hard_deletes_scope_column') %}
    {% set scope_values = config.get('hard_deletes_scope_values') %}

    {% if scope_column is none and scope_values is none %}
        {{ return(none) }}
    {% endif %}

    {% if scope_column is none or scope_values is none %}
        {% do exceptions.raise_compiler_error(
            "Snapshot '" ~ model.name ~ "': 'hard_deletes_scope_column' and "
            ~ "'hard_deletes_scope_values' must be set together.") %}
    {% endif %}

    {% if scope_column is string and scope_column | trim | length == 0 %}
        {% do exceptions.raise_compiler_error(
            "Snapshot '" ~ model.name ~ "': 'hard_deletes_scope_column' must not be empty.") %}
    {% endif %}

    {% if scope_values is string or scope_values is not iterable %}
        {% do exceptions.raise_compiler_error(
            "Snapshot '" ~ model.name ~ "': 'hard_deletes_scope_values' must be a list, got: "
            ~ scope_values) %}
    {% endif %}

    {% if scope_values | length == 0 %}
        {% do exceptions.raise_compiler_error(
            "Snapshot '" ~ model.name ~ "': 'hard_deletes_scope_values' must not be empty. "
            ~ "Declare the scope values covered by this run, or remove the scope config.") %}
    {% endif %}

    {% for value in scope_values %}
        {% if value is mapping or (value is iterable and value is not string) %}
            {% do exceptions.raise_compiler_error(
                "Snapshot '" ~ model.name ~ "': every entry of 'hard_deletes_scope_values' "
                ~ "must be a scalar value, got: " ~ value) %}
        {% endif %}
    {% endfor %}

    {{ return({'column': scope_column, 'values': scope_values}) }}
{% endmacro %}


{# Render one declared scope value as an Oracle literal #}
{% macro hard_deletes_scope_literal(value) %}
    {%- if value is none -%}
        NULL
    {%- elif value is string -%}
        '{{ value | replace("'", "''") }}'
    {%- elif value is number -%}
        {{ value }}
    {%- elif value is true -%}
        1
    {%- elif value is false -%}
        0
    {%- else -%}
        '{{ value | string | replace("'", "''") }}'
    {%- endif -%}
{% endmacro %}


{#
    Predicate restricting open snapshot rows to the declared authoritative
    scope. Only used inside the hard-delete CTEs. SQL NULL never matches an
    IN-list, so it has to be handled with an explicit IS NULL branch.
#}
{% macro hard_deletes_scope_predicate(relation_alias, scope) %}
    {%- if scope is not none -%}
        {%- set scope_col = relation_alias ~ '.' ~ scope['column'] -%}
        {%- set non_null_values = scope['values'] | reject('none') | list -%}
        (
        {%- if none in scope['values'] %}
            {{ scope_col }} is null
            {%- if non_null_values | length > 0 %} or {% endif -%}
        {%- endif -%}
        {%- if non_null_values | length > 0 %}
            {{ scope_col }} in (
                {%- for value in non_null_values -%}
                    {{ hard_deletes_scope_literal(value) }}
                    {%- if not loop.last %}, {% endif -%}
                {%- endfor -%}
            )
        {%- endif -%}
        )
    {%- endif -%}
{% endmacro %}


{#
    Fail before the snapshot table is touched when the source contains rows
    whose scope value was not declared for this run (including NULL scope
    values, unless NULL was explicitly declared).
#}
{% macro check_hard_deletes_scope(source_sql, scope) %}
    {% if scope is not none %}
        {% set scope_col = scope['column'] %}
        {% set non_null_values = scope['values'] | reject('none') | list %}
        {% set declared_literals = [] %}
        {% for value in non_null_values %}
            {% do declared_literals.append(hard_deletes_scope_literal(value)) %}
        {% endfor %}

        {% set validation_sql %}
            with snapshot_query as (
                {{ source_sql }}
            )
            select distinct {{ scope_col }} as undeclared_scope_value
            from snapshot_query
            where
                {% if none not in scope['values'] %}
                {{ scope_col }} is null
                {%- if declared_literals | length > 0 %} or {% endif -%}
                {% endif %}
                {%- if declared_literals | length > 0 %}
                {{ scope_col }} not in (
                    {% for literal in declared_literals %}{{ literal }}{% if not loop.last %}, {% endif %}{% endfor %}
                )
                {%- elif none in scope['values'] %}
                1 = 0
                {% endif %}
            fetch first 20 rows only
        {% endset %}

        {% set undeclared = run_query(validation_sql) %}
        {% if undeclared | length > 0 %}
            {% set found = [] %}
            {% for row in undeclared %}
                {% if row[0] is none %}
                    {% do found.append('NULL') %}
                {% else %}
                    {% do found.append(row[0] | string) %}
                {% endif %}
            {% endfor %}
            {% do exceptions.raise_compiler_error(
                "Snapshot '" ~ model.name ~ "': source contains rows outside the declared "
                ~ "hard-delete authoritative scope (column '" ~ scope['column'] ~ "'). "
                ~ "Undeclared values found: " ~ (found | join(', ')) ~ ". "
                ~ "Declared scope values: " ~ (scope['values'] | join(', ')) ~ ". "
                ~ "Add the missing values to 'hard_deletes_scope_values' or filter the source; "
                ~ "no changes were made to the snapshot table.") %}
        {% endif %}
    {% endif %}
{% endmacro %}
