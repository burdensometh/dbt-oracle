{#
 Copyright (c) 2022, Oracle and/or its affiliates.
 Copyright (c) 2020, Vitor Avancini

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
{% macro oracle_incremental_upsert_backup(tmp_relation, target_relation, unique_key=none, statement_name="main") %}
    {%- set dest_columns = adapter.get_columns_in_relation(target_relation) -%}
    {%- set dest_cols_csv = dest_columns | map(attribute='name') | join(', ') -%}

    {%- if unique_key is not none -%}
    delete
    from {{ target_relation }}
    where ({{ unique_key }}) in (
        select ({{ unique_key }})
        from {{ tmp_relation }}
    );
    {%- endif %}

    insert into {{ target_relation }} ({{ dest_cols_csv }})
    (
       select {{ dest_cols_csv }}
       from {{ tmp_relation }}
    )
{%- endmacro %}

{% macro oracle_check_and_quote_unique_key_for_incremental_merge(unique_key, incremental_predicates=none) %}
    {%- set quote = "\"" -%}
    {%- set unique_key_list = [] -%}
    {%- set unique_key_merge_predicates = [] if incremental_predicates is none else [] + incremental_predicates -%}
    {% if unique_key is sequence and unique_key is not mapping and unique_key is not string %}
          {% for key in unique_key | unique %}
                {% if adapter.should_identifier_be_quoted(key, model.columns) == true %}
                    {% do unique_key_list.append(quote ~ key ~ quote) %}
                {% else %}
                    {% do unique_key_list.append(key.upper()) %}
                {% endif %}
          {% endfor %}
    {% else %}
        {% if adapter.should_identifier_be_quoted(unique_key, model.columns) == true %}
            {% do unique_key_list.append(quote ~ unique_key ~ quote) %}
        {% else %}
            {% do unique_key_list.append(unique_key.upper()) %}
        {% endif %}
    {% endif %}
    {% for key in unique_key_list %}
        {% set this_key_match %}
            DBT_INTERNAL_SOURCE.{{ key }} = DBT_INTERNAL_DEST.{{ key }}
        {% endset %}
        {% do unique_key_merge_predicates.append(this_key_match) %}
    {% endfor %}
    {%- set unique_key_result = {'unique_key_list': unique_key_list, 'unique_key_merge_predicates': unique_key_merge_predicates} -%}
    {{ return(unique_key_result)}}
{% endmacro %}


{% macro oracle__get_merge_update_columns(merge_update_columns, merge_exclude_columns, dest_columns) %}
  {%- set default_cols = dest_columns | map(attribute='name') | list -%}

  {%- if merge_update_columns and merge_exclude_columns -%}
    {{ exceptions.raise_compiler_error(
        'Model cannot specify merge_update_columns and merge_exclude_columns. Please update model to use only one config'
    )}}
  {%- elif merge_update_columns -%}
    {%- set update_columns = merge_update_columns -%}
  {%- elif merge_exclude_columns -%}
    {%- set update_columns = [] -%}
    {%- for column in dest_columns -%}
      {% if column.column | lower not in merge_exclude_columns | map("lower") | list %}
        {%- do update_columns.append(column.name) -%}
      {% endif %}
    {%- endfor -%}
  {%- else -%}
    {%- set update_columns = default_cols -%}
  {%- endif -%}

   {%- set quoted_update_columns = [] -%}
   {% for col in update_columns %}
        {% do quoted_update_columns.append(adapter.check_and_quote_identifier(col, model.columns)) %}
   {% endfor %}
   {{ return(quoted_update_columns)}}
{% endmacro %}


{% macro oracle__get_incremental_append_sql(args_dict) %}
    {%- set parallel = config.get('parallel', none) -%}
    {%- set insert_mode = config.get('insert_mode', none) -%}
    {%- set insert_hint = generate_insert_hint(parallel, insert_mode) -%}
    {%- set dest_columns = args_dict["dest_columns"] -%}
    {%- set temp_relation = args_dict["temp_relation"] -%}
    {%- set target_relation = args_dict["target_relation"] -%}
    {%- set dest_column_names = dest_columns | map(attribute='name') | list -%}
    {%- set dest_cols_csv = get_quoted_column_csv(model, dest_column_names)  -%}
    INSERT {{ insert_hint }}
    INTO {{ target_relation }} ({{ dest_cols_csv }})
    (
       SELECT {{ dest_cols_csv }}
       FROM {{ temp_relation }}
    )
{% endmacro %}

{% macro oracle__get_incremental_merge_sql(args_dict) %}
    {%- set parallel = config.get('parallel', none) -%}
    {%- set insert_mode = config.get('insert_mode', none) -%}
    {%- set insert_hint = generate_insert_hint(parallel, insert_mode) -%}
    {%- set dest_columns = args_dict["dest_columns"] -%}
    {%- set temp_relation = args_dict["temp_relation"] -%}
    {%- set target_relation = args_dict["target_relation"] -%}
    {%- set unique_key = args_dict["unique_key"] -%}
    {%- set dest_column_names = dest_columns | map(attribute='name') | list -%}
    {%- set dest_cols_csv = get_quoted_column_csv(model, dest_column_names)  -%}
    {%- set merge_update_columns = config.get('merge_update_columns') -%}
    {%- set merge_exclude_columns = config.get('merge_exclude_columns') -%}
    {%- set incremental_predicates = args_dict["incremental_predicates"] -%}
    {%- set update_columns = get_merge_update_columns(merge_update_columns, merge_exclude_columns, dest_columns) -%}
    {%- if unique_key -%}
        {%- set unique_key_result = oracle_check_and_quote_unique_key_for_incremental_merge(unique_key, incremental_predicates) -%}
        {%- set unique_key_list = unique_key_result['unique_key_list'] -%}
        {%- set unique_key_merge_predicates = unique_key_result['unique_key_merge_predicates'] -%}
        merge {% if parallel %} /*+parallel({{ parallel }})*/ {% endif %} into {{ target_relation }} DBT_INTERNAL_DEST
          using {{ temp_relation }} DBT_INTERNAL_SOURCE
          on ({{ unique_key_merge_predicates | join(' AND ') }})
        when matched then
          update set
          {% for col in update_columns if (col.upper() not in unique_key_list and col not in unique_key_list) -%}
            DBT_INTERNAL_DEST.{{ col }} = DBT_INTERNAL_SOURCE.{{ col }}{% if not loop.last %}, {% endif %}
          {% endfor -%}
        when not matched then
          insert({{ dest_cols_csv }})
          values(
            {% for col in dest_columns -%}
              DBT_INTERNAL_SOURCE.{{ adapter.check_and_quote_identifier(col.name, model.columns) }}{% if not loop.last %}, {% endif %}
            {% endfor -%}
          )
    {%- else -%}
    insert {{ insert_hint }}
    into  {{ target_relation }} ({{ dest_cols_csv }})
    (
       select {{ dest_cols_csv }}
       from {{ temp_relation }}
    )
    {%- endif -%}
{% endmacro %}

{% macro oracle__get_incremental_default_sql(arg_dict) %}
  {% do return(get_incremental_merge_sql(arg_dict)) %}
{% endmacro %}

{% macro oracle__wrap_incremental_sql_with_tmp_cleanup(build_sql, tmp_relation) %}
    {%- set incremental_sql = build_sql | trim -%}
    BEGIN
      {{ incremental_sql }}{% if not incremental_sql.endswith(';') %};{% endif %}
    EXCEPTION
      WHEN OTHERS THEN
        BEGIN
          EXECUTE IMMEDIATE 'TRUNCATE TABLE {{ tmp_relation }}';
        EXCEPTION
          WHEN OTHERS THEN
            NULL;
        END;
        BEGIN
          EXECUTE IMMEDIATE 'DROP TABLE {{ tmp_relation }}';
        EXCEPTION
          WHEN OTHERS THEN
            NULL;
        END;
        RAISE;
    END;
{% endmacro %}

{% macro oracle__microbatch_timestamp_literal(timestamp_value) %}
  {% set timestamp_text = timestamp_value.strftime('%Y-%m-%d %H:%M:%S.%f') %}
  {% do return("TO_TIMESTAMP('" ~ timestamp_text ~ "', 'YYYY-MM-DD HH24:MI:SS.FF')") %}
{% endmacro %}

{% macro oracle__get_incremental_microbatch_sql(args_dict) %}
    {%- set parallel = config.get('parallel', none) -%}
    {%- set insert_mode = config.get('insert_mode', none) -%}
    {%- set insert_hint = generate_insert_hint(parallel, insert_mode) -%}
    {%- set dest_columns = args_dict["dest_columns"] -%}
    {%- set temp_relation = args_dict["temp_relation"] -%}
    {%- set target_relation = args_dict["target_relation"] -%}
    {%- set dest_column_names = dest_columns | map(attribute='name') | list -%}
    {%- set dest_cols_csv = get_quoted_column_csv(model, dest_column_names)  -%}
    {%- set event_time = adapter.check_and_quote_identifier(model.config.event_time, model.columns) -%}
    {%- set microbatch_predicates = [] -%}

    {% if model.batch and model.batch.event_time_start -%}
        {% do microbatch_predicates.append("DBT_INTERNAL_DEST." ~ event_time ~ " >= " ~ oracle__microbatch_timestamp_literal(model.batch.event_time_start)) %}
    {% endif %}
    {% if model.batch and model.batch.event_time_end -%}
        {% do microbatch_predicates.append("DBT_INTERNAL_DEST." ~ event_time ~ " < " ~ oracle__microbatch_timestamp_literal(model.batch.event_time_end)) %}
    {% endif %}

    BEGIN
    DELETE FROM {{ target_relation }} DBT_INTERNAL_DEST
      WHERE {% for predicate in microbatch_predicates -%}
        {{ "AND " if not loop.first }}{{ predicate }}
      {%- endfor %};
    INSERT {{ insert_hint }}
      INTO {{ target_relation }} ({{ dest_cols_csv }})
      (
        SELECT {{ dest_cols_csv }}
        FROM {{ temp_relation }}
      );
    END;
{% endmacro %}


{% macro oracle__get_delete_sql_for_delete_insert_strategy(target, source, unique_key, incremental_predicates) %}
    {%- if unique_key -%}
        {%- set unique_key_result = oracle_check_and_quote_unique_key_for_incremental_merge(unique_key, incremental_predicates) -%}
        {%- set unique_key_list = unique_key_result['unique_key_list'] -%}
            DELETE FROM {{ target }} DBT_INTERNAL_DEST
            WHERE ({% for key in unique_key_list %}
                   DBT_INTERNAL_DEST.{{ key }} {{ ", " if not loop.last}}
                   {% endfor %})
            IN    (SELECT {% for key in unique_key_list %}
                   DBT_INTERNAL_SOURCE.{{ key }} {{ ", " if not loop.last}}
                   {% endfor %} FROM {{source}} DBT_INTERNAL_SOURCE)
        {%- if incremental_predicates -%}
            {% for predicate in incremental_predicates %}
                AND {{ predicate }}
            {% endfor %}
        {%- endif -%}
    {%- elif incremental_predicates -%}
        DELETE FROM {{ target }} DBT_INTERNAL_DEST
        WHERE  {%- for predicate in incremental_predicates -%}
                    {{ "AND" if not loop.first}} {{ predicate }}
                {%- endfor -%}
    {%- endif -%}
{% endmacro %}


{#
  versioned_merge incremental strategy
  ====================================

  Version-aware incremental strategy for Change Data Capture (CDC) streams.

  A single CDC batch may contain several versions of the same business key and
  a batch may arrive late, i.e. after a newer version of a key was already
  applied to the target table. This strategy guarantees that:

    1. Only the highest version of every business key in the batch is applied.
    2. An event is applied only when its version is higher than the version
       already recorded for the key, so stale events never overwrite newer data.
    3. When the winning (highest version) event of the batch is a delete, the
       business row is removed from the target table while the business key and
       its version are kept in an anti-resurrection ledger table
       (<target>__dbt_cdc_ledger) so that older events cannot bring deleted
       rows back.
    4. Events carrying the same business key and the same version but different
       contents abort the run with an error before the target table is
       modified. This is checked both within the batch and against the events
       that were already applied.
    5. Re-applying the same batch is a no-op, so loads are idempotent.

  Non-delete events are written using the model column mapping exactly like the
  'merge' strategy, so downstream queries read the correct current table.

  Required model configs:
    - unique_key:          business key column(s) of the CDC stream
    - cdc_version_column:  monotonically increasing version column (number,
                           timestamp, ...) identifying the version of an event
    - cdc_delete_column:   column whose value flags a delete event

  Optional model configs:
    - cdc_delete_value:    value of cdc_delete_column marking a delete event.
                           Defaults to 'D'.
#}

{% macro oracle__cdc_ledger_relation(target_relation) %}
    {#-- Relation of the anti-resurrection ledger table belonging to a versioned_merge target --#}
    {%- set ledger_identifier = target_relation.identifier ~ '__dbt_cdc_ledger' -%}
    {{ return(target_relation.incorporate(path={'identifier': ledger_identifier})) }}
{% endmacro %}

{% macro oracle__cdc_serialize_column(column, source_alias) %}
    {#-- NLS-independent serialization of one column for content hashing --#}
    {%- set qcol = source_alias ~ '.' ~ adapter.check_and_quote_identifier(column.name, model.columns) -%}
    {%- set data_type_upper = column.data_type | upper -%}
    {%- set base_type = data_type_upper.split('(')[0].split(' ')[0] -%}
    {%- if base_type == 'DATE' -%}
        TO_CHAR({{ qcol }}, 'YYYY-MM-DD HH24:MI:SS')
    {%- elif base_type == 'TIMESTAMP' -%}
        {%- if 'WITH' in data_type_upper -%}
            TO_CHAR({{ qcol }}, 'YYYY-MM-DD HH24:MI:SS.FF9 TZH:TZM')
        {%- else -%}
            TO_CHAR({{ qcol }}, 'YYYY-MM-DD HH24:MI:SS.FF9')
        {%- endif -%}
    {%- elif base_type in ('NUMBER', 'FLOAT', 'INTEGER', 'INT', 'SMALLINT', 'DECIMAL', 'DEC', 'NUMERIC', 'BINARY_FLOAT', 'BINARY_DOUBLE', 'DOUBLE', 'REAL') -%}
        TO_CHAR({{ qcol }}, 'TM9', 'NLS_NUMERIC_CHARACTERS=''.,''')
    {%- elif base_type == 'BOOLEAN' -%}
        TO_CHAR({{ qcol }})
    {%- elif 'RAW' in data_type_upper -%}
        RAWTOHEX({{ qcol }})
    {%- elif base_type == 'BLOB' -%}
        RAWTOHEX(DBMS_LOB.SUBSTR({{ qcol }}, 4000, 1))
    {%- elif base_type in ('CLOB', 'NCLOB') -%}
        DBMS_LOB.SUBSTR({{ qcol }}, 4000, 1)
    {%- else -%}
        {{ qcol }}
    {%- endif -%}
{% endmacro %}

{% macro oracle__cdc_content_hash_expression(dest_columns, source_alias) %}
    {#-- Deterministic SHA-256 hash over the full row content. Columns are
         concatenated with a record separator and NULLs are mapped to a
         dedicated marker so that distinct contents hash distinctly. --#}
    RAWTOHEX(STANDARD_HASH(
    {%- for column in dest_columns -%}
        NVL({{ oracle__cdc_serialize_column(column, source_alias) }}, CHR(1)){% if not loop.last %} || CHR(30) ||{% endif %}
    {%- endfor -%}
    , 'SHA256'))
{% endmacro %}

{% macro oracle__cdc_key_join_predicates(unique_key_list, left_alias, right_alias) %}
    {%- set join_predicates = [] -%}
    {% for key in unique_key_list %}
        {%- do join_predicates.append(left_alias ~ '.' ~ key ~ ' = ' ~ right_alias ~ '.' ~ key) -%}
    {% endfor %}
    {{ return(join_predicates | join(' AND ')) }}
{% endmacro %}

{% macro oracle__get_incremental_versioned_merge_sql(args_dict) %}
    {%- set parallel = config.get('parallel', none) -%}
    {%- set dest_columns = args_dict["dest_columns"] -%}
    {%- set temp_relation = args_dict["temp_relation"] -%}
    {%- set target_relation = args_dict["target_relation"] -%}
    {%- set unique_key = args_dict["unique_key"] -%}
    {%- set incremental_predicates = args_dict["incremental_predicates"] -%}
    {%- set dest_column_names = dest_columns | map(attribute='name') | list -%}
    {%- set dest_cols_csv = get_quoted_column_csv(model, dest_column_names) -%}

    {%- set cdc_version_column = config.get('cdc_version_column') -%}
    {%- set cdc_delete_column = config.get('cdc_delete_column') -%}
    {%- set cdc_delete_value = config.get('cdc_delete_value', 'D') -%}

    {#-- Validate the strategy configuration --#}
    {%- if not unique_key -%}
        {{ exceptions.raise_compiler_error(
            "The 'versioned_merge' incremental strategy requires a 'unique_key' model config holding the business key of the CDC stream.") }}
    {%- endif -%}
    {%- if not cdc_version_column -%}
        {{ exceptions.raise_compiler_error(
            "The 'versioned_merge' incremental strategy requires the 'cdc_version_column' model config naming the monotonically increasing version column of the CDC stream.") }}
    {%- endif -%}
    {%- if not cdc_delete_column -%}
        {{ exceptions.raise_compiler_error(
            "The 'versioned_merge' incremental strategy requires the 'cdc_delete_column' model config naming the column that flags delete events.") }}
    {%- endif -%}
    {%- set dest_column_names_upper = dest_column_names | map('upper') | list -%}
    {%- if (cdc_version_column not in dest_column_names) and ((cdc_version_column | upper) not in dest_column_names_upper) -%}
        {{ exceptions.raise_compiler_error(
            "cdc_version_column '" ~ cdc_version_column ~ "' is not a column of model " ~ model.relation_name ~
            ". Available columns: " ~ (dest_column_names | join(', '))) }}
    {%- endif -%}
    {%- if (cdc_delete_column not in dest_column_names) and ((cdc_delete_column | upper) not in dest_column_names_upper) -%}
        {{ exceptions.raise_compiler_error(
            "cdc_delete_column '" ~ cdc_delete_column ~ "' is not a column of model " ~ model.relation_name ~
            ". Available columns: " ~ (dest_column_names | join(', '))) }}
    {%- endif -%}

    {%- set unique_key_result = oracle_check_and_quote_unique_key_for_incremental_merge(unique_key, incremental_predicates) -%}
    {%- set unique_key_list = unique_key_result['unique_key_list'] -%}
    {%- set unique_key_merge_predicates = unique_key_result['unique_key_merge_predicates'] -%}
    {%- set keys_csv = unique_key_list | join(', ') -%}
    {%- set first_key = unique_key_list[0] -%}
    {%- set version_col = adapter.check_and_quote_identifier(cdc_version_column, model.columns) -%}
    {%- set delete_col = adapter.check_and_quote_identifier(cdc_delete_column, model.columns) -%}
    {%- set delete_value_literal = "'" ~ (cdc_delete_value | replace("'", "''")) ~ "'" -%}
    {%- set ledger_relation = oracle__cdc_ledger_relation(target_relation) -%}
    {%- set merge_update_columns = config.get('merge_update_columns') -%}
    {%- set merge_exclude_columns = config.get('merge_exclude_columns') -%}
    {%- set update_columns = get_merge_update_columns(merge_update_columns, merge_exclude_columns, dest_columns) -%}
    {%- set src_to_ledger_join = oracle__cdc_key_join_predicates(unique_key_list, 'DBT_CDC_SRC', 'DBT_CDC_LEDGER') -%}
    {%- set batch_to_ledger_join = oracle__cdc_key_join_predicates(unique_key_list, 'DBT_CDC_BATCH', 'DBT_CDC_LEDGER') -%}

    {#-- The highest version of every business key in this batch, together with
         a hash of its content. Reused by every statement below. --#}
    {%- set winners_sql -%}
        select DBT_CDC_DEDUP.*,
               {{ oracle__cdc_content_hash_expression(dest_columns, 'DBT_CDC_DEDUP') }} as DBT_CDC_CONTENT_HASH
        from (
            select DBT_CDC_BATCH.*,
                   row_number() over (
                       partition by {{ keys_csv }}
                       order by {{ version_col }} desc nulls last
                   ) as DBT_CDC_RN
            from {{ temp_relation }} DBT_CDC_BATCH
        ) DBT_CDC_DEDUP
        where DBT_CDC_DEDUP.DBT_CDC_RN = 1
    {%- endset -%}

DECLARE
  DBT_CDC_CONFLICT_COUNT NUMBER;
BEGIN
  -- 1. Create the anti-resurrection ledger on first use and initialize it
  --    with the highest version of every key already present in the target.
  BEGIN
    EXECUTE IMMEDIATE 'CREATE TABLE {{ ledger_relation }} AS
        SELECT {{ keys_csv }},
               {{ version_col }} AS DBT_CDC_VERSION,
               CAST(NULL AS VARCHAR2(64)) AS DBT_CDC_CONTENT_HASH,
               CAST(NULL AS CHAR(1)) AS DBT_CDC_IS_DELETED
        FROM {{ target_relation }}
        WHERE 1 = 0';
    INSERT INTO {{ ledger_relation }} ({{ keys_csv }}, DBT_CDC_VERSION, DBT_CDC_CONTENT_HASH, DBT_CDC_IS_DELETED)
    SELECT {{ keys_csv }}, DBT_CDC_VERSION, DBT_CDC_CONTENT_HASH, DBT_CDC_IS_DELETED
    FROM (
        SELECT {{ keys_csv }},
               {{ version_col }} AS DBT_CDC_VERSION,
               {{ oracle__cdc_content_hash_expression(dest_columns, 'DBT_CDC_SEED') }} AS DBT_CDC_CONTENT_HASH,
               CASE WHEN {{ delete_col }} = {{ delete_value_literal }} THEN 'Y' ELSE 'N' END AS DBT_CDC_IS_DELETED,
               ROW_NUMBER() OVER (PARTITION BY {{ keys_csv }} ORDER BY {{ version_col }} DESC NULLS LAST) AS DBT_CDC_RN
        FROM {{ target_relation }} DBT_CDC_SEED
    )
    WHERE DBT_CDC_RN = 1;
    COMMIT;
  EXCEPTION
    WHEN OTHERS THEN
      -- ORA-00955: the ledger already exists, keep it as is
      IF SQLCODE != -955 THEN
        BEGIN
          EXECUTE IMMEDIATE 'DROP TABLE {{ ledger_relation }} CASCADE CONSTRAINTS PURGE';
        EXCEPTION
          WHEN OTHERS THEN
            NULL;
        END;
        RAISE;
      END IF;
  END;

  -- 2. Reject the batch before touching the target when the same business key
  --    and version arrive with conflicting contents inside the batch.
  SELECT COUNT(*) INTO DBT_CDC_CONFLICT_COUNT
  FROM (
      SELECT {{ keys_csv }}, {{ version_col }}
      FROM {{ temp_relation }} DBT_CDC_BATCH
      GROUP BY {{ keys_csv }}, {{ version_col }}
      HAVING COUNT(DISTINCT {{ oracle__cdc_content_hash_expression(dest_columns, 'DBT_CDC_BATCH') }}) > 1
  );
  IF DBT_CDC_CONFLICT_COUNT > 0 THEN
    RAISE_APPLICATION_ERROR(-20091, 'dbt versioned_merge: the CDC batch contains ' || DBT_CDC_CONFLICT_COUNT ||
      ' business key/version combination(s) with conflicting contents for {{ target_relation }}. No changes were applied.');
  END IF;

  -- 3. Reject the batch before touching the target when an event was already
  --    applied for the same business key and version with different contents.
  SELECT COUNT(*) INTO DBT_CDC_CONFLICT_COUNT
  FROM {{ temp_relation }} DBT_CDC_BATCH
  INNER JOIN {{ ledger_relation }} DBT_CDC_LEDGER
          ON {{ batch_to_ledger_join }}
         AND DBT_CDC_LEDGER.DBT_CDC_VERSION = DBT_CDC_BATCH.{{ version_col }}
  WHERE {{ oracle__cdc_content_hash_expression(dest_columns, 'DBT_CDC_BATCH') }} <> DBT_CDC_LEDGER.DBT_CDC_CONTENT_HASH;
  IF DBT_CDC_CONFLICT_COUNT > 0 THEN
    RAISE_APPLICATION_ERROR(-20092, 'dbt versioned_merge: ' || DBT_CDC_CONFLICT_COUNT ||
      ' CDC event(s) for {{ target_relation }} were already applied with different contents for the same business key and version. No changes were applied.');
  END IF;

  -- 4. Winning delete events remove the business row. The key and its version
  --    stay recorded in the ledger (step 6) so older events cannot resurrect it.
  DELETE FROM {{ target_relation }} DBT_INTERNAL_DEST
  WHERE ({% for key in unique_key_list %}DBT_INTERNAL_DEST.{{ key }}{{ ", " if not loop.last }}{% endfor %})
  IN    (SELECT {% for key in unique_key_list %}DBT_CDC_SRC.{{ key }}{{ ", " if not loop.last }}{% endfor %}
         FROM ({{ winners_sql }}) DBT_CDC_SRC
         LEFT JOIN {{ ledger_relation }} DBT_CDC_LEDGER
                ON {{ src_to_ledger_join }}
         WHERE DBT_CDC_SRC.{{ delete_col }} = {{ delete_value_literal }}
           AND (DBT_CDC_LEDGER.{{ first_key }} IS NULL
                OR DBT_CDC_SRC.{{ version_col }} > DBT_CDC_LEDGER.DBT_CDC_VERSION))
  {%- if incremental_predicates -%}
    {% for predicate in incremental_predicates %}
        AND {{ predicate }}
    {% endfor %}
  {%- endif -%};

  -- 5. Winning non-delete events newer than the applied version are written
  --    using the model column mapping.
  MERGE {% if parallel %} /*+parallel({{ parallel }})*/ {% endif %} INTO {{ target_relation }} DBT_INTERNAL_DEST
  USING (
      SELECT DBT_CDC_SRC.*
      FROM ({{ winners_sql }}) DBT_CDC_SRC
      LEFT JOIN {{ ledger_relation }} DBT_CDC_LEDGER
             ON {{ src_to_ledger_join }}
      WHERE (DBT_CDC_SRC.{{ delete_col }} IS NULL OR DBT_CDC_SRC.{{ delete_col }} <> {{ delete_value_literal }})
        AND (DBT_CDC_LEDGER.{{ first_key }} IS NULL
             OR DBT_CDC_SRC.{{ version_col }} > DBT_CDC_LEDGER.DBT_CDC_VERSION)
  ) DBT_INTERNAL_SOURCE
  ON ({{ unique_key_merge_predicates | join(' AND ') }})
  WHEN MATCHED THEN
    UPDATE SET
    {% for col in update_columns if (col.upper() not in unique_key_list and col not in unique_key_list) -%}
      DBT_INTERNAL_DEST.{{ col }} = DBT_INTERNAL_SOURCE.{{ col }}{% if not loop.last %}, {% endif %}
    {% endfor -%}
  WHEN NOT MATCHED THEN
    INSERT({{ dest_cols_csv }})
    VALUES(
      {% for col in dest_columns -%}
        DBT_INTERNAL_SOURCE.{{ adapter.check_and_quote_identifier(col.name, model.columns) }}{% if not loop.last %}, {% endif %}
      {% endfor -%}
    );

  -- 6. Record the applied version of every business key in the ledger.
  MERGE INTO {{ ledger_relation }} DBT_CDC_LEDGER
  USING ({{ winners_sql }}) DBT_CDC_SRC
  ON ({{ src_to_ledger_join }})
  WHEN MATCHED THEN
    UPDATE SET
      DBT_CDC_LEDGER.DBT_CDC_VERSION = DBT_CDC_SRC.{{ version_col }},
      DBT_CDC_LEDGER.DBT_CDC_CONTENT_HASH = DBT_CDC_SRC.DBT_CDC_CONTENT_HASH,
      DBT_CDC_LEDGER.DBT_CDC_IS_DELETED = CASE WHEN DBT_CDC_SRC.{{ delete_col }} = {{ delete_value_literal }} THEN 'Y' ELSE 'N' END
    WHERE DBT_CDC_SRC.{{ version_col }} > DBT_CDC_LEDGER.DBT_CDC_VERSION
  WHEN NOT MATCHED THEN
    INSERT({{ keys_csv }}, DBT_CDC_VERSION, DBT_CDC_CONTENT_HASH, DBT_CDC_IS_DELETED)
    VALUES(
      {% for key in unique_key_list %}DBT_CDC_SRC.{{ key }}{{ ", " if not loop.last }}{% endfor %},
      DBT_CDC_SRC.{{ version_col }},
      DBT_CDC_SRC.DBT_CDC_CONTENT_HASH,
      CASE WHEN DBT_CDC_SRC.{{ delete_col }} = {{ delete_value_literal }} THEN 'Y' ELSE 'N' END
    );
END;
{% endmacro %}

{% macro oracle__get_incremental_delete_insert_sql(args_dict) %}
    {%- set parallel = config.get('parallel', none) -%}
    {%- set insert_mode = config.get('insert_mode', none) -%}
    {%- set insert_hint = generate_insert_hint(parallel, insert_mode) -%}
    {%- set dest_columns = args_dict["dest_columns"] -%}
    {%- set temp_relation = args_dict["temp_relation"] -%}
    {%- set target_relation = args_dict["target_relation"] -%}
    {%- set unique_key = args_dict["unique_key"] -%}
    {%- set dest_column_names = dest_columns | map(attribute='name') | list -%}
    {%- set dest_cols_csv = get_quoted_column_csv(model, dest_column_names)  -%}
    {%- set incremental_predicates = args_dict["incremental_predicates"] -%}
    {%- if unique_key or incremental_predicates -%}
        BEGIN
        EXECUTE IMMEDIATE  '{{ oracle__get_delete_sql_for_delete_insert_strategy(target_relation, temp_relation, unique_key, incremental_predicates) }}';
        EXECUTE IMMEDIATE 'INSERT {{ insert_hint }}
           into {{ target_relation }} ({{ dest_cols_csv }})(
           select {{ dest_cols_csv }}
           from {{ temp_relation }})';
        END;
    {%- else -%}
    insert {{ insert_hint }}
    into  {{ target_relation }} ({{ dest_cols_csv }})
    (
       select {{ dest_cols_csv }}
       from {{ temp_relation }}
    )
    {%- endif -%}
{% endmacro %}
