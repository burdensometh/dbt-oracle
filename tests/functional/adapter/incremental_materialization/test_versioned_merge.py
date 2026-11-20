"""
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
"""

import datetime
from pathlib import Path

import pytest
from dbt.tests.util import run_dbt

# seeds/cdc_landing.csv
# The landing table holds one CDC batch at a time. The initial batch is the
# initial snapshot load; every following batch is swapped in before a dbt run.
cdc_landing_csv = """
id,name,balance,updated_at,cdc_version,cdc_op
1,Alice,100,2026-01-01T10:00:00,1,I
2,Bob,200,2026-01-01T10:01:00,1,I
3,Carol,300,2026-01-01T10:02:00,1,I
""".lstrip()

# models/cdc_versioned.sql
cdc_versioned_model_sql = """
{{
    config(
        materialized='incremental',
        unique_key='id',
        incremental_strategy='versioned_merge',
        cdc_version_column='cdc_version',
        cdc_delete_column='cdc_op',
        cdc_delete_value='D'
    )
}}
select id, name, balance, updated_at, cdc_version, cdc_op
from {{ ref('cdc_landing') }}
"""

# seeds/batch2_multi_version.sql
# - id=1 arrives twice in the same batch: v2 must lose to v3
# - id=2 is deleted at v2
# - id=4 is new
batch2_multi_version_sql = """
INSERT ALL
    INTO {schema}.cdc_landing (id, name, balance, updated_at, cdc_version, cdc_op)
        VALUES (1, 'Alice', 150, TO_TIMESTAMP('2026-01-02 09:00:00', 'YYYY-MM-DD HH24:MI:SS'), 2, 'U')
    INTO {schema}.cdc_landing (id, name, balance, updated_at, cdc_version, cdc_op)
        VALUES (1, 'Alice Cooper', 160.75, TO_TIMESTAMP('2026-01-02 10:00:00', 'YYYY-MM-DD HH24:MI:SS'), 3, 'U')
    INTO {schema}.cdc_landing (id, name, balance, updated_at, cdc_version, cdc_op)
        VALUES (2, NULL, NULL, NULL, 2, 'D')
    INTO {schema}.cdc_landing (id, name, balance, updated_at, cdc_version, cdc_op)
        VALUES (4, 'Dave', 400, TO_TIMESTAMP('2026-01-02 11:00:00', 'YYYY-MM-DD HH24:MI:SS'), 1, 'I')
SELECT * FROM dual
"""

# seeds/batch3_out_of_order.sql
# - id=1 v2 is older than the applied v3 and must not overwrite it
# - id=2 v1 is older than the applied delete v2 and must not resurrect the row
# - id=3 v2 is newer and must be applied
batch3_out_of_order_sql = """
INSERT ALL
    INTO {schema}.cdc_landing (id, name, balance, updated_at, cdc_version, cdc_op)
        VALUES (1, 'Alice', 150, TO_TIMESTAMP('2026-01-02 09:00:00', 'YYYY-MM-DD HH24:MI:SS'), 2, 'U')
    INTO {schema}.cdc_landing (id, name, balance, updated_at, cdc_version, cdc_op)
        VALUES (2, 'Bob', 200, TO_TIMESTAMP('2026-01-01 10:01:00', 'YYYY-MM-DD HH24:MI:SS'), 1, 'I')
    INTO {schema}.cdc_landing (id, name, balance, updated_at, cdc_version, cdc_op)
        VALUES (3, 'Carolyn', 350, TO_TIMESTAMP('2026-01-03 08:00:00', 'YYYY-MM-DD HH24:MI:SS'), 2, 'U')
SELECT * FROM dual
"""

# seeds/batch4_conflicting_with_applied.sql
# id=1 v3 was already applied with different contents: the run must fail
# before the target table is modified.
batch4_conflicting_with_applied_sql = """
INSERT ALL
    INTO {schema}.cdc_landing (id, name, balance, updated_at, cdc_version, cdc_op)
        VALUES (1, 'Alice Changed', 999, TO_TIMESTAMP('2026-01-02 10:00:00', 'YYYY-MM-DD HH24:MI:SS'), 3, 'U')
SELECT * FROM dual
"""

# seeds/batch5_conflicting_within_batch.sql
# id=5 v1 arrives twice in the same batch with different contents:
# the run must fail before the target table is modified.
batch5_conflicting_within_batch_sql = """
INSERT ALL
    INTO {schema}.cdc_landing (id, name, balance, updated_at, cdc_version, cdc_op)
        VALUES (5, 'Eve', 500, TO_TIMESTAMP('2026-01-04 08:00:00', 'YYYY-MM-DD HH24:MI:SS'), 1, 'I')
    INTO {schema}.cdc_landing (id, name, balance, updated_at, cdc_version, cdc_op)
        VALUES (5, 'Eve Changed', 555, TO_TIMESTAMP('2026-01-04 08:00:00', 'YYYY-MM-DD HH24:MI:SS'), 1, 'I')
SELECT * FROM dual
"""


class TestVersionedMerge:

    @pytest.fixture(scope="class")
    @classmethod
    def seeds(self):
        return {
            "cdc_landing.csv": cdc_landing_csv,
            "batch2_multi_version.sql": batch2_multi_version_sql,
            "batch3_out_of_order.sql": batch3_out_of_order_sql,
            "batch4_conflicting_with_applied.sql": batch4_conflicting_with_applied_sql,
            "batch5_conflicting_within_batch.sql": batch5_conflicting_within_batch_sql,
        }

    @pytest.fixture(scope="class")
    @classmethod
    def models(self):
        return {
            "cdc_versioned.sql": cdc_versioned_model_sql,
        }

    @staticmethod
    def load_batch(project, batch_file):
        # swap in the next CDC batch
        project.run_sql("delete from {schema}.cdc_landing")
        project.run_sql_file(Path("seeds") / Path(batch_file))

    @staticmethod
    def current_rows(project):
        query = "select id, name, balance, updated_at, cdc_version, cdc_op " \
                "from {schema}.cdc_versioned order by id".format(schema=project.test_schema)
        return project.run_sql(query, fetch="all")

    @staticmethod
    def ledger_rows(project):
        query = "select id, dbt_cdc_version, dbt_cdc_is_deleted " \
                "from {schema}.cdc_versioned__dbt_cdc_ledger order by id".format(schema=project.test_schema)
        return project.run_sql(query, fetch="all")

    def test_versioned_merge(self, project):
        # initial snapshot load
        results = run_dbt(['seed'])
        assert len(results) == 1
        results = run_dbt(['run'])
        assert len(results) == 1
        assert self.current_rows(project) == [
            (1, 'Alice', 100, datetime.datetime(2026, 1, 1, 10, 0, 0), 1, 'I'),
            (2, 'Bob', 200, datetime.datetime(2026, 1, 1, 10, 1, 0), 1, 'I'),
            (3, 'Carol', 300, datetime.datetime(2026, 1, 1, 10, 2, 0), 1, 'I'),
        ]

        # batch 2: highest in-batch version wins, deletes remove the business
        # row, new keys are inserted
        self.load_batch(project, "batch2_multi_version.sql")
        results = run_dbt(['run'])
        assert len(results) == 1
        expected_after_batch2 = [
            (1, 'Alice Cooper', 160.75, datetime.datetime(2026, 1, 2, 10, 0, 0), 3, 'U'),
            (3, 'Carol', 300, datetime.datetime(2026, 1, 1, 10, 2, 0), 1, 'I'),
            (4, 'Dave', 400, datetime.datetime(2026, 1, 2, 11, 0, 0), 1, 'I'),
        ]
        assert self.current_rows(project) == expected_after_batch2
        # the ledger keeps the applied version of every key, deleted or not
        expected_ledger_after_batch2 = [
            (1, 3, 'N'),
            (2, 2, 'Y'),
            (3, 1, 'N'),
            (4, 1, 'N'),
        ]
        assert self.ledger_rows(project) == expected_ledger_after_batch2

        # re-loading the same batch must not change the result
        results = run_dbt(['run'])
        assert len(results) == 1
        assert self.current_rows(project) == expected_after_batch2
        assert self.ledger_rows(project) == expected_ledger_after_batch2

        # batch 3: stale events must neither overwrite newer versions nor
        # resurrect a deleted row; newer events are still applied
        self.load_batch(project, "batch3_out_of_order.sql")
        results = run_dbt(['run'])
        assert len(results) == 1
        expected_after_batch3 = [
            (1, 'Alice Cooper', 160.75, datetime.datetime(2026, 1, 2, 10, 0, 0), 3, 'U'),
            (3, 'Carolyn', 350, datetime.datetime(2026, 1, 3, 8, 0, 0), 2, 'U'),
            (4, 'Dave', 400, datetime.datetime(2026, 1, 2, 11, 0, 0), 1, 'I'),
        ]
        assert self.current_rows(project) == expected_after_batch3
        expected_ledger_after_batch3 = [
            (1, 3, 'N'),
            (2, 2, 'Y'),
            (3, 2, 'N'),
            (4, 1, 'N'),
        ]
        assert self.ledger_rows(project) == expected_ledger_after_batch3

        # re-loading the out-of-order batch is a no-op as well
        results = run_dbt(['run'])
        assert len(results) == 1
        assert self.current_rows(project) == expected_after_batch3
        assert self.ledger_rows(project) == expected_ledger_after_batch3

        # same business key and version but different contents than what was
        # already applied: the run fails and the table is left untouched
        self.load_batch(project, "batch4_conflicting_with_applied.sql")
        results = run_dbt(['run'], expect_pass=False)
        assert self.current_rows(project) == expected_after_batch3
        assert self.ledger_rows(project) == expected_ledger_after_batch3

        # conflicting contents for the same key and version inside one batch:
        # the run fails and the table is left untouched
        self.load_batch(project, "batch5_conflicting_within_batch.sql")
        results = run_dbt(['run'], expect_pass=False)
        assert self.current_rows(project) == expected_after_batch3
        assert self.ledger_rows(project) == expected_ledger_after_batch3


# seeds/cdc_landing_composite.csv
cdc_landing_composite_csv = """
order_id,line_no,amount,cdc_version,cdc_op
1001,1,50,1,I
1001,2,75,1,I
1002,1,120,1,I
""".lstrip()

# models/cdc_versioned_composite.sql
cdc_versioned_composite_model_sql = """
{{
    config(
        materialized='incremental',
        unique_key=['order_id', 'line_no'],
        incremental_strategy='versioned_merge',
        cdc_version_column='cdc_version',
        cdc_delete_column='cdc_op'
    )
}}
select order_id, line_no, amount, cdc_version, cdc_op
from {{ ref('cdc_landing_composite') }}
"""

# seeds/composite_batch2.sql
composite_batch2_sql = """
INSERT ALL
    INTO {schema}.cdc_landing_composite (order_id, line_no, amount, cdc_version, cdc_op)
        VALUES (1001, 1, 55, 2, 'U')
    INTO {schema}.cdc_landing_composite (order_id, line_no, amount, cdc_version, cdc_op)
        VALUES (1001, 2, NULL, 2, 'D')
    INTO {schema}.cdc_landing_composite (order_id, line_no, amount, cdc_version, cdc_op)
        VALUES (1003, 1, 200, 1, 'I')
SELECT * FROM dual
"""

# seeds/composite_batch3_out_of_order.sql
# stale versions of both order lines of 1001: must be ignored entirely
composite_batch3_out_of_order_sql = """
INSERT ALL
    INTO {schema}.cdc_landing_composite (order_id, line_no, amount, cdc_version, cdc_op)
        VALUES (1001, 1, 50, 1, 'I')
    INTO {schema}.cdc_landing_composite (order_id, line_no, amount, cdc_version, cdc_op)
        VALUES (1001, 2, 75, 1, 'I')
SELECT * FROM dual
"""


class TestVersionedMergeCompositeKey:

    @pytest.fixture(scope="class")
    @classmethod
    def seeds(self):
        return {
            "cdc_landing_composite.csv": cdc_landing_composite_csv,
            "composite_batch2.sql": composite_batch2_sql,
            "composite_batch3_out_of_order.sql": composite_batch3_out_of_order_sql,
        }

    @pytest.fixture(scope="class")
    @classmethod
    def models(self):
        return {
            "cdc_versioned_composite.sql": cdc_versioned_composite_model_sql,
        }

    @staticmethod
    def current_rows(project):
        query = "select order_id, line_no, amount, cdc_version, cdc_op " \
                "from {schema}.cdc_versioned_composite order by order_id, line_no".format(schema=project.test_schema)
        return project.run_sql(query, fetch="all")

    def test_composite_key(self, project):
        results = run_dbt(['seed'])
        assert len(results) == 1
        results = run_dbt(['run'])
        assert len(results) == 1
        assert self.current_rows(project) == [
            (1001, 1, 50, 1, 'I'),
            (1001, 2, 75, 1, 'I'),
            (1002, 1, 120, 1, 'I'),
        ]

        project.run_sql("delete from {schema}.cdc_landing_composite")
        project.run_sql_file(Path("seeds") / Path("composite_batch2.sql"))
        results = run_dbt(['run'])
        assert len(results) == 1
        expected = [
            (1001, 1, 55, 2, 'U'),
            (1002, 1, 120, 1, 'I'),
            (1003, 1, 200, 1, 'I'),
        ]
        assert self.current_rows(project) == expected

        # idempotent re-apply of the same batch
        results = run_dbt(['run'])
        assert len(results) == 1
        assert self.current_rows(project) == expected

        # stale batch: no overwrite and no resurrection of the deleted line
        project.run_sql("delete from {schema}.cdc_landing_composite")
        project.run_sql_file(Path("seeds") / Path("composite_batch3_out_of_order.sql"))
        results = run_dbt(['run'])
        assert len(results) == 1
        assert self.current_rows(project) == expected


# seeds/cdc_bad_cfg_landing.csv
cdc_bad_cfg_landing_csv = """
id,name,cdc_version,cdc_op
1,Alice,1,I
""".lstrip()

# models/cdc_bad_config.sql: cdc_version_column is missing
cdc_bad_config_model_sql = """
{{
    config(
        materialized='incremental',
        unique_key='id',
        incremental_strategy='versioned_merge',
        cdc_delete_column='cdc_op'
    )
}}
select id, name, cdc_version, cdc_op
from {{ ref('cdc_bad_cfg_landing') }}
"""


class TestVersionedMergeMissingVersionColumn:

    @pytest.fixture(scope="class")
    @classmethod
    def seeds(self):
        return {
            "cdc_bad_cfg_landing.csv": cdc_bad_cfg_landing_csv,
        }

    @pytest.fixture(scope="class")
    @classmethod
    def models(self):
        return {
            "cdc_bad_config.sql": cdc_bad_config_model_sql,
        }

    def test_missing_version_column_fails(self, project):
        results = run_dbt(['seed'])
        assert len(results) == 1
        # the first run only creates the table and does not use the strategy
        results = run_dbt(['run'])
        assert len(results) == 1
        # the first incremental run validates the strategy configuration
        run_dbt(['run'], expect_pass=False)


# seeds/cdc_nokey_landing.csv
cdc_nokey_landing_csv = """
id,name,cdc_version,cdc_op
1,Alice,1,I
""".lstrip()

# models/cdc_no_key_config.sql: unique_key is missing
cdc_no_key_config_model_sql = """
{{
    config(
        materialized='incremental',
        incremental_strategy='versioned_merge',
        cdc_version_column='cdc_version',
        cdc_delete_column='cdc_op'
    )
}}
select id, name, cdc_version, cdc_op
from {{ ref('cdc_nokey_landing') }}
"""


class TestVersionedMergeMissingUniqueKey:

    @pytest.fixture(scope="class")
    @classmethod
    def seeds(self):
        return {
            "cdc_nokey_landing.csv": cdc_nokey_landing_csv,
        }

    @pytest.fixture(scope="class")
    @classmethod
    def models(self):
        return {
            "cdc_no_key_config.sql": cdc_no_key_config_model_sql,
        }

    def test_missing_unique_key_fails(self, project):
        results = run_dbt(['seed'])
        assert len(results) == 1
        results = run_dbt(['run'])
        assert len(results) == 1
        run_dbt(['run'], expect_pass=False)
