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

import pytest

from dbt.tests.util import run_dbt
from dbt_common.exceptions import DbtRuntimeError


# Two tenants (t1, t2), two rows each. Every run only pulls a subset of
# tenants, so rows of tenants not pulled must never be treated as deleted.
seed_csv = """
id,tenant_id,name,status
1,t1,alpha,active
2,t1,beta,active
3,t2,gamma,active
4,t2,delta,active
""".lstrip()


invalidate_snapshot_sql = """
{% snapshot scoped_snapshot %}
    {{ config(
        check_cols=['name', 'status'],
        unique_key=['id'],
        strategy='check',
        target_database=database,
        target_schema=schema,
        hard_deletes='invalidate',
        hard_deletes_scope_column='tenant_id',
        hard_deletes_scope_values=var('scope_values')
    ) }}
    SELECT * FROM {{ ref('seed') }}
{% endsnapshot %}
""".strip()


new_record_snapshot_sql = """
{% snapshot scoped_snapshot %}
    {{ config(
        check_cols=['name', 'status'],
        unique_key=['id'],
        strategy='check',
        target_database=database,
        target_schema=schema,
        hard_deletes='new_record',
        hard_deletes_scope_column='tenant_id',
        hard_deletes_scope_values=var('scope_values')
    ) }}
    SELECT * FROM {{ ref('seed') }}
{% endsnapshot %}
""".strip()

incomplete_snapshot_sql = """
{% snapshot scoped_snapshot %}
    {{ config(
        check_cols=['name', 'status'],
        unique_key=['id'],
        strategy='check',
        target_database=database,
        target_schema=schema,
        hard_deletes='invalidate',
        hard_deletes_scope_values=var('scope_values')
    ) }}
    SELECT * FROM {{ ref('seed') }}
{% endsnapshot %}
""".strip()


# Source now only contains tenant t1; tenant t2 rows are missing because they
# were not collected, not because they were deleted.
source_only_t1_sql = """
DELETE FROM {schema}.seed WHERE tenant_id = 't2'
"""

# An undeclared tenant appears in the source while the run only claims t1.
source_with_undeclared_sql = """
INSERT INTO {schema}.seed (id, tenant_id, name, status)
VALUES (5, 't3', 'epsilon', 'active')
"""

# Source for t1 is completely empty: deletion must still be detected inside
# the declared scope.
source_empty_t1_sql = """
DELETE FROM {schema}.seed WHERE tenant_id = 't1'
"""

# The previously deleted t1 row comes back (re-appearance).
reinsert_row_sql = """
INSERT INTO {schema}.seed (id, tenant_id, name, status)
VALUES (2, 't1', 'beta', 'active')
"""


class ScopedSnapshotBase:
    snapshot_sql = invalidate_snapshot_sql
    hard_deletes_mode = "invalidate"

    @pytest.fixture(scope="class")
    @classmethod
    def seeds(cls):
        return {"seed.csv": seed_csv}

    @pytest.fixture(scope="class")
    @classmethod
    def snapshots(cls):
        return {"scoped_snapshot.sql": cls.snapshot_sql}

    @staticmethod
    def _open_rows(project, id_value):
        return project.run_sql(
            f"select * from scoped_snapshot where id = {id_value} and dbt_valid_to is null",
            fetch="all",
        )

    @staticmethod
    def _all_rows(project, id_value):
        return project.run_sql(
            f"select * from scoped_snapshot where id = {id_value}",
            fetch="all",
        )

    def test_scoped_hard_deletes(self, project):
        # 1. Initial load declares both tenants.
        results = run_dbt(["seed"])
        assert len(results) == 1
        results = run_dbt(["snapshot", "--vars", "scope_values: ['t1', 't2']"])
        assert all(r.status == "success" for r in results)
        assert len(self._all_rows(project, 1)) == 1
        assert len(self._all_rows(project, 3)) == 1

        # 2. Only t1 is collected and id=2 is removed from t1's source.
        project.run_sql(source_only_t1_sql.format(schema=project.test_schema))
        project.run_sql(
            "DELETE FROM {schema}.seed WHERE id = 2".format(schema=project.test_schema)
        )
        results = run_dbt(["snapshot", "--vars", "scope_values: ['t1']"])
        assert all(r.status == "success" for r in results)

        # id=2 is inside the declared scope and missing from the source:
        # its open record must be closed (invalidate) / marked deleted.
        rows_id2 = self._all_rows(project, 2)
        assert len(rows_id2) == 1
        assert len(self._open_rows(project, 2)) == 0

        # t2 rows are outside the declared scope: even though the source no
        # longer contains them, their open records stay untouched.
        assert len(self._open_rows(project, 3)) == 1
        assert len(self._open_rows(project, 4)) == 1
        assert len(self._all_rows(project, 3)) == 1
        assert len(self._all_rows(project, 4)) == 1

        if self.hard_deletes_mode == "new_record":
            deleted_rows = project.run_sql(
                "select * from scoped_snapshot where id = 2 and dbt_is_deleted = 'True'",
                fetch="all",
            )
            assert len(deleted_rows) == 1

        # 3. The deleted row re-appears in the source.
        project.run_sql(reinsert_row_sql.format(schema=project.test_schema))
        results = run_dbt(["snapshot", "--vars", "scope_values: ['t1']"])
        assert all(r.status == "success" for r in results)

        rows_id2 = self._all_rows(project, 2)
        if self.hard_deletes_mode == "invalidate":
            # re-appearance opens a brand new version
            assert len(rows_id2) == 2
            assert len(self._open_rows(project, 2)) == 1
        else:
            # new_record: one closed deleted row + one new open live row
            assert len(rows_id2) == 2
            open_rows = self._open_rows(project, 2)
            assert len(open_rows) == 1
            open_flag = project.run_sql(
                "select dbt_is_deleted from scoped_snapshot "
                "where id = 2 and dbt_valid_to is null",
                fetch="one",
            )
            assert open_flag[0] == "False"

        # t2 remains fully untouched.
        assert len(self._open_rows(project, 3)) == 1
        assert len(self._open_rows(project, 4)) == 1

        # 4. Empty source inside the declared scope: deletion is still
        # detected for the remaining t1 row (id=1).
        project.run_sql(source_empty_t1_sql.format(schema=project.test_schema))
        results = run_dbt(["snapshot", "--vars", "scope_values: ['t1']"])
        assert all(r.status == "success" for r in results)
        assert len(self._open_rows(project, 1)) == 0
        # out-of-scope tenant still open
        assert len(self._open_rows(project, 3)) == 1
        assert len(self._open_rows(project, 4)) == 1

    def test_undeclared_scope_value_fails_before_changes(self, project):
        results = run_dbt(["seed"])
        assert len(results) == 1
        results = run_dbt(["snapshot", "--vars", "scope_values: ['t1', 't2']"])
        assert all(r.status == "success" for r in results)

        project.run_sql(source_only_t1_sql.format(schema=project.test_schema))
        project.run_sql(source_with_undeclared_sql.format(schema=project.test_schema))

        with pytest.raises(DbtRuntimeError, match="authoritative scope"):
            run_dbt(["snapshot", "--vars", "scope_values: ['t1']"])

        # Nothing changed: every original row still has an open version.
        for id_value in (1, 2, 3, 4):
            assert len(self._open_rows(project, id_value)) == 1

    def test_empty_scope_values_fails(self, project):
        run_dbt(["seed"])
        with pytest.raises(DbtRuntimeError, match="must not be empty"):
            run_dbt(["snapshot", "--vars", "scope_values: []"])


class TestScopedSnapshotInvalidate(ScopedSnapshotBase):
    snapshot_sql = invalidate_snapshot_sql
    hard_deletes_mode = "invalidate"


class TestScopedSnapshotNewRecord(ScopedSnapshotBase):
    snapshot_sql = new_record_snapshot_sql
    hard_deletes_mode = "new_record"


class TestScopedSnapshotIncompleteConfig:
    @pytest.fixture(scope="class")
    @classmethod
    def seeds(cls):
        return {"seed.csv": seed_csv}

    @pytest.fixture(scope="class")
    @classmethod
    def snapshots(cls):
        return {"scoped_snapshot.sql": incomplete_snapshot_sql}

    def test_missing_scope_column_fails(self, project):
        run_dbt(["seed"])
        with pytest.raises(DbtRuntimeError, match="must be set together"):
            run_dbt(["snapshot", "--vars", "scope_values: ['t1']"])
