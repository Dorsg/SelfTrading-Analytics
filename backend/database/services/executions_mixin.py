from __future__ import annotations

from sqlalchemy.orm import Session

from database.models import RunnerExecution


class ExecutionsMixin:
    """
    RunnerExecution recording and reads. Requires:
      self.db : Session
      self._commit(msg: str, retries: int = 1) -> bool
    """

    def save_runner_execution(self, data: dict) -> RunnerExecution | None:
        """
        `data` must include user_id, runner_id and cycle_seq.
        """
        required = {"user_id", "runner_id", "cycle_seq"}
        if not required.issubset(data):
            raise ValueError("runner_execution data missing required keys")
        obj = RunnerExecution(**data)
        self.db.add(obj)
        return obj if self._commit("Insert runner execution") else None

    def get_runner_executions(self, *, user_id: int, runner_id: int, limit: int | None = None):
        q = (
            self.db.query(RunnerExecution)
            .filter(
                RunnerExecution.user_id == user_id,
                RunnerExecution.runner_id == runner_id,
            )
            .order_by(RunnerExecution.execution_time.desc())
        )
        if isinstance(limit, int) and limit > 0:
            q = q.limit(limit)
        return q.all()

    def get_last_runner_execution(
        self,
        *,
        user_id: int,
        runner_id: int,
        cycle_seq: str
    ) -> RunnerExecution | None:
        """
        Return the most recent RunnerExecution row for the given
        user/runner/cycle_seq, or None if none recorded.
        """
        return (
            self.db.query(RunnerExecution)
            .filter(
                RunnerExecution.user_id   == user_id,
                RunnerExecution.runner_id == runner_id,
                RunnerExecution.cycle_seq == cycle_seq,
            )
            .order_by(RunnerExecution.execution_time.desc())
            .first()
        )
