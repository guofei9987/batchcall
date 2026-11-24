"""Bounded, thread-based batch scheduling with per-caller adaptive concurrency."""
import logging
import math
import time
from collections import deque
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import dataclass, field
from pathlib import Path
from threading import Lock
from uuid import uuid4

from .config import LogConfig, RetryConfig, StorageConfig, positive
from .storage import ResultWriter, prepare


@dataclass
class _CallerState:
    caller: object
    name: str
    current: int
    maximum: int
    adaptive: bool
    every: int
    active: int = 0
    count: int = 0
    limited: int = 0
    starts: deque = field(default_factory=deque)
    lock: Lock = field(default_factory=Lock)

    @classmethod
    def from_caller(cls, caller, index):
        if not callable(caller):
            raise TypeError('Each caller must be callable')
        name = getattr(caller, 'name', f'caller_{index}')
        initial = getattr(caller, 'concurrency', 10)
        adaptive = getattr(caller, 'adaptive', False)
        maximum = getattr(caller, 'max_concurrency', 50)
        every = getattr(caller, 'adjust_every_n', 100)
        positive('concurrency', initial)
        positive('adjust_every_n', every)
        if type(adaptive) is not bool:
            raise ValueError('adaptive must be a bool')
        if adaptive:
            positive('max_concurrency', maximum)
            if maximum < initial:
                raise ValueError('max_concurrency must be >= concurrency in adaptive mode')
        if not isinstance(name, str) or not name:
            raise ValueError('caller name must be a nonempty string')
        return cls(caller, name, initial, maximum if adaptive else initial, adaptive, every)

    def _prune(self, now):
        while self.starts and self.starts[0] <= now - 60:
            self.starts.popleft()

    def started(self):
        with self.lock:
            now = time.monotonic()
            self._prune(now)
            self.starts.append(now)

    def rpm(self, now):
        with self.lock:
            self._prune(now)
            return sum(t <= now for t in self.starts)

    def completed(self, status, logger):
        if not self.adaptive:
            return
        self.count += 1
        self.limited += status == 'rate_limited'
        if self.count == self.every:
            ratio = self.limited / self.count
            old = self.current
            if ratio <= 0.01:
                self.current = min(self.maximum, old + 1)
            elif ratio > 0.05:
                self.current = max(1, old // 2)
            if old != self.current:
                logger.info('ADJUST caller=%s 429_rate=%.2f%% concurrency=%d -> %d',
                            self.name, ratio * 100, old, self.current)
            self.count = self.limited = 0


def _invoke(state, task_id, payload, round_no):
    state.started()
    start = time.monotonic()
    try:
        result = state.caller(payload)
        elapsed = time.monotonic() - start
        if not isinstance(result, dict):
            raise TypeError('caller must return a dict')
        status, output = result['status'], result['output']
        error = result.get('error')
        elapsed = result.get('duration_s', elapsed)
        if not isinstance(status, str) or not status:
            raise ValueError('status must be a nonempty string')
        if output is not None and not isinstance(output, str):
            raise TypeError('output must be a string or None')
        if status == 'success' and output is None:
            raise ValueError('successful output must be a string')
        if error is not None and not isinstance(error, str):
            raise TypeError('error must be a string or None')
        if (type(elapsed) not in (int, float) or not math.isfinite(elapsed)
                or elapsed < 0):
            raise ValueError('duration_s must be a finite nonnegative number')
    except Exception as exc:
        status, output = 'exception', None
        error = f'{type(exc).__name__}: {exc}'
        elapsed = time.monotonic() - start
    return {'id': task_id, 'input': payload,
            'output': {'response': output, 'status': status, 'elapsed': round(elapsed, 3)},
            'error_msg': error, 'caller_name': state.name, 'round': round_no}


def _logger(config):
    logger = logging.Logger('batchcall.' + uuid4().hex)
    logger.propagate = False
    logger.setLevel(logging.INFO)
    try:
        if config.console:
            logger.addHandler(logging.StreamHandler())
        if config.log_path is not None:
            path = Path(config.log_path)
            path.parent.mkdir(parents=True, exist_ok=True)
            logger.addHandler(logging.FileHandler(path, encoding='utf-8'))
        if not logger.handlers:
            logger.addHandler(logging.NullHandler())
        formatter = logging.Formatter('[%(asctime)s] %(levelname)s | %(message)s', '%Y-%m-%d %H:%M:%S')
        for handler in logger.handlers:
            handler.setFormatter(formatter)
        return logger
    except BaseException:
        _close_logger(logger)
        raise


def _close_logger(logger):
    for handler in list(logger.handlers):
        logger.removeHandler(handler)
        handler.close()


@dataclass
class _Stats:
    attempts: int = 0
    success: int = 0
    failed: int = 0
    limited: int = 0
    duration_sum: float = 0.0


def _report(logger, states, stats, writer, round_no, done, total):
    now = time.monotonic()
    rpms = [s.rpm(now) for s in states]
    logger.info('round=%d progress=%d/%d rpm=%d concurrency=%d active=%d '
                'success=%d failed=%d 429=%d attempts=%d avg_elapsed=%.3fs persisted=%d',
                round_no, done, total, sum(rpms), sum(s.current for s in states),
                sum(s.active for s in states), stats.success, stats.failed,
                stats.limited, stats.attempts, stats.duration_sum / max(1, stats.attempts),
                writer.persisted)
    for s, rpm in zip(states, rpms):
        logger.info('caller=%s rpm=%d concurrency=%d active=%d', s.name, rpm, s.current, s.active)


def _run_round(pool, data, ids, states, writer, stats, round_no, log_every, logger):
    pending = iter(ids)
    exhausted = False
    futures = {}
    failures = []
    cursor = 0
    done_count = 0
    logger.info('ROUND %d start: tasks=%d', round_no, len(ids))
    while True:
        # Fair round robin over currently available slots; no unbounded submission.
        while not exhausted:
            chosen = None
            for step in range(len(states)):
                i = (cursor + step) % len(states)
                if states[i].active < states[i].current:
                    chosen = i
                    break
            if chosen is None:
                break
            try:
                task_id = next(pending)
            except StopIteration:
                exhausted = True
                break
            state = states[chosen]
            cursor = (chosen + 1) % len(states)
            future = pool.submit(_invoke, state, task_id, data[task_id], round_no)
            futures[future] = state
            state.active += 1
        if not futures:
            break
        completed, _ = wait(futures, return_when=FIRST_COMPLETED)
        for future in completed:
            state = futures.pop(future)
            state.active -= 1
            record = future.result()
            status = record['output']['status']
            writer.append(record)
            stats.attempts += 1
            done_count += 1
            stats.duration_sum += record['output']['elapsed']
            stats.limited += status == 'rate_limited'
            if status == 'success':
                stats.success += 1
                if round_no:
                    stats.failed -= 1
            else:
                failures.append(record['id'])
                if not round_no:
                    stats.failed += 1
            state.completed(status, logger)
            if stats.attempts % log_every == 0:
                _report(logger, states, stats, writer, round_no, done_count, len(ids))
    writer.flush()
    logger.info('ROUND %d end: success=%d failed=%d', round_no, len(ids)-len(failures), len(failures))
    _report(logger, states, stats, writer, round_no, done_count, len(ids))
    return failures


def run(data, callers, *, storage_config=None, retry_config=None, log_config=None):
    """Run a mapping of ID -> JSON string; return summary, not all results.

    A caller is a thread-safe callable, or a list/tuple of distinct callables.
    Optional caller attributes configure independent concurrency. No active RPM cap.
    """
    start = time.monotonic()
    storage = storage_config if storage_config is not None else StorageConfig()
    retry = retry_config if retry_config is not None else RetryConfig()
    logs = log_config if log_config is not None else LogConfig()
    for config, cls in ((storage, StorageConfig), (retry, RetryConfig), (logs, LogConfig)):
        if not isinstance(config, cls):
            raise TypeError(f'expected {cls.__name__}')
    if not isinstance(data, dict) or any(not isinstance(k, str) or not isinstance(v, str) for k, v in data.items()):
        raise TypeError('data must be a dict[str, str]')
    callers = list(callers) if isinstance(callers, (list, tuple)) else [callers]
    if not callers or len({id(c) for c in callers}) != len(callers):
        raise ValueError('provide at least one caller; do not repeat the same object')
    states = [_CallerState.from_caller(c, i) for i, c in enumerate(callers)]
    if len({s.name for s in states}) != len(states):
        raise ValueError('caller names must be unique')
    path = Path(storage.output_path)
    if logs.log_path is not None:
        log_path = Path(logs.log_path)
        if path.resolve() == log_path.resolve() or (path.exists() and log_path.exists() and path.samefile(log_path)):
            raise ValueError('log_path and output_path must be different files')
    logger = _logger(logs)
    stats = _Stats()
    try:
        success_ids = prepare(path, storage.resume, logger)
        ids = [key for key in data if key not in success_ids]
        del success_ids
        skipped = len(data) - len(ids)
        logger.info('START total=%d skipped=%d pending=%d callers=%d', len(data), skipped, len(ids), len(states))
        with path.open('a', encoding='utf-8', newline='\n') as file:
            writer = ResultWriter(file, storage.save_every_n)
            try:
                if ids:
                    with ThreadPoolExecutor(max_workers=min(len(ids), sum(s.maximum for s in states))) as pool:
                        for round_no in range(retry.max_retries + 1):
                            ids = _run_round(pool, data, ids, states, writer, stats, round_no, logs.log_every_n, logger)
                            if not ids:
                                break
            finally:
                writer.flush()
        summary = {'total': len(data), 'skipped': skipped, 'executed': len(data)-skipped,
                   'success': stats.success, 'failed': stats.failed, 'attempts': stats.attempts,
                   'rate_limited': stats.limited, 'elapsed': round(time.monotonic()-start, 3),
                   'output_path': str(path)}
        logger.info('FINISH %s', summary)
        return summary
    finally:
        _close_logger(logger)
