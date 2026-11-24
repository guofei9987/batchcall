"""Streaming JSONL access and single-writer persistence."""
import json
from pathlib import Path


def _record(raw, line_no):
    try:
        obj = json.loads(raw)
    except (ValueError, UnicodeError) as exc:
        raise ValueError(f'Invalid JSONL at line {line_no}') from exc
    return _validate(obj, line_no)


def _validate(obj, line_no):
    if (not isinstance(obj, dict) or not isinstance(obj.get('id'), str)
            or not isinstance(obj.get('output'), dict)
            or not isinstance(obj['output'].get('status'), str)):
        raise ValueError(f'Invalid batchcall record at line {line_no}')
    return obj


def iter_results(path):
    """Yield every record (including failures/retries), without changing the file."""
    with Path(path).open('rb') as f:
        for i, raw in enumerate(f, 1):
            yield _record(raw, i)


def load_success_ids(path):
    """Return IDs with at least one successful record; missing file means empty."""
    try:
        return {r['id'] for r in iter_results(path) if r['output']['status'] == 'success'}
    except FileNotFoundError:
        return set()


def prepare(path, resume, logger):
    """Scan once, optionally collect success IDs, repair only an incomplete tail."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    ids = set()
    with path.open('a+b') as f:
        f.seek(0)
        line_no = 0
        while True:
            offset = f.tell()
            raw = f.readline()
            if not raw:
                break
            line_no += 1
            try:
                # An unterminated, syntactically broken final line is recoverable.
                obj = json.loads(raw)
            except (ValueError, UnicodeError) as exc:
                if not raw.endswith(b'\n'):
                    f.truncate(offset)
                    logger.warning('Truncated incomplete JSONL tail: %d bytes', len(raw))
                    break
                raise ValueError(f'Invalid JSONL at line {line_no}') from exc
            obj = _validate(obj, line_no)
            if resume and obj['output']['status'] == 'success':
                ids.add(obj['id'])
            if not raw.endswith(b'\n'):
                f.write(b'\n')
                logger.warning('Added missing final JSONL newline')
                break
        f.flush()
    return ids


class ResultWriter:
    def __init__(self, file, batch_size):
        self.file = file
        self.batch_size = batch_size
        self.buffer = []
        self.persisted = 0
        self.broken = False

    def append(self, record):
        self.buffer.append(json.dumps(record, ensure_ascii=True, allow_nan=False) + '\n')
        if len(self.buffer) >= self.batch_size:
            self.flush()

    def flush(self):
        if self.buffer and not self.broken:
            try:
                self.file.writelines(self.buffer)
                self.file.flush()
            except BaseException:
                # Never retry a possibly partial write in the same run.
                self.broken = True
                raise
            self.persisted += len(self.buffer)
            self.buffer.clear()
