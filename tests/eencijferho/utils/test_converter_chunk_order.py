"""Parallel conversion must return rows in input order, whatever chunk finishes first."""

import time

from eencijferho.core import converter


def _slow_first_chunk(chunk_data):
    """Stand-in for process_chunk: the chunk holding line 0 finishes last."""
    _, chunk = chunk_data
    if chunk and chunk[0] == "0\n":
        time.sleep(0.5)
    return [line.rstrip("\n") for line in chunk]


def test_run_parallel_keeps_chunk_order_when_chunks_finish_out_of_order(monkeypatch):
    # 4 workers, 16 one-line chunks: with imap_unordered chunk 0 would come back last.
    monkeypatch.setattr(converter.mp, "cpu_count", lambda: 5)
    monkeypatch.setattr(converter, "process_chunk", _slow_first_chunk)
    lines = [f"{i}\n" for i in range(16)]

    result = converter._run_parallel(lines, positions=[])

    assert result == [str(i) for i in range(16)]
