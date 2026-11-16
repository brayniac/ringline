#!/usr/bin/env python3
"""Mutations of the writable gate on io_uring: each is applied to the tree,
its tests are run, and the file is restored. A mutation is expected to turn
every test it names red. Run from the repository root on a Linux 6.8+ host:

    python3 experiments/send-admission-d-gate-mutations.py          # apply and test each
    python3 experiments/send-admission-d-gate-mutations.py --check  # patterns occur once

Prints one line per mutation: the tests that failed, and whether that is
what was expected."""
import pathlib, subprocess, sys

IO = "ringline/src/runtime/io.rs"
EL = "ringline/src/backend/uring/event_loop.rs"
MUTATIONS = [
    ("the gate arms no probe on a multishot poll", IO,
     "SendPoll::Multi => driver.arm_send_probe(this.conn_index),",
     "SendPoll::Multi => {}",
     ["--test writable_gate a_drained_socket_after_a_half_close_is_writable",
      "--lib a_writable_gate_on_a_multishot_connection_probes_the_socket"]),
    ("the gate resolves at once on a multishot poll", IO,
     "SendPoll::Multi => driver.arm_send_probe(this.conn_index),",
     "SendPoll::Multi => {\n                        this.stop_waiting(driver);\n                        return Poll::Ready(Ok(()));\n                    }",
     ["--test writable_gate a_socket_filled_by_whole_sends_is_not_writable",
      "--lib a_writable_gate_on_a_multishot_connection_waits_when_the_probe_finds_no_room"]),
    ("queued and waiting sends do not block the gate", IO,
     "if !state.queue.is_empty() || state.waiting_send.is_some() {",
     "if false {",
     ["--test writable_gate sends_queued_before_the_gate_are_written_first",
      "--lib a_writable_gate_waits_for_the_queued_sends",
      "--lib a_writable_gate_waits_for_a_send_waiting_for_room"]),
    ("a queued poll is dropped though a gate waits", EL,
     "if (state.waiting_send.is_none() && state.writable_task.is_none())",
     "if state.waiting_send.is_none()",
     ["--lib a_pending_poll_for_a_writable_gate_is_armed"]),
    ("a queued probe is armed with no gate", EL,
     "if state.writable_task.is_none() || state.close_submitted {",
     "if state.close_submitted {",
     ["--lib a_pending_probe_is_armed_only_while_a_gate_waits"]),
    ("the Close CQE does not wake the gate", EL,
     "        self.end_writable_wait(conn_index, Err(libc::ECANCELED));\n",
     "",
     ["--lib the_close_cqe_wakes_a_writable_gate"]),
    ("the probe's error and hangup are ignored", EL,
     "        if hangup {\n            self.end_writable_wait(conn_index, Err(writable_errno(mask)));\n        } else if pollout {\n            self.end_writable_wait(conn_index, Ok(()));\n        } else if self.driver.send_queues[conn_index as usize].send_poll == SendPoll::None {",
     "        if false {\n            self.end_writable_wait(conn_index, Err(writable_errno(mask)));\n        } else if pollout && !hangup {\n            self.end_writable_wait(conn_index, Ok(()));\n        } else if self.driver.send_queues[conn_index as usize].send_poll == SendPoll::None {",
     ["--lib a_probe_error_or_hangup_fails_the_writable_gate"]),
    ("the probe's POLLOUT does not resubmit a waiting send", EL,
     "        {\n            self.resubmit_waiting_send(conn_index, true);\n        }\n        if !self.writable_waits(conn_index) {",
     "        {\n        }\n        if !self.writable_waits(conn_index) {",
     ["--lib a_probe_pollout_resubmits_a_send_waiting_for_room"]),
]


def run(test):
    cmd = f"cargo test -p ringline {test} -- --exact --test-threads=1 --include-ignored"
    # The lib tests' paths are long; match by name instead of --exact for them.
    if test.startswith("--lib"):
        cmd = f"cargo test -p ringline {test} -- --test-threads=1"
    r = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    out = r.stdout + r.stderr
    ran = "running 0 tests" not in out
    # A mutation that does not compile proves nothing about the tests.
    compiled = "could not compile" not in out and "error[E" not in out
    return r.returncode != 0, ran and compiled, out


def main():
    check = "--check" in sys.argv
    bad = 0
    for name, path, old, new, tests in MUTATIONS:
        p = pathlib.Path(path)
        src = p.read_text()
        n = src.count(old)
        if n != 1:
            print(f"PATTERN {name}: occurs {n} times in {path}")
            bad += 1
            continue
        if check:
            print(f"ok pattern: {name}")
            continue
        p.write_text(src.replace(old, new))
        try:
            for test in tests:
                failed, ran, out = run(test)
                verdict = "red, as expected" if failed and ran else ("NOT RUN OR DID NOT COMPILE" if not ran else "GREEN: the test does not catch it")
                print(f"{name} | {test}: {verdict}")
                if not (failed and ran):
                    bad += 1
                    tail = "\n".join(out.splitlines()[-15:])
                    print(tail)
        finally:
            p.write_text(src)
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()
