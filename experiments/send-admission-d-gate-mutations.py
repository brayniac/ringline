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
    ("queued sends do not block the gate", IO,
     "if !state.queue.is_empty()\n                    || state.waiting_send.is_some()",
     "if state.waiting_send.is_some()",
     ["--test writable_gate sends_queued_before_the_gate_are_written_first",
      "--lib a_writable_gate_waits_for_the_queued_sends"]),
    # On a one-shot poll the gate would join the send's poll, which pushes
    # nothing, and its POLLOUT sends Recheck: only a send waiting on a
    # multishot poll shows the check, by the probe the gate would arm.
    ("a send waiting for room does not block the gate", IO,
     "                    || state.waiting_send.is_some()\n                    || state.resubmit_queued",
     "                    || state.resubmit_queued",
     ["--lib a_writable_gate_after_eagain_on_a_pollout_wakes_on_the_multishot_pollout"]),
    ("a send in the kernel does not block the gate after a Recheck", IO,
     "                    || (this.rechecking && state.in_flight)\n",
     "",
     ["--lib a_send_refused_on_a_writable_gate_s_poll_moves_to_the_multishot_poll",
      "--lib a_send_refused_after_a_gate_s_recheck_waits_on_a_multishot_poll",
      "--lib a_writable_gate_does_not_resolve_on_the_pollout_that_resubmits_a_send",
      "--lib a_probe_pollout_resubmits_a_send_waiting_for_room"]),
    ("a queued poll is dropped though a gate waits", EL,
     "if (state.waiting_send.is_none() && state.writable_task.is_none())",
     "if state.waiting_send.is_none()",
     ["--lib a_pending_poll_for_a_writable_gate_is_armed"]),
    ("a queued probe is armed with no gate", EL,
     "if state.writable_task.is_none() || state.close_submitted {",
     "if state.close_submitted {",
     ["--lib a_pending_probe_is_armed_only_while_a_gate_waits"]),
    ("the Close CQE does not wake the gate", EL,
     "        self.end_writable_wait(conn_index, WritableEvent::Failed(libc::ECANCELED));\n",
     "",
     ["--lib the_close_cqe_wakes_a_writable_gate"]),
    ("the probe's error and hangup are ignored", EL,
     "        if hangup {\n            self.end_writable_wait(conn_index, WritableEvent::Failed(writable_errno(mask)));\n        } else if pollout {\n            // As on the connection's poll",
     "        if false {\n            self.end_writable_wait(conn_index, WritableEvent::Failed(writable_errno(mask)));\n        } else if pollout && !hangup {\n            // As on the connection's poll",
     ["--lib a_probe_error_or_hangup_fails_the_writable_gate"]),
    ("the probe's POLLOUT does not resubmit a waiting send", EL,
     "        if resubmitted {\n            self.resubmit_waiting_send(conn_index, true);\n        }\n",
     "        if resubmitted {\n        }\n",
     ["--lib a_probe_pollout_resubmits_a_send_waiting_for_room"]),
    ("the gate resolves on the POLLOUT that resubmits a send", EL,
     "self.end_writable_wait(conn_index, WritableEvent::Recheck);",
     "self.end_writable_wait(conn_index, WritableEvent::Room);",
     ["--lib a_send_refused_on_a_writable_gate_s_poll_moves_to_the_multishot_poll"]),
    ("the gate resolves on the probe POLLOUT that resubmits a send", EL,
     "            let event = if resubmitted {\n                WritableEvent::Recheck\n",
     "            let event = if resubmitted {\n                WritableEvent::Room\n",
     ["--lib a_probe_pollout_resubmits_a_send_waiting_for_room"]),
    ("the gate stays armed on a Recheck", IO,
     "                if event == Some(WritableEvent::Recheck) {\n                    this.armed = false;\n                    this.rechecking = true;\n                }\n",
     "",
     ["--lib a_writable_gate_does_not_resolve_on_the_pollout_that_resubmits_a_send",
      "--lib a_probe_pollout_resubmits_a_send_waiting_for_room"]),
    ("a resubmit on a retry list does not block the gate", EL,
     "        } else {\n            self.driver.send_queues[conn_index as usize].resubmit_queued = true;\n        }\n",
     "        }\n",
     ["--lib a_writable_gate_waits_for_a_resubmit_on_a_retry_list"]),
    ("the copy retry drain does not wake the gate", EL,
     "            } else {\n                self.resubmit_pushed(conn_index);\n            }\n        }\n        self.driver.copy_retry_scratch.clear();",
     "            }\n        }\n        self.driver.copy_retry_scratch.clear();",
     ["--lib a_writable_gate_waits_for_a_resubmit_on_a_retry_list"]),
    ("dropping the last half does not wake the gate", IO,
     "        wake_writable_if_closing(driver, executor, conn.conn_index, conn.generation);\n",
     "",
     ["--test writable_gate a_close_by_dropping_the_connection_while_waiting"]),
    ("the wake after a resubmit sends no Recheck", EL,
     "        if pushed {\n            self.recheck_writable_if_unblocked(conn_index);",
     "        if pushed {\n            self.wake_writable_if_unblocked(conn_index);",
     ["--lib a_writable_gate_waits_for_a_send_waiting_for_room",
      "--lib a_writable_gate_after_eagain_on_a_pollout_wakes_on_the_multishot_pollout"]),
    ("the wake after a retry push sends no Recheck", EL,
     "resubmit_queued) {\n            self.recheck_writable_if_unblocked(conn_index);",
     "resubmit_queued) {\n            self.wake_writable_if_unblocked(conn_index);",
     ["--lib a_writable_gate_waits_for_a_resubmit_on_a_retry_list",
      "--lib a_writable_gate_waits_for_a_zero_copy_re_send_on_a_retry_list"]),
    ("a zero-copy re-send on a retry list does not block the gate", EL,
     "                    .push((conn_index, generation, slab_idx));\n                self.driver.send_queues[conn_index as usize].resubmit_queued = true;\n",
     "                    .push((conn_index, generation, slab_idx));\n",
     ["--lib a_writable_gate_waits_for_a_zero_copy_re_send_on_a_retry_list"]),
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
