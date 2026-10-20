"""Prints a Markdown summary of a JUnit XML report, for a GitHub job summary.

Usage: junit-summary.py TITLE REPORT

Exits with status 1 when REPORT does not exist, after it prints a line that
says so.

GitHub drops a step summary larger than 1 MiB, so the summary lists at most
MAX_FAILURES failures, and clips each failure's text to MAX_DETAIL characters.
A failure's text can hold a backtrace, and a run where a shared service is down
fails hundreds of tests.
"""

import re
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

SLOWEST = 10
STDERR_LINES = 20
MAX_FAILURES = 30
MAX_DETAIL = 4000


def seconds(element: ET.Element) -> float:
    return float(element.get("time") or 0)


def failure_detail(case: ET.Element, element: ET.Element) -> str:
    """Returns the message of a failure or error element.

    nextest writes no message for a test that timed out or exited without a
    panic, only a `type` such as "test timeout". For those, this adds the end
    of the test's stderr.
    """
    message = (element.text or element.get("message") or "").strip()
    if message:
        # A panic message starts with the panic, so this keeps the start.
        if len(message) > MAX_DETAIL:
            return f"{message[:MAX_DETAIL]}\n[{len(message) - MAX_DETAIL} more characters in junit.xml]"
        return message
    stderr = (case.findtext("system-err") or "").strip().splitlines()
    lines = [element.get("type") or "failed without a message"]
    if stderr:
        tail = "\n".join(stderr[-STDERR_LINES:])
        # The end of stderr is closest to the failure, so this keeps the end.
        if len(tail) > MAX_DETAIL:
            tail = f"[{len(tail) - MAX_DETAIL} earlier characters in junit.xml]\n{tail[-MAX_DETAIL:]}"
        lines += ["", f"Last {min(len(stderr), STDERR_LINES)} lines of stderr:", tail]
    return "\n".join(lines)


def fence(text: str) -> str:
    """Returns a code fence longer than any run of backticks in `text`."""
    longest = max((len(run) for run in re.findall("`+", text)), default=0)
    return "`" * max(3, longest + 1)


def cell(text: str) -> str:
    """Escapes `|`, which ends a Markdown table cell."""
    return text.replace("|", "\\|")


def duration(total: float) -> str:
    minutes, rest = divmod(total, 60)
    return f"{int(minutes)}m {rest:.1f}s" if minutes else f"{rest:.1f}s"


def main() -> int:
    title, report = sys.argv[1], Path(sys.argv[2])
    print(f"## {title}\n")
    if not report.exists():
        print(f"The test run did not write a report to `{report}`.")
        return 1

    root = ET.parse(report).getroot()
    rows = []
    failures = []
    timings = []
    for suite in root.iter("testsuite"):
        suite_name = suite.get("name", "")
        passed = failed = skipped = 0
        suite_time = 0.0
        for case in suite.iter("testcase"):
            case_time = seconds(case)
            suite_time += case_time
            timings.append((suite_name, case.get("name", ""), case_time))
            failure = case.find("failure")
            if failure is None:
                failure = case.find("error")
            if failure is not None:
                failed += 1
                failures.append((suite_name, case.get("name", ""), failure_detail(case, failure)))
            elif case.find("skipped") is not None:
                skipped += 1
            else:
                passed += 1
        rows.append((suite_name, passed, failed, skipped, suite_time))

    total_passed = sum(row[1] for row in rows)
    total_failed = sum(row[2] for row in rows)
    total_skipped = sum(row[3] for row in rows)
    # nextest runs tests in parallel and writes the run's wall-clock time on
    # the root element, so the test times add up to more than that.
    wall = f" in {duration(seconds(root))}" if root.get("time") else ""
    print(f"{total_passed} passed, {total_failed} failed, {total_skipped} skipped{wall}.\n")
    print("| Suite | Passed | Failed | Skipped | Test time |")
    print("| --- | --- | --- | --- | --- |")
    for suite_name, passed, failed, skipped, suite_time in rows:
        print(f"| {cell(suite_name)} | {passed} | {failed} | {skipped} | {duration(suite_time)} |")

    print("\n### Slowest Tests\n")
    print("| Suite | Test | Time |")
    print("| --- | --- | --- |")
    for suite_name, case_name, case_time in sorted(timings, key=lambda t: -t[2])[:SLOWEST]:
        print(f"| {cell(suite_name)} | {cell(case_name)} | {duration(case_time)} |")

    if failures:
        print("\n### Failed Tests\n")
        for suite_name, case_name, detail in failures[:MAX_FAILURES]:
            print(f"**{suite_name}**: {case_name}\n")
            print(f"{fence(detail)}text\n{detail}\n{fence(detail)}\n")
        if len(failures) > MAX_FAILURES:
            print(f"{len(failures) - MAX_FAILURES} more failed tests are in junit.xml.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
