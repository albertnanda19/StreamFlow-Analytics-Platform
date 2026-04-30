import sys
import xml.etree.ElementTree as ET
from pathlib import Path


def parse_junit(xml_path: str) -> dict:
    tree = ET.parse(xml_path)
    root = tree.getroot()

    suites = root.findall("testsuite") or [root]

    results = {"unit": [], "integration": [], "e2e": [], "accuracy": [], "other": []}
    failed_tests = []
    slow_tests = []

    for suite in suites:
        for tc in suite.findall("testcase"):
            name = f"{tc.get('classname', '')}.{tc.get('name', '')}"
            duration = float(tc.get("time", 0))
            status = "passed"
            failure = tc.find("failure") or tc.find("error")
            skipped = tc.find("skipped")

            if failure is not None:
                status = "failed"
                failed_tests.append((name, failure.text or ""))
            elif skipped is not None:
                status = "skipped"

            if duration > 30:
                slow_tests.append((duration, name))

            if "unit" in name.lower() or "/unit/" in tc.get("classname", ""):
                results["unit"].append(status)
            elif "e2e" in name.lower() or "/e2e/" in tc.get("classname", ""):
                results["e2e"].append(status)
            elif "accuracy" in name.lower():
                results["accuracy"].append(status)
            elif "integration" in name.lower() or "/integration/" in tc.get("classname", ""):
                results["integration"].append(status)
            else:
                results["other"].append(status)

    slow_tests.sort(reverse=True)
    return results, failed_tests, slow_tests[:5]


def print_summary(xml_path: str) -> None:
    if not Path(xml_path).exists():
        print(f"No test results found at {xml_path}")
        print("Run: make -f tests/Makefile.test test-all")
        return

    results, failed_tests, slow_tests = parse_junit(xml_path)

    print("\n" + "=" * 53)
    print(" STREAMFLOW ANALYTICS PLATFORM — TEST SUMMARY")
    print("=" * 53)

    total_passed = 0
    total_all = 0

    for category in ("unit", "integration", "e2e", "accuracy"):
        statuses = results[category]
        if not statuses:
            continue
        passed = statuses.count("passed")
        total = len([s for s in statuses if s != "skipped"])
        skipped = statuses.count("skipped")
        total_passed += passed
        total_all += total
        icon = "✅" if passed == total and total > 0 else ("⚠️ " if passed > 0 else "❌")
        label = f"{category.upper():<22}"
        skip_note = f"({skipped} skipped)" if skipped else ""
        print(f"{label} {passed}/{total} passed  {icon}  {skip_note}")

    print("-" * 53)
    pass_rate = (total_passed / total_all * 100) if total_all > 0 else 0
    status_icon = "✅" if pass_rate == 100 else ("⚠️ " if pass_rate >= 80 else "❌")
    print(f"{'TOTAL':<22} {total_passed}/{total_all} passed  {status_icon}  ({pass_rate:.1f}% pass rate)")

    if failed_tests:
        print("\nFAILED TESTS:")
        for name, msg in failed_tests:
            print(f"  ❌ {name}")
            first_line = (msg or "").strip().split("\n")[0]
            if first_line:
                print(f"     {first_line[:100]}")

    if slow_tests:
        print("\nSLOWEST TESTS (>30s):")
        for duration, name in slow_tests:
            print(f"  ⏱ {duration:>5.0f}s  {name}")

    report_path = str(Path(xml_path).parent / "full_report.html")
    print(f"\nFull HTML report: {report_path}")
    print("=" * 53 + "\n")


if __name__ == "__main__":
    xml_path = sys.argv[1] if len(sys.argv) > 1 else "tests/reports/junit.xml"
    print_summary(xml_path)
