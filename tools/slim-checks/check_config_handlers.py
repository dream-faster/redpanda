#!/usr/bin/env python3
"""Report topic properties that can be set at creation but not altered.

A removal pass that deletes a block of per-property handlers can take a
surviving property's handler with it: the property still parses on
CreateTopics and still appears in DescribeConfigs, so nothing looks wrong,
but AlterConfigs silently ignores it. That is what happened to
delete.retention.ms and message.timestamp.after.max.ms, and neither the
compiler nor Bazel analysis can see it -- the handlers are independent `if`
blocks, so losing one is valid code.

Every property in create_topics' supported_configs is expected in both
alter_configs.cc and incremental_alter_configs.cc. There is currently no
legitimate create-only property, so any gap is a bug.
"""

import re
import sys
from pathlib import Path

HANDLERS = Path("src/v/kafka/server/handlers")
PROP = re.compile(r"topic_property_[a-z_0-9]+")


def supported_configs():
    src = (HANDLERS / "create_topics.cc").read_text()
    m = re.search(r"supported_configs = std::to_array\(\{(.*?)\}\);", src, re.DOTALL)
    if not m:
        sys.exit("could not find supported_configs in create_topics.cc")
    return set(PROP.findall(m.group(1)))


def handled_in(name):
    return set(PROP.findall((HANDLERS / name).read_text()))


def main():
    supported = supported_configs()
    gaps = 0
    for name in ("alter_configs.cc", "incremental_alter_configs.cc"):
        for prop in sorted(supported - handled_in(name)):
            print(f"{name}: no handler for {prop}")
            gaps += 1
    print(f"creatable properties without an alter handler: {gaps}")
    return 1 if gaps else 0


if __name__ == "__main__":
    sys.exit(main())
