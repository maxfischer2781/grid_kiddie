from typing import NamedTuple, Optional
import subprocess
import argparse
import math


CLI = argparse.ArgumentParser(
    description="Compile concurrency limit statements from dynamic group quotas",
)
CLI.add_argument(
    "--slots",
    type=int,
    default=100000,
    help="Number of slots to distribute across groups",
)


def escape_dots(name: str, by: str, tail: bool):
    parts = name.split(".")
    if len(parts) == 1:
        return parts[0]
    if tail:
        return parts[0] + "." + by.join(parts[1:])
    return by.join(parts[1:])


class Group(NamedTuple):
    name: str
    quota: float
    parent: "Optional[Group]"

    @property
    def absolute_quota(self):
        parent_aq = 1 if self.parent is None else self.parent.absolute_quota
        return self.quota * parent_aq


def read_groups() -> "list[Group]":
    """Read group settings from the condor config"""
    config_query = subprocess.run(
        ["condor_config_val", "-dump", "GROUP_QUOTA_DYNAMIC"],
        stdout=subprocess.PIPE,
        universal_newlines=True
    )
    head_len = len("GROUP_QUOTA_DYNAMIC") + 1
    config_groups = {}
    for line in config_query.stdout.splitlines():  # type: str
        if not line.startswith("GROUP_QUOTA_DYNAMIC") or "=" not in line:
            continue
        name, quota = line[head_len:].split("=")
        config_groups[name.strip()] = float(quota)
    groups: dict[str, Group] = {}
    # construct groups sorted from parents ("few '.'s") to children ("many '.'s")
    for name, quota in sorted(config_groups.items(), key=lambda k_v: k_v[0].count(".")):
        parent, parent_name = None, name
        while parent is None and parent_name:
            parent_name = name.rpartition(".")[0]
            parent = groups.get(parent_name, None)
        groups[name] = Group(name=name, quota=quota, parent=parent)
    return list(groups.values())


def main():
    options = CLI.parse_args()
    slots = options.slots
    digits = math.ceil(math.log(slots, 10))
    groups = read_groups()
    groups.sort(key=lambda g: g.name)
    max_name = max(len(group.name) for group in groups)
    for group in groups:
        print(
            f"# Group {group.name:{max_name}} Quota",
            f"Absolute {group.absolute_quota:.{digits}f}",
            f"Relative {group.quota}"
        )
        print(
            f"{escape_dots(group.name.lower(), '_', True)}_LIMIT",
            f"= {int(group.absolute_quota * slots)}"
        )


if __name__ == "__main__":
    main()
