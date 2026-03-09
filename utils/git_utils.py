import json

def parse_version(v):
    v = v.lstrip("v")
    major, minor, patch = map(int, v.split("."))
    return major, minor, patch

def get_changelog():
    with open("changelog.json") as f:
        data = json.load(f)

    latest_patch = data[0]
    latest_major, latest_minor_num, _ = parse_version(latest_patch["version"])
    last_minor = None

    for release in data:
        major, minor, patch = parse_version(release["version"])
        if major == latest_major and minor == latest_minor_num and patch == 0:
            last_minor = release
            break

    return latest_patch, last_minor

def get_changes(release):
    changes = []
    if "features" in release:
        changes.extend(f"• {c}" for c in release["features"])
    if "fixes" in release:
        changes.extend(f"• {c}" for c in release["fixes"])
    return changes