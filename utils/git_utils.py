import json

def get_changelog():
    with open("changelog.json", "r") as f:
        data = json.load(f)

    # The first key in a JSON dict is the latest version
    latest_version = list(data.keys())[0]
    return latest_version, data[latest_version]