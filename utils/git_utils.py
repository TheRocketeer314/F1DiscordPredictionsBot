import subprocess

def get_version():
    try:
        return subprocess.check_output(
            ["git", "describe", "--tags", "--abbrev=0"]
        ).decode().strip()
    except:
        return "unknown"
    
def get_release_date():
    try:
        tag = subprocess.check_output(
            ["git", "describe", "--tags", "--abbrev=0"]
        ).decode().strip()

        date = subprocess.check_output(
            ["git", "log", "-1", "--format=%ai", tag]
        ).decode().strip().split(" ")[0]

        return date
    except:
        return "unknown"
    
def get_changes(limit=20):
    try:
        log = subprocess.check_output(
            ["git", "log", f"-{limit}", "--pretty=%s"]
        ).decode().split("\n")

        features = []
        fixes = []

        for msg in log:
            if msg.startswith("feat:"):
                features.append(msg[6:])
            elif msg.startswith("fix:"):
                fixes.append(msg[5:])

        return features, fixes

    except:
        return [], []