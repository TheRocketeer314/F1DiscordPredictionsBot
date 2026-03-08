import subprocess
    
def get_changelog():
    try:
        log = subprocess.check_output(
            ["git", "log", "--pretty=format:%s", "-n", "15"]
        ).decode()

        features = []
        fixes = []

        for commit in log.split("\n"):
            if commit.startswith("feat:"):
                features.append(commit[5:].strip())
            elif commit.startswith("fix:"):
                fixes.append(commit[4:].strip())

        return features.reverse(), fixes.reverse()

    except:
        return [], []