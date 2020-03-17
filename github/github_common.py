import sys
import pygit2

sys.path.append("..")
import common.settings as settings


def commit(repo, message, branch="master"):
    index = repo.index
    index.add_all()
    index.write()
    signature = pygit2.Signature(settings.github_user, settings.github_user)
    tree = index.write_tree()
    oid = repo.create_commit(f"refs/heads/{branch}", signature, signature, message, tree, [repo.head.peel().hex])
    

def push(repo, branch="master"):
    remote = repo.remotes["origin"]
    remote.credentials = pygit2.UserPass(settings.github_user, settings.github_token)
    remote.push([f"refs/heads/{branch}"], callbacks=pygit2.RemoteCallbacks(credentials=remote.credentials))