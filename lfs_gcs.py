from functools import cache
from typing import Optional

from google.cloud.storage import Client, Blob
from os.path import abspath, join
from subprocess import run, PIPE
from logging import getLogger, basicConfig, DEBUG
from json import loads, dumps
from sys import stdin
from os import environ

logger = getLogger("lfs_gcs")
MB = 1024 * 1024
CHUNK_SIZE = 4 * MB


class GCSTransport:
    @staticmethod
    def create():
        url = config_option("lfs.url")
        project = config_option("lfs-gcs.project")
        if not url:
            raise RuntimeError("LFS url must be set")
        logger.debug("Initializing lfs_gcs for project %s, url %s", project, url)
        return GCSTransport(url, project)

    @cache
    def client(self):
        return Client(project=self.project)

    def __init__(self, url: str, project: Optional[str]):
        self.url = url
        self.project = project

    def download(self, oid: str, **_):
        file_uri = join(self.url, oid)
        download_path = abspath(f"lfs-download-{oid}.tmp")
        logger.debug("Downloading %s to %s", file_uri, download_path)
        with Blob.from_uri(file_uri, client=self.client()).open("rb") as src:
            with open(download_path, "wb") as dst:
                yield from transfer(oid, src, dst)
        yield {"event": "complete", "oid": oid, "path": download_path}

    def upload(self, oid: str, path: str, size: int, action: type(None)):
        file_uri = join(self.url, oid)
        logger.debug("Uploading %s", file_uri)
        with open(path, "rb") as src:
            with Blob.from_uri(file_uri, client=self.client()).open("wb") as dst:
                yield from transfer(oid, src, dst)
        yield {"event": "complete", "oid": oid}


def main():
    if "LFS_GCS_LOG" in environ:
        basicConfig(filename=environ["LFS_GCS_LOG"], level=DEBUG)

    first_message = loads(stdin.readline())
    assert first_message["event"] == "init"
    client = GCSTransport.create()
    print({}, flush=True)

    for line in stdin:
        request = loads(line)
        event = request.pop("event")
        if event == "terminate":
            return
        else:
            oid = request.pop("oid")
            try:
                for response in getattr(client, event)(oid=oid, **request):
                    respond(response)
            except Exception as e:
                logger.exception("Transfer failed")
                error = {"code": 2, "message": str(e)}
                respond({"event": "complete", "oid": oid, "error": error})


def transfer(oid: str, src, dst):
    total = 0
    for chunk in iter(lambda: src.read(CHUNK_SIZE), b""):
        dst.write(chunk)
        total += len(chunk)
        yield {
            "event": "progress",
            "oid": oid,
            "bytesSoFar": total,
            "bytesSinceLast": len(chunk),
        }


def respond(message: dict):
    print(dumps(message), flush=True)


def config_option(option: str) -> Optional[str]:
    git_config = ("git", "config", option)
    lfs_config = ("git", "config", "--file", ".lfsconfig", option)
    for cmd in git_config, lfs_config:
        value = run(cmd, stdout=PIPE).stdout.strip().decode()
        if value:
            return value


if __name__ == "__main__":
    exit(main())
