from argparse import ArgumentParser
from functools import cache
from contextlib import contextmanager
from pathlib import Path
from typing import Optional, Generator

from google.cloud.storage import Client, Blob
from os.path import join
from subprocess import run, PIPE, check_call
from logging import getLogger, basicConfig, DEBUG
from json import loads, dumps
from sys import stdin, argv
from os import environ

logger = getLogger("git_lfs_gcs")
MB = 1024 * 1024
CHUNK_SIZE = 4 * MB


class GCSTransport:
    @staticmethod
    def create() -> "GCSTransport":
        url = config_option("lfs.url")
        if not url:
            raise RuntimeError("LFS url must be set")
        logger.debug("Initializing lfs_gcs for url %s", url)
        return GCSTransport(url)

    @cache
    def client(self) -> Client:
        return Client(project=None)

    def __init__(self, url: str):
        self.url = url

    def download(self, oid: str, size: int, action=None) -> Generator[dict, None, None]:
        del size, action
        file_uri = join(self.url, oid)
        download_path = Path(f"lfs-download-{oid}.tmp").absolute()
        logger.debug("Downloading %s to %s", file_uri, download_path)
        with Blob.from_uri(file_uri, client=self.client()).open("rb") as src:
            with open_auto_cleanup(download_path) as dst:
                yield from transfer(oid, src, dst)
        yield {"event": "complete", "oid": oid, "path": str(download_path)}

    def upload(
        self, oid: str, path: str, size: int, action=None
    ) -> Generator[dict, None, None]:
        del size, action
        file_uri = join(self.url, oid)
        logger.debug("Uploading %s", file_uri)
        with open(path, "rb") as src:
            with Blob.from_uri(file_uri, client=self.client()).open("wb") as dst:
                yield from transfer(oid, src, dst)
        yield {"event": "complete", "oid": oid}


def main():
    parser = ArgumentParser(description="git lfs google storage transport")
    commands = parser.add_subparsers(title="subcommands", dest="command")
    install_command = commands.add_parser("install")
    install_command.add_argument(
        "--local",
        dest="local",
        action="store_true",
        help="install in repository config instead of ~/.gitconfig",
    )

    args = parser.parse_args()
    if args.command == "install":
        install(args.local)
    else:
        return agent()


def install(is_local: bool):
    lfs_install = ("git", "lfs", "install") + (("--local",) if is_local else ())
    git_config = ("git", "config") + (() if is_local else ("--global",))
    check_call(lfs_install)
    check_call(git_config + ("lfs.standalonetransferagent", "git-lfs-gcs"))
    check_call(git_config + ("lfs.customtransfer.git-lfs-gcs.path", argv[0]))
    print("Git LFS GCS transport initialized.")


def agent():
    if "GIT_LFS_GCS_LOG" in environ:
        basicConfig(filename=environ["LFS_GCS_LOG"], level=DEBUG)

    first_request = loads(stdin.readline())
    assert first_request["event"] == "init"
    client = GCSTransport.create()
    respond({})

    for request in (loads(line) for line in stdin):
        event = request.pop("event")
        if event == "terminate":
            break
        oid = request.pop("oid")
        try:
            for response in getattr(client, event)(oid=oid, **request):
                respond(response)
        except Exception as e:
            logger.exception("Transfer failed")
            error = {"code": 2, "message": str(e)}
            respond({"event": "complete", "oid": oid, "error": error})


def transfer(oid: str, src, dst) -> Generator[dict, None, None]:
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


@contextmanager
def open_auto_cleanup(file_path: Path):
    try:
        with file_path.open("wb") as file:
            yield file
    except Exception:
        file_path.unlink()
        raise


if __name__ == "__main__":
    exit(main())
