import argparse
import contextlib
import tomllib
from collections.abc import Generator
from pathlib import Path, PurePath, PurePosixPath

import duckdb
import paramiko
from tqdm import tqdm

conf: dict


@contextlib.contextmanager
def HomeAssistantSSHClient() -> Generator[paramiko.SSHClient, None, None]:
    global conf
    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh_client.connect(
        hostname=conf["HA_HOSTNAME"],
        username=conf["HA_USERNAME"],
        key_filename=conf["HA_SSH_KEY_FILE"],
    )

    print("[✅] connected to HomeAssistant")
    (_, stdout, _) = ssh_client.exec_command("uname -a")
    uname = stdout.read().decode()
    (_, stdout, _) = ssh_client.exec_command("echo $USER@$(hostname)")
    user_host = stdout.read().decode()
    print(f"[ℹ️ ] {user_host}[ℹ️ ]{uname}", end="")
    yield ssh_client
    print("[✅] closing connection")
    ssh_client.close()


class TqdmUpTo(tqdm):
    """Provides `update_to(n)` which uses `tqdm.update(delta_n)`."""

    def update_to(self, b=1, tsize=None, bsize=1):
        """
        b  : int, optional
            Number of blocks transferred so far [default: 1].
        bsize  : int, optional
            Size of each block (in tqdm units) [default: 1].
        tsize  : int, optional
            Total size (in tqdm units). If [default: None] remains unchanged.
        """
        if tsize is not None:
            self.total = tsize
        return self.update(b * bsize - self.n)  # also sets self.n = b * bsize


def download_database(ssh_client, db_file: PurePath, destination_folder: Path):
    print("downloading db")
    ftp_client = ssh_client.open_sftp()
    with TqdmUpTo(
        unit="B",
        unit_scale=True,
        unit_divisor=1024,
        miniters=1,
        desc="[ℹ️ ] main db        ",  # padding with
    ) as pbar:
        ftp_client.get(
            str(db_file),
            str(destination_folder / db_file.name),
            callback=pbar.update_to,
        )
    wal_file = db_file.name + "-wal"
    with TqdmUpTo(
        unit="B", unit_scale=True, unit_divisor=1024, desc="[ℹ️ ] write-ahead log"
    ) as pbar:
        ftp_client.get(
            str(db_file.parent / wal_file),
            str(destination_folder / wal_file),
            callback=pbar.update_to,
        )


def main() -> int:
    global conf

    parser = argparse.ArgumentParser(
        description="script that imports raw sensor data from homeassistant into a duckdb database",
        usage="ingest_homeassistant_db [--full_load] [--config_file=path/to/config.toml]",
    )
    parser.add_argument(
        "-f",
        "--full_load",
        help="TRUNCATES the existing raw table and reloads from staging (will delete older data)",
        action=argparse.BooleanOptionalAction,
    )
    parser.add_argument(
        "--config_file",
        type=argparse.FileType("r"),
        help="config file in TOML format",
        default=Path("./config.toml"),
    )
    args = parser.parse_args()

    with open(args.config_file, "rb") as fp:
        conf = tomllib.load(fp)

    # download db locally
    with HomeAssistantSSHClient() as ha:
        Path(conf["STAGING_FOLDER"]).mkdir(parents=True, exist_ok=True)
        download_database(
            ha, PurePosixPath(conf["HA_DB_FILE"]), Path(conf["STAGING_FOLDER"])
        )

    return 0

    # # create a connection to a file called 'file.db'
    # con = duckdb.connect("file.db")
    # # create a table and load data into it
    # con.sql("CREATE TABLE test (i INTEGER)")
    # con.sql("INSERT INTO test VALUES (42)")
    # # query the table
    # con.table("test").show()
    # # explicitly close the connection
    # con.close()
    # # Note: connections also closed implicitly when they go out of scope


if __name__ == "__main__":
    raise SystemExit(main())
