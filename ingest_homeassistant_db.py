import argparse
import contextlib
import tomllib
from collections.abc import Generator
from pathlib import Path, PurePath, PurePosixPath
from datetime import datetime
import logging

import duckdb
import paramiko
from tqdm import tqdm

conf: dict
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.propagate = False


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

    logger.info("[✅] connected to HomeAssistant")
    (_, stdout, _) = ssh_client.exec_command("uname -a")
    uname = stdout.read().decode()
    (_, stdout, _) = ssh_client.exec_command("echo $USER@$(hostname)")
    user_host = stdout.read().decode()
    logger.info(f"[ℹ️] {user_host}[ℹ️]{uname}")
    yield ssh_client
    logger.info("[✅] closing connection")
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
    logger.info("[ℹ️] downloading db")
    ftp_client = ssh_client.open_sftp()
    with TqdmUpTo(
        unit="B",
        unit_scale=True,
        unit_divisor=1024,
        miniters=1,
        desc="[ℹ️] main db        ",  # padding with
    ) as pbar:
        ftp_client.get(
            str(db_file),
            str(destination_folder / db_file.name),
            callback=pbar.update_to,
        )
    wal_file = db_file.name + "-wal"
    with TqdmUpTo(
        unit="B", unit_scale=True, unit_divisor=1024, desc="[ℹ️] write-ahead log"
    ) as pbar:
        ftp_client.get(
            str(db_file.parent / wal_file),
            str(destination_folder / wal_file),
            callback=pbar.update_to,
        )


def run_sql_query_file(
    con: duckdb.DuckDBPyConnection, file: str | Path
) -> duckdb.DuckDBPyRelation:
    with open(file, "r") as fp:
        q = fp.read()
    return con.sql(q)


def table_stats_str(result: tuple[int, datetime, datetime]) -> str:
    """returns a string from a result tuple containing count, min timestamp, max timestamp"""
    return f"count {result[0]}, min ts: {result[1].strftime("%Y-%m-%d %H:%M:%S %z")}, max ts: {result[2].strftime("%Y-%m-%d %H:%M:%S %z")}"


def main() -> int:
    global conf

    parser = argparse.ArgumentParser(
        description="script that imports raw sensor data from homeassistant into a duckdb database",
    )
    parser.add_argument(
        "-f",
        "--full_load",
        help="TRUNCATES the existing raw table and reloads from staging (will delete older data)",
        action=argparse.BooleanOptionalAction,
    )
    parser.add_argument(
        "-s",
        "--skip_download",
        help="skips downloading file from ha server",
        action=argparse.BooleanOptionalAction,
    )
    parser.add_argument(
        "-v",
        "--verbose",
        type=argparse.FileType("r"),
        help="should print output to stdout",
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

    logger.debug("loaded config: {conf}")

    fileformatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    filehandler = logging.FileHandler(conf["LOG_FILE"], encoding="utf-8")
    filehandler.setFormatter(fileformatter)
    logger.addHandler(filehandler)
    if args.verbose:
        stdoutformatter = logging.Formatter("%(message)s")
        stdouthandler = logging.StreamHandler()
        stdouthandler.setFormatter(stdoutformatter)
        logger.addHandler(stdouthandler)

    logger.info(" ----- STARTING NEW SESSION ----- ")

    # download db locally
    if not args.skip_download:
        with HomeAssistantSSHClient() as ha:
            Path(conf["STAGING_FOLDER"]).mkdir(parents=True, exist_ok=True)
            download_database(
                ha, PurePosixPath(conf["HA_DB_FILE"]), Path(conf["STAGING_FOLDER"])
            )
    else:
        logger.info("[❗] skipping downloading file from ha server")

    # create a connection to the db file
    con = duckdb.connect(conf["ANALYTICAL_DB_FILE"])
    # ensure schemas and tables are there
    run_sql_query_file(con, "queries/schemas.sql")
    logger.info("[✅] ensured schemas are present in db")

    # read staging file into duckdb database
    logger.info("[ℹ️] reading from ha database to analytical db...")
    run_sql_query_file(con, "queries/staging.home_assistant_events.sql")
    result = con.sql(
        """select 
        count(*) cnt,
        to_timestamp(min(last_updated_ts)) min_ts, 
        to_timestamp(max(last_updated_ts)) max_ts 
        from staging.home_assistant_events;""",
    ).fetchone()
    logger.info(
        f"[✅] staging success - {table_stats_str(result)}"  # pyright: ignore[reportArgumentType]
    )

    result = con.sql(
        """select 
        count(*) cnt,
        min(last_updated) min_ts, 
        max(last_updated) max_ts 
        from raw_data.events;""",
    ).fetchone()
    logger.info(
        f"[ℹ️] events stats before - {table_stats_str(result)}"  # pyright: ignore[reportArgumentType]
    )
    if args.full_load:
        logger.info("[❗] integrating data to events table - full load")
        run_sql_query_file(con, "queries/raw.events-full.sql")
        logger.info("[✅] success")
    else:
        logger.info("[ℹ️] integrating data to events table - delta load")
        run_sql_query_file(con, "queries/raw.events-delta.sql")
        logger.info("[✅] success")

    result = con.sql(
        """select 
        count(*) cnt,
        min(last_updated) min_ts, 
        max(last_updated) max_ts 
        from raw_data.events;""",
    ).fetchone()
    logger.info(
        f"[✅] events stats - {table_stats_str(result)}"  # pyright: ignore[reportArgumentType]
    )

    # explicitly close the connection
    con.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
