import contextlib
import paramiko
from tqdm import tqdm
from pathlib import Path, PurePosixPath

HA_HOSTNAME="homeassistant"
HA_USERNAME="root"
HA_SSH_KEY_FILE="C:/Users/pedro/.ssh/homeassitant_ed25519"
HA_DB_FILE="/config/home-assistant_v2.db"
STAGING_FOLDER="./staging/"

@contextlib.contextmanager
def HomeAssistantSSHClient() -> paramiko.SSHClient:
    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh_client.connect(
        hostname=HA_HOSTNAME,
        username=HA_USERNAME,
        key_filename=HA_SSH_KEY_FILE,
    )

    print("connected to HomeAssistant")
    (_, stdout, _) = ssh_client.exec_command("uname -a")
    uname = stdout.read().decode()
    (_, stdout, _) = ssh_client.exec_command("echo $USER@$(hostname)")
    user_host = stdout.read().decode()
    print(f"{user_host}{uname}", end="")
    yield ssh_client
    print("closing connection")
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

def download_database(ssh_client, db_file:Path, destination_folder:Path):
    print("downloading db")
    ftp_client = ssh_client.open_sftp()
    with TqdmUpTo(
        unit="B", unit_scale=True, unit_divisor=1024, miniters=1, desc="main db"
    ) as pbar:
        ftp_client.get(
            str(db_file),
            str(destination_folder/db_file.name),
            callback=pbar.update_to,
        )
    wal_file = db_file.name + "-wal"
    with TqdmUpTo(
        unit="B", unit_scale=True, unit_divisor=1024, desc="write-ahead log"
    ) as pbar:
        ftp_client.get(
            str(db_file.parent / wal_file),
            str(destination_folder / wal_file),
            callback=pbar.update_to,
        )




# download db locally
with HomeAssistantSSHClient() as ha:
    Path(STAGING_FOLDER).mkdir(parents=True, exist_ok=True)
    download_database(ha, PurePosixPath(HA_DB_FILE), Path(STAGING_FOLDER))