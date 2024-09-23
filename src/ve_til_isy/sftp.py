# https://medium.com/nerd-for-tech/paramiko-how-to-transfer-files-with-remote-system-sftp-servers-using-python-52d3e51d2cfa

import paramiko
from base64 import decodebytes
import os


def upload_sftp():
    server = os.getenv("FTP_SERVER")
    username = os.getenv("FTP_BRUKER")
    password = os.getenv("FTP_PASSORD")
    with paramiko.SSHClient() as ssh:
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        ssh.connect(
            server,
            username=username,
            password=password,
        )

        sftp = ssh.open_sftp()

        # For testing
        # sftp.chdir("opplasting/1")
        # sftp.put("test2.txt", "test2.txt")
    return


class SFTPServerClient:
    def __init__(self, hostname, port, username, password):
        self.__hostName = hostname
        self.__port = port
        self.__userName = username
        self.__password = password
        self.__SSH_Client = paramiko.SSHClient()

    def connect(self):
        try:
            self.__SSH_Client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            self.__SSH_Client.connect(
                hostname=self.__hostName,
                port=self.__port,
                username=self.__userName,
                password=self.__password,
                look_for_keys=False,
            )
        except Exception as excp:
            raise Exception(excp)
            return
        else:
            print(
                f"Connected to server {self.__hostName}:{self.__port} as {self.__userName}."
            )

    def disconnect(self):
        self.__SSH_Client.close()
        print(
            f"{self.__userName} is disconnected to server {self.__hostName}:{self.__port}"
        )

    def uploadFiles(self, remoteFilePath, localFilePath):
        localFilePath = os.path.join("temp", remoteFilePath)
        print(f"uploading file {localFilePath} to remote {remoteFilePath}")

        sftp_client = self.__SSH_Client.open_sftp()
        try:
            sftp_client.put(localFilePath, remoteFilePath)
        except FileNotFoundError as err:
            print(f"File {localFilePath} was not found on the local system")
        sftp_client.close()

    def executeCommand(self, command):
        stdin, stdout, stderr = self.__SSH_Client.exec_command(command)
        print(stdout.readlines())
        print(stderr.readlines())
