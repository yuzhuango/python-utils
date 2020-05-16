import os
from datetime import datetime

import ftplib
import ftputil
import ftputil.session
import paramiko
from dateutil.parser import parse

from abc import ABCMeta, abstractmethod


class AbstractFTPClient(metaclass=ABCMeta):
    """ftp/sftp/... client(file transfer client) abstract class
        抽象方法需要全部实现，其它方法，子类自行扩展。"""

    @abstractmethod
    def cd(self, path) -> None:
        """change directory"""
        pass

    @abstractmethod
    def pwd(self) -> str:
        """return current path"""
        pass

    @abstractmethod
    def ls(self, path='.') -> list:
        """list files"""
        pass

    @abstractmethod
    def get(self, remote, local) -> None:
        """download remote file to local"""
        pass

    @abstractmethod
    def put(self, local, remote) -> None:
        """upload local file to remote"""
        pass

    @abstractmethod
    def put_r(self, local, remote) -> None:
        """recursive put, mkdir if directory does not exist"""
        pass

    @abstractmethod
    def getsize(self, path) -> int:
        """get file size"""
        pass

    @abstractmethod
    def getmtime(self, path) -> datetime:
        """get file recently modify time"""
        pass

    @abstractmethod
    def close(self) -> None:
        pass


class FTPClient(AbstractFTPClient):
    """ base on ftputil """
    def __init__(self, ip, user, password, port=21, mode='passive'):
        my_session_factory = ftputil.session.session_factory(port=port, use_passive_mode=None)
        self._ftp = ftputil.FTPHost(ip, user, password, session_factory=my_session_factory)

    def cd(self, path):
        self._ftp.chdir(path)

    def pwd(self):
        return self._ftp.getcwd()

    def ls(self, path='.'):
        return self._ftp.listdir(path)

    def get(self, remote, local):
        self._ftp.download(remote, local)

    def put(self, local, remote):
        self._ftp.upload(self, local, remote)

    def put_r(self, local, remote):
        try:
            self.put(local, remote)
            print("put: {}".format(local))
        except:
            parent = local
            dirs = []
            while parent != '/':
                parent = os.path.dirname(parent)
                dirs.append(parent)
            for d in reversed(dirs):
                try:
                    self._ftp.mkdir(d)
                    print("mkdir: {}".format(d))
                except:
                    pass
            self.put(local, remote)
            print("put after mkdir: {}".format(local))

    def getsize(self, path):
        return self._ftp.path.getsize(path)

    def getmtime(self, path):
        return datetime.fromtimestamp(self._ftp.path.getmtime(path))

    def close(self):
        self._ftp.close()


class SFTPClient(AbstractFTPClient):
    """ base on paramiko """
    def __init__(self, ip, user, password, port=22, mode='passive'):
        t = paramiko.Transport(sock=(ip, port))
        t.connect(username=user, password=password)
        self._ftp = paramiko.SFTPClient.from_transport(t)

    def cd(self, path):
        self._ftp.chdir(path)

    def pwd(self):
        return self._ftp.getcwd()

    def ls(self, path='.'):
        return self._ftp.listdir(path)

    def get(self, remote, local):
        self._ftp.get(remote, local)

    def put(self, local, remote):
        self._ftp.put(local, remote)

    def put_r(self, local, remote):
        try:
            self.put(local, remote)
        except:
            parent = local
            dirs = []
            while parent != '/':
                parent = os.path.dirname(parent)
                dirs.append(parent)
            for d in reversed(dirs):
                try:
                    self._ftp.mkdir(d)
                except:
                    pass
            self.put(local, remote)

    def getsize(self, path):
        return self._ftp.stat(path).st_size

    def getmtime(self, path):
        return datetime.fromtimestamp(self._ftp.stat(path).st_mtime)

    def close(self):
        self._ftp.close()


class TestFTPClient(AbstractFTPClient):
    """ base on  python library ftplib
        使用模块中set_pasv方法， 测试ftp mode的问题"""
    def __init__(self, ip, user, password, port=21, mode='passive'):
        self._ftp = ftplib.FTP()
        self._ftp.connect(host=ip, port=port, timeout=None)
        self._ftp.login(user=user, passwd=password)
        if mode != 'passive': self._ftp.set_pasv(False)

    def cd(self, path):
        self._ftp.cwd(path)

    def pwd(self):
        return self._ftp.pwd()

    def ls(self, path='.'):
        return self._ftp.nlst(path)

    def get(self, remote, local):
        with open(local, "wb") as fp:
            self._ftp.retrbinary('RETR {}'.format(remote), fp.write, 8192)

    def put(self, local, remote):
        with open(local, "rb") as fp:
            self._ftp.storbinary("STOR {}".format(remote), fp, 8192)

    def put_r(self, local, remote):
        try:
            self.put(local, remote)
            print("put: {}".format(local))
        except:
            parent = local
            dirs = []
            while parent != '/':
                parent = os.path.dirname(parent)
                dirs.append(parent)
            for d in reversed(dirs):
                try:
                    self._ftp.mkd(d)
                except:
                    pass
            self.put(local, remote)

    def getsize(self, path):
        return self._ftp.size(path)

    def getmtime(self, path):
        return parse(self._ftp.voidcmd("MDTM {}".format(path))[4:].strip())

    def close(self):
        self._ftp.close()
