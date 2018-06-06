import asyncio
from pathlib import Path
import re
from typing import Sequence, Union

import aiohttp
from tqdm import tqdm

from .base import BaseFunction, SyncFunctionMixin
from .config import APIConfig
from .request import Request
from .cli.pretty import ProgressReportingReader

__all__ = (
    'BaseVFolder',
    'VFolder',
)

_rx_slug = re.compile(r'^[a-zA-Z0-9]([a-zA-Z0-9._-]*[a-zA-Z0-9])?$')


class BaseVFolder(BaseFunction):
    @classmethod
    def _create(cls, name: str, *,
                config: APIConfig=None):
        assert _rx_slug.search(name) is not None
        resp = yield Request('POST', '/folders/', {
            'name': name,
        }, config=config)
        return resp.json()

    @classmethod
    def _list(cls, *, config: APIConfig=None):
        resp = yield Request('GET', '/folders/', config=config)
        return resp.json()

    @classmethod
    def _get(cls, name: str, *, config: APIConfig=None):
        return cls(name, config=config)

    def _info(self):
        resp = yield Request('GET', '/folders/{0}'.format(self.name),
                             config=self.config)
        return resp.json()

    def _delete(self):
        resp = yield Request('DELETE', '/folders/{0}'.format(self.name),
                             config=self.config)
        if resp.status == 200:
            return resp.json()

    def _upload(self, files: Sequence[Union[str, Path]],
               basedir: Union[str, Path]=None,
               show_progress: bool=False):
        fields = []
        base_path = (Path.cwd() if basedir is None
                     else Path(basedir).resolve())
        files = [Path(file).resolve() for file in files]
        total_size = 0
        for file_path in files:
            total_size += file_path.stat().st_size
        tqdm_obj = tqdm(desc='Uploading files',
                        unit='bytes', unit_scale=True,
                        total=total_size,
                        disable=not show_progress)
        with tqdm_obj:
            for file_path in files:
                try:
                    fields.append(aiohttp.web.FileField(
                        'src',
                        str(file_path.relative_to(base_path)),
                        ProgressReportingReader(str(file_path),
                                                tqdm_instance=tqdm_obj),
                        'application/octet-stream',
                        None
                    ))
                except ValueError:
                    msg = 'File "{0}" is outside of the base directory "{1}".' \
                          .format(file_path, base_path)
                    raise ValueError(msg) from None

            rqst = Request('POST', '/folders/{}/upload'.format(self.name),
                           config=self.config)
            rqst.content = fields
            resp = yield rqst
        return resp

    def _download(self, files: Sequence[Union[str, Path]],
                  show_progress: bool=False):
        resp = yield Request('GET', '/folders/{}/download'.format(self.name), {
            'files': files,
        }, config=self.config)
        total_bytes = resp.response.content.total_bytes
        tqdm_obj = tqdm(desc='Downloading files',
                        unit='bytes', unit_scale=True,
                        total=total_bytes,
                        disable=not show_progress)

        async def save_multipart_files(reader):
            with tqdm_obj as pbar:
                acc_bytes = 0
                while True:
                    part = await reader.next()
                    if part is None:
                        break
                    fp = open(part.filename, 'wb')
                    while True:
                        chunk = await part.read_chunk()  # default chunk size: 8192
                        if not chunk:
                            break
                        fp.write(chunk)
                        curr_pos = total_bytes - reader.resp.content._size
                        read_bytes = curr_pos - acc_bytes
                        acc_bytes = curr_pos
                        pbar.update(read_bytes)
                    fp.close()
                pbar.update(total_bytes - curr_pos)

        loop = asyncio.get_event_loop()
        reader = aiohttp.MultipartReader.from_response(resp.response)
        loop.run_until_complete(save_multipart_files(reader))
        loop.close()

    def _list_files(self, path: Union[str, Path]='.'):
        resp = yield Request('GET', '/folders/{}/files'.format(self.name), {
            'path': path,
        }, config=self.config)
        return resp.json()

    def __init__(self, name: str, *, config: APIConfig=None):
        assert _rx_slug.search(name) is not None
        self.name = name
        self.config = config
        self.delete   = self._call_base_method(self._delete)
        self.info     = self._call_base_method(self._info)
        self.upload   = self._call_base_method(self._upload)
        self.download = self._call_base_method(self._download)
        self.list_files = self._call_base_method(self._list_files)

    def __init_subclass__(cls):
        cls.create = cls._call_base_clsmethod(cls._create)
        cls.list   = cls._call_base_clsmethod(cls._list)
        cls.get    = cls._call_base_clsmethod(cls._get)


class VFolder(SyncFunctionMixin, BaseVFolder):
    pass
