import asyncio
from pathlib import Path
from typing import Sequence, Union

import aiohttp
from aiohttp import hdrs
from tqdm import tqdm

from .base import api_function
from .config import DEFAULT_CHUNK_SIZE
from .exceptions import BackendAPIError, BackendClientError
from .request import Request, AttachedFile
from .cli.pretty import ProgressReportingReader

__all__ = (
    'VFolder',
)


class VFolder:

    session = None
    '''The client session instance that this function class is bound to.'''

    def __init__(self, name: str):
        self.name = name

    @api_function
    @classmethod
    async def create(cls, name: str, host: str = None, group: str = None):
        rqst = Request(cls.session, 'POST', '/folders')
        rqst.set_json({
            'name': name,
            'host': host,
            'group': group,
        })
        async with rqst.fetch() as resp:
            return await resp.json()

    @api_function
    @classmethod
    async def list(cls, list_all=False):
        rqst = Request(cls.session, 'GET', '/folders')
        rqst.set_json({'all': list_all})
        async with rqst.fetch() as resp:
            return await resp.json()

    @api_function
    @classmethod
    async def list_hosts(cls):
        rqst = Request(cls.session, 'GET', '/folders/_/hosts')
        async with rqst.fetch() as resp:
            return await resp.json()

    @api_function
    @classmethod
    async def list_all_hosts(cls):
        rqst = Request(cls.session, 'GET', '/folders/_/all_hosts')
        async with rqst.fetch() as resp:
            return await resp.json()

    @api_function
    @classmethod
    async def list_allowed_types(cls):
        rqst = Request(cls.session, 'GET', '/folders/_/allowed_types')
        async with rqst.fetch() as resp:
            return await resp.json()

    @api_function
    async def info(self):
        rqst = Request(self.session, 'GET', '/folders/{0}'.format(self.name))
        async with rqst.fetch() as resp:
            return await resp.json()

    @api_function
    async def delete(self):
        rqst = Request(self.session, 'DELETE', '/folders/{0}'.format(self.name))
        async with rqst.fetch():
            return {}

    @api_function
    async def rename(self, new_name):
        rqst = Request(self.session, 'POST', '/folders/{0}/rename'.format(self.name))
        rqst.set_json({
            'new_name': new_name,
        })
        async with rqst.fetch() as resp:
            self.name = new_name
            return await resp.text()

    @api_function
    async def upload(self, files: Sequence[Union[str, Path]],
                     basedir: Union[str, Path] = None,
                     show_progress: bool = False):
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
            attachments = []
            for file_path in files:
                try:
                    attachments.append(AttachedFile(
                        str(file_path.relative_to(base_path)),
                        ProgressReportingReader(str(file_path),
                                                tqdm_instance=tqdm_obj),
                        'application/octet-stream',
                    ))
                except ValueError:
                    msg = 'File "{0}" is outside of the base directory "{1}".' \
                          .format(file_path, base_path)
                    raise ValueError(msg) from None

            rqst = Request(self.session,
                           'POST', '/folders/{}/upload'.format(self.name))
            rqst.attach_files(attachments)
            async with rqst.fetch() as resp:
                return await resp.text()

    @api_function
    async def mkdir(self, path: Union[str, Path]):
        rqst = Request(self.session, 'POST',
                       '/folders/{}/mkdir'.format(self.name))
        rqst.set_json({
            'path': path,
        })
        async with rqst.fetch() as resp:
            return await resp.text()

    @api_function
    async def delete_files(self,
                            files: Sequence[Union[str, Path]],
                            recursive: bool = False):
        rqst = Request(self.session, 'DELETE',
                       '/folders/{}/delete_files'.format(self.name))
        rqst.set_json({
            'files': files,
            'recursive': recursive,
        })
        async with rqst.fetch() as resp:
            return await resp.text()

    @api_function
    async def download(self, files: Sequence[Union[str, Path]],
                        show_progress: bool = False):

        rqst = Request(self.session, 'GET',
                       '/folders/{}/download'.format(self.name))
        rqst.set_json({
            'files': files,
        })
        try:
            async with rqst.fetch() as resp:
                if resp.status // 100 != 2:
                    raise BackendAPIError(resp.status, resp.reason,
                                          await resp.text())
                total_bytes = int(resp.headers['X-TOTAL-PAYLOADS-LENGTH'])
                tqdm_obj = tqdm(desc='Downloading files',
                                unit='bytes', unit_scale=True,
                                total=total_bytes,
                                disable=not show_progress)
                reader = aiohttp.MultipartReader.from_response(resp.raw_response)
                with tqdm_obj as pbar:
                    acc_bytes = 0
                    while True:
                        part = await reader.next()
                        if part is None:
                            break
                        assert part.headers.get(hdrs.CONTENT_ENCODING, 'identity').lower() == 'identity'
                        assert part.headers.get(hdrs.CONTENT_TRANSFER_ENCODING, 'binary').lower() in (
                            'binary', '8bit', '7bit',
                        )
                        with open(part.filename, 'wb') as fp:
                            while True:
                                chunk = await part.read_chunk(DEFAULT_CHUNK_SIZE)
                                if not chunk:
                                    break
                                fp.write(chunk)
                                acc_bytes += len(chunk)
                                pbar.update(len(chunk))
                    pbar.update(total_bytes - acc_bytes)
        except (asyncio.CancelledError, asyncio.TimeoutError):
            # These exceptions must be bubbled up.
            raise
        except aiohttp.ClientError as e:
            msg = 'Request to the API endpoint has failed.\n' \
                  'Check your network connection and/or the server status.'
            raise BackendClientError(msg) from e

    @api_function
    async def list_files(self, path: Union[str, Path] = '.'):
        rqst = Request(self.session, 'GET', '/folders/{}/files'.format(self.name))
        rqst.set_json({
            'path': path,
        })
        async with rqst.fetch() as resp:
            return await resp.json()

    @api_function
    async def invite(self, perm: str, emails: Sequence[str]):
        rqst = Request(self.session, 'POST', '/folders/{}/invite'.format(self.name))
        rqst.set_json({
            'perm': perm, 'user_ids': emails,
        })
        async with rqst.fetch() as resp:
            return await resp.json()

    @api_function
    @classmethod
    async def invitations(cls):
        rqst = Request(cls.session, 'GET', '/folders/invitations/list')
        async with rqst.fetch() as resp:
            return await resp.json()

    @api_function
    @classmethod
    async def accept_invitation(cls, inv_id: str):
        rqst = Request(cls.session, 'POST', '/folders/invitations/accept')
        rqst.set_json({'inv_id': inv_id})
        async with rqst.fetch() as resp:
            return await resp.json()

    @api_function
    @classmethod
    async def delete_invitation(cls, inv_id: str):
        rqst = Request(cls.session, 'DELETE', '/folders/invitations/delete')
        rqst.set_json({'inv_id': inv_id})
        async with rqst.fetch() as resp:
            return await resp.json()

    @api_function
    @classmethod
    async def get_fstab_contents(cls, agent_id=None):
        rqst = Request(cls.session, 'GET', '/folders/_/fstab')
        rqst.set_json({
            'agent_id': agent_id,
        })
        async with rqst.fetch() as resp:
            return await resp.json()

    @api_function
    @classmethod
    async def list_mounts(cls):
        rqst = Request(cls.session, 'GET', '/folders/_/mounts')
        async with rqst.fetch() as resp:
            return await resp.json()

    @api_function
    @classmethod
    async def mount_host(cls, name: str, fs_location: str, options=None,
                         edit_fstab: bool = False):
        rqst = Request(cls.session, 'POST', '/folders/_/mounts')
        rqst.set_json({
            'name': name,
            'fs_location': fs_location,
            'options': options,
            'edit_fstab': edit_fstab,
        })
        async with rqst.fetch() as resp:
            return await resp.json()

    @api_function
    @classmethod
    async def umount_host(cls, name: str, edit_fstab: bool = False):
        rqst = Request(cls.session, 'DELETE', '/folders/_/mounts')
        rqst.set_json({
            'name': name,
            'edit_fstab': edit_fstab,
        })
        async with rqst.fetch() as resp:
            return await resp.json()
