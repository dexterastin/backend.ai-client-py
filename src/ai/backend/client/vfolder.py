import asyncio
from pathlib import Path
from typing import Sequence, Union
import zlib

import aiohttp
from tqdm import tqdm

from .base import api_function
from .exceptions import BackendAPIError, BackendClientError
from .request import Request, AttachedFile
from .cli.pretty import ProgressReportingReader

__all__ = (
    'VFolder',
)


class VFolder:
    '''
    Provides vfolder operations via class methods and an object representation of a vfolder.
    '''

    session = None
    '''The client session instance that this function class is bound to.'''

    def __init__(self, name: str):
        self.name = name

    @api_function
    @classmethod
    async def create(cls, name: str, host: str = None, group: str = None):
        '''
        Create a new virtual folder.
        Returns the API response.

        :param name: The human-friendly name of vfolder name.
        :param host: The target host to create the vfolder (e.g., NAS server).
            If not set, "default" is used.
        :param group: The target user group ID (``uuid.UUID``) or name (``str``)
            to create the vfolder in.  If not set, "default" is used.

        :returns: A mapping that represents :ref:`vfolder-creation-result-object`.
        '''
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
    async def list(cls, list_all: bool = False):
        '''
        List all vfolders accessible by the client user,
        including the user's owned ones, the vfolders shared by
        other users, and the vfolders in the groups where
        the user has memberships.

        :param list_all: Show all vfolders in the cluster,
            regardless of the client user's group and domain
            memberships. Superadmin-only.

        :returns: A list of :ref:`vfolder-list-item-object`.
        '''
        rqst = Request(cls.session, 'GET', '/folders')
        rqst.set_json({'all': list_all})
        async with rqst.fetch() as resp:
            return await resp.json()

    @api_function
    @classmethod
    async def list_hosts(cls):
        '''
        List all vfolder hosts accessible by the client user.

        :returns: A mapping with two keys, ``"default"`` and ``"allowed"``.
            The *default* is the host used when the user does not specify
            any host when creating new vfolders.
            The *allowed* list is the hosts that the user can create vfolders on.
        '''
        rqst = Request(cls.session, 'GET', '/folders/_/hosts')
        async with rqst.fetch() as resp:
            return await resp.json()

    @api_function
    @classmethod
    async def list_all_hosts(cls):
        '''
        List all vfolder hosts available in the whole cluster.
        Super-admin only.

        :returns: Same as :meth:`list_hosts`.
        '''
        rqst = Request(cls.session, 'GET', '/folders/_/all_hosts')
        async with rqst.fetch() as resp:
            return await resp.json()

    @api_function
    @classmethod
    async def list_allowed_types(cls):
        '''
        List all vfolder types usable by the client user.

        :returns: A list of strings where each item may be either "user" or "group".
        '''
        rqst = Request(cls.session, 'GET', '/folders/_/allowed_types')
        async with rqst.fetch() as resp:
            return await resp.json()

    @api_function
    async def info(self):
        '''
        Get a brief information about the vfolder.

        :returns: A mapping that represents :ref:`vfolder-item-object`.
        '''
        rqst = Request(self.session, 'GET', '/folders/{0}'.format(self.name))
        async with rqst.fetch() as resp:
            return await resp.json()

    @api_function
    async def delete(self):
        '''
        Delete the vfolder.

        :returns: None.
        '''
        rqst = Request(self.session, 'DELETE', '/folders/{0}'.format(self.name))
        async with rqst.fetch():
            return {}

    @api_function
    async def rename(self, new_name):
        '''
        Rename the vfolder into the given name.

        :returns: None.
        '''
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
        '''
        Upload one or more files to the vfolder.

        :param files: A list of file paths inside *basedir*.
            They may be either relative or absolute paths, but they must begin with
            *basedir* when absolute.
        :param basedir: A path that represents the common prefix of given files.
            The files are stored in the vfolder with sub-paths taken from the original
            paths after stripping this prefix.

            For example, if *basedir* is ``/home/user`` and a path in *files* is
            ``/home/user/part1/data.txt``, the file is uploaded to
            ``<vfolder-root>/part1/data.txt``.
            If you mount the vfolder named as "mydata" in a session, the file is
            available at ``/home/work/mydata/part1/data.txt``.

            If not specified, it uses the current working directory.
        :param show_progress: Display a text-based progress bar in the terminal.

        :returns: None.
        '''
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
        '''
        Create a directory inside the vfolder.

        :param path: A path relative to the vfolder root.

        :returns: None.
        '''
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
        '''
        Delete one or more files from the vfolder.
        This operation is **not reversible!**

        :param files: The list of paths relative to the vfolder root.
        :param recursive: Perform a recursive deletion if it encounters directories.

        :returns: None.
        '''
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
                       stored_path: Union[str, Path] = None,
                       show_progress: bool = False):
        '''
        Download one or more files from the vfolder.

        :param files: The list of paths relative to the vfolder host.
        :param stored_path: The path to store the downloaded files, including
            their sub-directory paths.  If not specified, the current working directory
            is used.
        :param show_progress: Display a text-based progress bar in the terminal.

        :returns: None.
        '''

        stored_path = (Path.cwd() if stored_path is None
                       else Path(stored_path).resolve())
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
                        fp = open(stored_path / part.filename, 'wb')
                        while True:
                            chunk = await part.read_chunk()
                            if not chunk:
                                break
                            fp.write(chunk)
                            acc_bytes += len(chunk)
                            pbar.update(len(chunk))
                        fp.close()
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
        '''
        Get the list of files in the given subpath inside the vfolder.

        :param path: The sub-path in the vfolder root.

        :returns: A list of :ref:`vfolder-file-object`.
        '''
        rqst = Request(self.session, 'GET', '/folders/{}/files'.format(self.name))
        rqst.set_json({
            'path': path,
        })
        async with rqst.fetch() as resp:
            return await resp.json()

    @api_function
    async def invite(self, perm: str, emails: Sequence[str]):
        '''
        Send an invitation to the given list of users represented in emails.

        :param perm: The permission of invited users on this vfolder.
            Either ``"rw"`` (read-write) or ``"ro"`` (read-only).
        :param emails: The list of invited users.

        :returns: A list of the generated invitation IDs.
            These IDs can be used to refer each invitations.
        '''
        rqst = Request(self.session, 'POST', '/folders/{}/invite'.format(self.name))
        rqst.set_json({
            'perm': perm, 'user_ids': emails,
        })
        async with rqst.fetch() as resp:
            return await resp.json()

    @api_function
    @classmethod
    async def invitations(cls):
        '''
        Get the list of vfolder invitations to me.

        :returns: A list of :ref:`vfolder-invitation-object`.
        '''
        rqst = Request(cls.session, 'GET', '/folders/invitations/list')
        async with rqst.fetch() as resp:
            return await resp.json()

    @api_function
    @classmethod
    async def accept_invitation(cls, inv_id: str):
        '''
        Accept the vfolder invitation.

        :returns: A mapping with a server-generated message in the ``"msg"`` field.
        '''
        rqst = Request(cls.session, 'POST', '/folders/invitations/accept')
        rqst.set_json({'inv_id': inv_id})
        async with rqst.fetch() as resp:
            return await resp.json()

    @api_function
    @classmethod
    async def delete_invitation(cls, inv_id: str):
        '''
        Cancel or reject (depending on the client user) the vfolder invitation.

        :returns: A mapping with a server-generated message in the ``"msg"`` field.
        '''
        rqst = Request(cls.session, 'DELETE', '/folders/invitations/delete')
        rqst.set_json({'inv_id': inv_id})
        async with rqst.fetch() as resp:
            return await resp.json()

    @api_function
    @classmethod
    async def get_fstab_contents(cls, agent_id: str = None):
        '''
        Retrieve the /etc/fstab file content of a specific agent.
        Super-admin only.

        :param agent_id: The agent ID.

        :returns: A mapping that represents :ref:`vfolder-fstab-object`.
        '''
        rqst = Request(cls.session, 'GET', '/folders/_/fstab')
        rqst.set_json({
            'agent_id': agent_id,
        })
        async with rqst.fetch() as resp:
            return await resp.json()

    @api_function
    @classmethod
    async def list_mounts(cls):
        '''
        Scan the mounted vfolder hosts in all agents.
        Super-admin only.

        :returns: A mapping with two keys, ``"manager"`` and ``"agents"``
            which contain per-node scanning results, composed of three fields:
            ``"success"``, ``"mounts"``, and ``"message"``.
        '''
        rqst = Request(cls.session, 'GET', '/folders/_/mounts')
        async with rqst.fetch() as resp:
            return await resp.json()

    @api_function
    @classmethod
    async def mount_host(cls, name: str, fs_location: str, options=None,
                         edit_fstab: bool = False):
        '''
        Mount the given filesystem at *fs_location* to all manager/agent nodes,
        as the given *name* under the configured mount path.
        If *edit_fstab* is set true, write the mount options into that file.
        Super-admin only.
        '''
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
        '''
        The reverse of :meth:`mount_host` operation.
        '''
        rqst = Request(cls.session, 'DELETE', '/folders/_/mounts')
        rqst.set_json({
            'name': name,
            'edit_fstab': edit_fstab,
        })
        async with rqst.fetch() as resp:
            return await resp.json()
