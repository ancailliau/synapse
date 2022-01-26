import os
import time
import tarfile
import asyncio

from unittest import mock


import synapse.exc as s_exc
import synapse.common as s_common
import synapse.daemon as s_daemon
import synapse.telepath as s_telepath

import synapse.lib.cell as s_cell
import synapse.lib.link as s_link
import synapse.tests.utils as s_t_utils


# # Defective versions of spawned backup processes
# def _sleeperProc(pipe, srcdir, dstdir, lmdbpaths, logconf):
#     time.sleep(3.0)
#
# def _sleeper2Proc(pipe, srcdir, dstdir, lmdbpaths, logconf):
#     time.sleep(2.0)
#
# def _exiterProc(pipe, srcdir, dstdir, lmdbpaths, logconf):
#     pipe.send('captured')
#     sys.exit(1)
#
def _backupSleep(path, linkinfo):
    time.sleep(3.0)

async def _doEOFBackup(path):
    return

async def _iterBackupEOF(path, linkinfo):
    link = await s_link.fromspawn(linkinfo)
    await s_daemon.t2call(link, _doEOFBackup, (path,), {})
    link.writer.write_eof()
    await link.fini()

def _backupEOF(path, linkinfo):
    asyncio.run(_iterBackupEOF(path, linkinfo))


class CovFailTest(s_t_utils.SynTest):

    async def test_cell_stream_backup(self):
        with self.getTestDir() as dirn:

            backdirn = os.path.join(dirn, 'backups')
            coredirn = os.path.join(dirn, 'cortex')
            bkuppath = os.path.join(dirn, 'bkup.tar.gz')
            bkuppath2 = os.path.join(dirn, 'bkup2.tar.gz')
            bkuppath3 = os.path.join(dirn, 'bkup3.tar.gz')
            bkuppath4 = os.path.join(dirn, 'bkup4.tar.gz')
            bkuppath5 = os.path.join(dirn, 'bkup5.tar.gz')

            conf = {'backup:dir': backdirn}
            s_common.yamlsave(conf, coredirn, 'cell.yaml')

            async with self.getTestCore(dirn=coredirn) as core:

                core.certdir.genCaCert('localca')
                core.certdir.genHostCert('localhost', signas='localca')
                core.certdir.genUserCert('root@localhost', signas='localca')

                root = await core.auth.addUser('root@localhost')
                await root.setAdmin(True)

                nodes = await core.nodes('[test:str=streamed]')
                self.len(1, nodes)

                async with core.getLocalProxy() as proxy:

                    with self.raises(s_exc.BadArg):
                        async for msg in proxy.iterBackupArchive('nope'):
                            pass

                    await proxy.runBackup(name='bkup')

                    with mock.patch('synapse.lib.cell._iterBackupProc', _backupSleep):
                        arch = s_t_utils.alist(proxy.iterBackupArchive('bkup'))
                        with self.raises(asyncio.TimeoutError):
                            await asyncio.wait_for(arch, timeout=0.1)

                        async def _fakeBackup(self, name=None, wait=True):
                            s_common.gendir(os.path.join(backdirn, name))

                        with mock.patch.object(s_cell.Cell, 'runBackup', _fakeBackup):
                            arch = s_t_utils.alist(proxy.iterNewBackupArchive('nobkup', remove=True))
                            with self.raises(asyncio.TimeoutError):
                                await asyncio.wait_for(arch, timeout=0.1)

                        async def _slowFakeBackup(self, name=None, wait=True):
                            s_common.gendir(os.path.join(backdirn, name))
                            await asyncio.sleep(3.0)

                        with mock.patch.object(s_cell.Cell, 'runBackup', _slowFakeBackup):
                            arch = s_t_utils.alist(proxy.iterNewBackupArchive('nobkup2', remove=True))
                            with self.raises(asyncio.TimeoutError):
                                await asyncio.wait_for(arch, timeout=0.1)

                    with self.raises(s_exc.BadArg):
                        async for msg in proxy.iterNewBackupArchive('bkup'):
                            pass

                    # Get an existing backup
                    with open(bkuppath, 'wb') as bkup:
                        async for msg in proxy.iterBackupArchive('bkup'):
                            bkup.write(msg)

                    # Create a new backup
                    nodes = await core.nodes('[test:str=freshbkup]')
                    self.len(1, nodes)

                    with open(bkuppath2, 'wb') as bkup2:
                        async for msg in proxy.iterNewBackupArchive('bkup2'):
                            bkup2.write(msg)

                    self.eq(('bkup', 'bkup2'), sorted(await proxy.getBackups()))
                    self.true(os.path.isdir(os.path.join(backdirn, 'bkup2')))

                    # Create a new backup and remove after
                    nodes = await core.nodes('[test:str=lastbkup]')
                    self.len(1, nodes)

                    with open(bkuppath3, 'wb') as bkup3:
                        async for msg in proxy.iterNewBackupArchive('bkup3', remove=True):
                            bkup3.write(msg)

                    self.eq(('bkup', 'bkup2'), sorted(await proxy.getBackups()))
                    self.false(os.path.isdir(os.path.join(backdirn, 'bkup3')))

                    # Create a new backup without a name param
                    nodes = await core.nodes('[test:str=noname]')
                    self.len(1, nodes)

                    with open(bkuppath4, 'wb') as bkup4:
                        async for msg in proxy.iterNewBackupArchive(remove=True):
                            bkup4.write(msg)

                    self.eq(('bkup', 'bkup2'), sorted(await proxy.getBackups()))

                    # Start another backup while one is already running
                    bkup = s_t_utils.alist(proxy.iterNewBackupArchive('runbackup', remove=True))
                    task = core.schedCoro(bkup)
                    await asyncio.sleep(0)

                    fail = s_t_utils.alist(proxy.iterNewBackupArchive('alreadyrunning', remove=True))
                    await self.asyncraises(s_exc.BackupAlreadyRunning, fail)
                    await asyncio.wait_for(task, 5)

            with tarfile.open(bkuppath, 'r:gz') as tar:
                tar.extractall(path=dirn)

            bkupdirn = os.path.join(dirn, 'bkup')
            async with self.getTestCore(dirn=bkupdirn) as core:
                nodes = await core.nodes('test:str=streamed')
                self.len(1, nodes)

                nodes = await core.nodes('test:str=freshbkup')
                self.len(0, nodes)

            with tarfile.open(bkuppath2, 'r:gz') as tar:
                tar.extractall(path=dirn)

            bkupdirn2 = os.path.join(dirn, 'bkup2')
            async with self.getTestCore(dirn=bkupdirn2) as core:
                nodes = await core.nodes('test:str=freshbkup')
                self.len(1, nodes)

            with tarfile.open(bkuppath3, 'r:gz') as tar:
                tar.extractall(path=dirn)

            bkupdirn3 = os.path.join(dirn, 'bkup3')
            async with self.getTestCore(dirn=bkupdirn3) as core:
                nodes = await core.nodes('test:str=lastbkup')
                self.len(1, nodes)

            with tarfile.open(bkuppath4, 'r:gz') as tar:
                bkupname = os.path.commonprefix(tar.getnames())
                tar.extractall(path=dirn)

            bkupdirn4 = os.path.join(dirn, bkupname)
            async with self.getTestCore(dirn=bkupdirn4) as core:
                nodes = await core.nodes('test:str=noname')
                self.len(1, nodes)

            # Test backup over SSL
            async with self.getTestCore(dirn=coredirn) as core:

                nodes = await core.nodes('[test:str=ssl]')
                addr, port = await core.dmon.listen('ssl://0.0.0.0:0?hostname=localhost&ca=localca')

                async with await s_telepath.openurl(f'ssl://root@127.0.0.1:{port}?hostname=localhost') as proxy:
                    with open(bkuppath5, 'wb') as bkup5:
                        async for msg in proxy.iterNewBackupArchive(remove=True):
                            bkup5.write(msg)

                    with mock.patch('synapse.lib.cell._iterBackupProc', _backupEOF):
                        await s_t_utils.alist(proxy.iterNewBackupArchive('eof', remove=True))

            with tarfile.open(bkuppath5, 'r:gz') as tar:
                bkupname = os.path.commonprefix(tar.getnames())
                tar.extractall(path=dirn)

            bkupdirn5 = os.path.join(dirn, bkupname)
            async with self.getTestCore(dirn=bkupdirn5) as core:
                nodes = await core.nodes('test:str=ssl')
                self.len(1, nodes)
