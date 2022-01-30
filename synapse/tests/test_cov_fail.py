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

            async with self.getTestCell(ctor=s_cell.Cell, dirn=coredirn) as core:

                core.certdir.genCaCert('localca')
                core.certdir.genHostCert('localhost', signas='localca')
                core.certdir.genUserCert('root@localhost', signas='localca')

                root = await core.auth.addUser('root@localhost')
                await root.setAdmin(True)

                await core.cellinfo.set(name='v0', valu='streamed')

                async with core.getLocalProxy() as proxy:

                    with self.raises(s_exc.BadArg):
                        async for msg in proxy.iterBackupArchive('nope'):
                            pass

                    await proxy.runBackup(name='bkup')

                    with self.raises(s_exc.BadArg):
                        async for msg in proxy.iterNewBackupArchive('bkup'):
                            pass

                    # Get an existing backup
                    with open(bkuppath, 'wb') as bkup:
                        async for msg in proxy.iterBackupArchive('bkup'):
                            bkup.write(msg)

                    # Create a new backup
                    await core.cellinfo.set(name='v1', valu='freshbkup')

                    with open(bkuppath2, 'wb') as bkup2:
                        async for msg in proxy.iterNewBackupArchive('bkup2'):
                            bkup2.write(msg)

                    self.eq(('bkup', 'bkup2'), sorted(await proxy.getBackups()))
                    self.true(os.path.isdir(os.path.join(backdirn, 'bkup2')))

                    # Create a new backup and remove after
                    await core.cellinfo.set(name='v2', valu='lastbkup')

                    with open(bkuppath3, 'wb') as bkup3:
                        async for msg in proxy.iterNewBackupArchive('bkup3', remove=True):
                            bkup3.write(msg)

                    self.eq(('bkup', 'bkup2'), sorted(await proxy.getBackups()))
                    self.false(os.path.isdir(os.path.join(backdirn, 'bkup3')))

                    # Create a new backup without a name param
                    await core.cellinfo.set(name='v3', valu='noname')

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
