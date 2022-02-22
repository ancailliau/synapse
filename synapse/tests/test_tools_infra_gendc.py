import synapse.exc as s_exc
import synapse.common as s_common

import synapse.lib.certdir as s_certdir

import synapse.tests.utils as s_t_utils

import synapse.tools.infra.gendc as s_t_gendc

class InfraGendcTest(s_t_utils.SynTest):

    async def test_basic_gen(self):

        _coreconf = {
            'storm:log': True,
            'provenance:en': False,
            'axon': 'GENAHAURL_axon'
        }

        _svcs = [
            {
                'name': 'axon',
                'docker': {
                    'image': 'vertexproject/synapse-axon:v2.x.x'
                }
            },
            {
                'name': 'cortex',
                'docker': {
                    'image': 'vertexproject/synapse-cortex:v2.x.x'
                },
                'cellconf': _coreconf
            }
        ]

        basic_cells = {
            'version': '0.1.0',
            'aha': {
                'aha:network': 'mytest.loop.vertex.link',
            },
            'svcs': _svcs
        }

        with self.getTestDir() as dirn:
            yamlfp = s_common.genpath(dirn, 'input.yaml')
            s_common.yamlsave(basic_cells, yamlfp)
            outdir = s_common.genpath(dirn, 'output')
            argv = [yamlfp, outdir]
            ret = await s_t_gendc.main(argv=argv, outp=None)
            self.eq(0, ret)

            ahaconf = s_common.yamlload(outdir, 'aha', 'storage', 'cell.yaml')
            self.eq(ahaconf.get('aha:admin'), 'root@mytest.loop.vertex.link')
            self.eq(ahaconf.get('aha:name'), 'aha.mytest.loop.vertex.link')
            self.eq(ahaconf.get('aha:network'), 'mytest.loop.vertex.link')
            self.eq(ahaconf.get('backup:dir'), '/vertex/backups')
            lstn = 'ssl://0.0.0.0:27492/?hostname=aha.mytest.loop.vertex.link&ca=mytest.loop.vertex.link'
            self.eq(ahaconf.get('dmon:listen'), lstn)
            self.eq(ahaconf.get('backup:dir'), '/vertex/backups')
            self.none(ahaconf.get('https:port'))

            axonconf = s_common.yamlload(outdir, 'axon', 'storage', 'cell.yaml')
            self.eq(axonconf.get('aha:admin'), 'root@mytest.loop.vertex.link')
            self.eq(axonconf.get('aha:name'), 'axon')
            self.eq(axonconf.get('aha:network'), 'mytest.loop.vertex.link')
            self.eq(axonconf.get('backup:dir'), '/vertex/backups')
            self.eq(axonconf.get('aha:registry'), (
                'ssl://root@aha.mytest.loop.vertex.link:27492/',
            ))
            lstn = 'ssl://0.0.0.0:0/?hostname=axon.mytest.loop.vertex.link&ca=mytest.loop.vertex.link'
            self.eq(axonconf.get('dmon:listen'), lstn)
            self.eq(axonconf.get('backup:dir'), '/vertex/backups')
            self.none(axonconf.get('https:port'))

            coreconf = s_common.yamlload(outdir, 'cortex', 'storage', 'cell.yaml')
            self.eq(coreconf.get('aha:admin'), 'root@mytest.loop.vertex.link')
            self.eq(coreconf.get('aha:name'), 'cortex')
            self.eq(coreconf.get('aha:network'), 'mytest.loop.vertex.link')
            self.eq(coreconf.get('backup:dir'), '/vertex/backups')
            self.eq(coreconf.get('aha:registry'), (
                'ssl://root@aha.mytest.loop.vertex.link:27492/',
            ))
            lstn = 'ssl://0.0.0.0:0/?hostname=cortex.mytest.loop.vertex.link&ca=mytest.loop.vertex.link'
            self.eq(coreconf.get('dmon:listen'), lstn)
            self.eq(coreconf.get('backup:dir'), '/vertex/backups')
            self.none(coreconf.get('https:port'))
            self.true(coreconf.get('storm:log'))
            self.false(coreconf.get('provenance:en'))
            self.eq(coreconf.get('axon'), 'aha://root@axon.mytest.loop.vertex.link/')

            certs = s_common.genpath(outdir, '_syndir', 'certs')
            certdir = s_certdir.CertDir(path=certs)
            self.nn(certdir.getCaCert('mytest.loop.vertex.link'))
            self.nn(certdir.getCaKey('mytest.loop.vertex.link'))
            self.nn(certdir.getUserCert('axon@mytest.loop.vertex.link'))
            self.nn(certdir.getUserCert('root@mytest.loop.vertex.link'))
            self.nn(certdir.getUserCert('cortex@mytest.loop.vertex.link'))
            self.nn(certdir.getUserKey('axon@mytest.loop.vertex.link'))
            self.nn(certdir.getUserKey('root@mytest.loop.vertex.link'))
            self.nn(certdir.getUserKey('cortex@mytest.loop.vertex.link'))
            self.nn(certdir.getHostCert('aha.mytest.loop.vertex.link'))
            self.nn(certdir.getHostCert('axon.mytest.loop.vertex.link'))
            self.nn(certdir.getHostCert('cortex.mytest.loop.vertex.link'))
            self.nn(certdir.getHostKey('aha.mytest.loop.vertex.link'))
            self.nn(certdir.getHostKey('axon.mytest.loop.vertex.link'))
            self.nn(certdir.getHostKey('cortex.mytest.loop.vertex.link'))

            tnfo = s_common.yamlload(outdir, '_syndir', 'telepath.yaml')
            self.eq(tnfo.get('version'), 1)
            self.eq(tnfo.get('aha:servers'), [['ssl://root@aha.mytest.loop.vertex.link:27492/']])

            with self.raises(s_exc.NoSuchFile):
                with s_common.reqfile(s_common.genpath(outdir, 'usergens.sh')):
                    pass

        # Tweak coreconf to use a service account on the axon
        with self.getTestDir() as dirn:
            _coreconf['axon'] = 'GENSVCAHAURL_axon'
            yamlfp = s_common.genpath(dirn, 'input.yaml')
            s_common.yamlsave(basic_cells, yamlfp)
            outdir = s_common.genpath(dirn, 'output')
            argv = [yamlfp, outdir]
            ret = await s_t_gendc.main(argv=argv, outp=None)
            self.eq(0, ret)

            coreconf = s_common.yamlload(outdir, 'cortex', 'storage', 'cell.yaml')
            self.eq(coreconf.get('axon'), 'aha://cortex@axon.mytest.loop.vertex.link/')
            usergenspath = s_common.genpath(outdir, 'usergens.sh')
            with s_common.reqfile(usergenspath) as fd:
                usergens = fd.read().decode()
            self.isin('python -m synapse.tools.cellauth aha://root@axon.mytest.loop.vertex.link/ modify '
                      '--adduser cortex@mytest.loop.vertex.link',
                      usergens)
            self.isin('python -m synapse.tools.cellauth aha://root@axon.mytest.loop.vertex.link/ modify '
                      '--admin cortex@mytest.loop.vertex.link',
                      usergens)

        # Add a arbitrary stormservice into the configs
        with self.getTestDir() as dirn:
            _coreconf['axon'] = 'GENAHAURL_axon'

            stormsvc = {
                'name': 'fooservice',
                'cellconf': {
                    'foo': 'bar',
                    'svc:fqdn': 'GENFQDN_fooservice'
                },
                'docker': {
                    'image': 'vertexproject/synapse-fooservice:dev',
                },
                'stormsvc': True,
            }

            _svcs.append(stormsvc)

            yamlfp = s_common.genpath(dirn, 'input.yaml')
            s_common.yamlsave(basic_cells, yamlfp)
            outdir = s_common.genpath(dirn, 'output')
            argv = [yamlfp, outdir]
            ret = await s_t_gendc.main(argv=argv, outp=None)
            self.eq(0, ret)

            fooconf = s_common.yamlload(outdir, 'fooservice', 'storage', 'cell.yaml')
            self.eq(fooconf.get('foo'), 'bar')
            self.eq(fooconf.get('aha:name'), 'fooservice')
            self.eq(fooconf.get('svc:fqdn'), 'fooservice.mytest.loop.vertex.link')

            usergenspath = s_common.genpath(outdir, 'storm_services.storm')
            with s_common.reqfile(usergenspath) as fd:
                stormline = fd.read().decode()
            self.isin('service.add fooservice aha://root@fooservice.mytest.loop.vertex.link/', stormline)

    async def test_docker(self):

        _coreconf = {
            'storm:log': True,
            'provenance:en': False,
        }

        _svcs = [
            {
                'name': 'axon',
                'docker': {
                    'image': 'vertexproject/synapse-axon:v2.x.x',
                    'environment': {
                        'SYN_LOG_LEVEL': 'INFO',
                    },
                    'labels': {
                        'thing': 'axon'
                    }
                }
            },
            {
                'name': 'cortex',
                'docker': {
                    'image': 'vertexproject/synapse-cortex:v2.x.x',
                    'labels': {
                        'thing': 'cortex'
                    }
                },
                'cellconf': _coreconf
            }
        ]

        basic_cells = {
            'version': '0.1.0',
            'aha': {
                'aha:network': 'mytest.loop.vertex.link',
            },
            'docker': {
                'labels': {
                    'env': 'dev',
                },
            },
            'svcs': _svcs
        }

        with self.getTestDir() as dirn:
            yamlfp = s_common.genpath(dirn, 'input.yaml')
            s_common.yamlsave(basic_cells, yamlfp)
            outdir = s_common.genpath(dirn, 'output')
            argv = [yamlfp, outdir]
            ret = await s_t_gendc.main(argv=argv, outp=None)
            self.eq(0, ret)

            axondc = s_common.yamlload(outdir, 'axon', 'docker-compose.yaml')
            axonsvc = axondc.get('services').get('axon.mytest.loop.vertex.link')
            env = axonsvc.get('environment')
            self.eq(env.get('SYN_LOG_LEVEL'), 'INFO')
            self.eq(env.get('SYN_LOG_STRUCT'), '1')
            labels = axonsvc.get('labels')
            self.eq(labels.get('env'), 'dev')
            self.eq(labels.get('thing'), 'axon')
            self.eq(axonsvc.get('image'), 'vertexproject/synapse-axon:v2.x.x')

            coredc = s_common.yamlload(outdir, 'cortex', 'docker-compose.yaml')
            coresvc = coredc.get('services').get('cortex.mytest.loop.vertex.link')
            env = coresvc.get('environment')
            self.eq(env.get('SYN_LOG_LEVEL'), 'DEBUG')
            self.eq(env.get('SYN_LOG_STRUCT'), '1')
            labels = coresvc.get('labels')
            self.eq(labels.get('env'), 'dev')
            self.eq(labels.get('thing'), 'cortex')
            self.eq(coresvc.get('image'), 'vertexproject/synapse-cortex:v2.x.x')
