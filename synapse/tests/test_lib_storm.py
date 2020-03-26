import asyncio
import datetime

import synapse.exc as s_exc
import synapse.common as s_common

import synapse.tests.utils as s_t_utils
from synapse.tests.utils import alist

class StormTest(s_t_utils.SynTest):

    async def test_storm_movetag(self):

        async with self.getTestCore() as core:

            async with await core.snap() as snap:
                node = await snap.addNode('test:str', 'foo')
                await node.addTag('hehe.haha', valu=(20, 30))

                tagnode = await snap.getNodeByNdef(('syn:tag', 'hehe.haha'))

                await tagnode.set('doc', 'haha doc')
                await tagnode.set('title', 'haha title')

            await s_common.aspin(core.eval('movetag #hehe #woot'))

            await self.agenlen(0, core.eval('#hehe'))
            await self.agenlen(0, core.eval('#hehe.haha'))

            await self.agenlen(1, core.eval('#woot'))
            await self.agenlen(1, core.eval('#woot.haha'))

            async with await core.snap() as snap:

                newt = await core.getNodeByNdef(('syn:tag', 'woot.haha'))

                self.eq(newt.get('doc'), 'haha doc')
                self.eq(newt.get('title'), 'haha title')

                node = await snap.getNodeByNdef(('test:str', 'foo'))
                self.eq((20, 30), node.tags.get('woot.haha'))

                self.none(node.tags.get('hehe'))
                self.none(node.tags.get('hehe.haha'))

                node = await snap.getNodeByNdef(('syn:tag', 'hehe'))
                self.eq('woot', node.get('isnow'))

                node = await snap.getNodeByNdef(('syn:tag', 'hehe.haha'))
                self.eq('woot.haha', node.get('isnow'))

                node = await snap.addNode('test:str', 'bar')

                # test isnow plumbing
                await node.addTag('hehe.haha')

                self.nn(node.tags.get('woot'))
                self.nn(node.tags.get('woot.haha'))

                self.none(node.tags.get('hehe'))
                self.none(node.tags.get('hehe.haha'))

        async with self.getTestCore() as core:

            async with await core.snap() as snap:
                node = await snap.addNode('test:str', 'foo')
                await node.addTag('hehe', valu=(20, 30))

                tagnode = await snap.getNodeByNdef(('syn:tag', 'hehe'))

                await tagnode.set('doc', 'haha doc')

            await s_common.aspin(core.eval('movetag #hehe #woot'))

            await self.agenlen(0, core.eval('#hehe'))
            await self.agenlen(1, core.eval('#woot'))

            async with await core.snap() as snap:
                newt = await core.getNodeByNdef(('syn:tag', 'woot'))

                self.eq(newt.get('doc'), 'haha doc')

        # Test moving a tag which has tags on it.
        async with self.getTestCore() as core:
            async with await core.snap() as snap:
                node = await snap.addNode('test:str', 'V')
                await node.addTag('a.b.c', (None, None))
                tnode = await snap.getNodeByNdef(('syn:tag', 'a.b'))
                await tnode.addTag('foo', (None, None))

            await alist(core.eval('movetag #a.b #a.m'))
            await self.agenlen(2, core.eval('#foo'))
            await self.agenlen(1, core.eval('syn:tag=a.b +#foo'))
            await self.agenlen(1, core.eval('syn:tag=a.m +#foo'))

        # Test moving a tag to itself
        async with self.getTestCore() as core:
            await self.agenraises(s_exc.BadOperArg, core.eval('movetag #foo.bar #foo.bar'))

        # Test moving a tag which does not exist
        async with self.getTestCore() as core:
            await self.agenraises(s_exc.BadOperArg, core.eval('movetag #foo.bar #duck.knight'))

        # Test moving a tag to another tag which is a string prefix of the source
        async with self.getTestCore() as core:
            # core.conf['storm:log'] = True
            async with await core.snap() as snap:
                node = await snap.addNode('test:str', 'V')
                await node.addTag('aaa.b.ccc', (None, None))
                await node.addTag('aaa.b.ddd', (None, None))
                node = await snap.addNode('test:str', 'Q')
                await node.addTag('aaa.barbarella.ccc', (None, None))

            await alist(core.eval('movetag #aaa.b #aaa.barbarella'))

            await self.agenlen(7, core.eval('syn:tag'))
            await self.agenlen(1, core.eval('syn:tag=aaa.barbarella.ccc'))
            await self.agenlen(1, core.eval('syn:tag=aaa.barbarella.ddd'))

    async def test_storm_spin(self):

        async with self.getTestCore() as core:

            await self.agenlen(0, core.eval('[ test:str=foo test:str=bar ] | spin'))
            await self.agenlen(2, core.eval('test:str=foo test:str=bar'))

    async def test_storm_reindex_sudo(self):

        async with self.getTestCore() as core:

            mesgs = await core.stormlist('reindex')
            self.stormIsInWarn('reindex currently does nothing', mesgs)

            msgs = await core.stormlist('.created | sudo')
            self.stormIsInWarn('Sudo is deprecated and does nothing', msgs)

    async def test_storm_count(self):

        async with self.getTestCoreAndProxy() as (realcore, core):
            await self.agenlen(2, core.eval('[ test:str=foo test:str=bar ]'))

            mesgs = await alist(core.storm('test:str=foo test:str=bar | count |  [+#test.tag]'))
            nodes = [mesg for mesg in mesgs if mesg[0] == 'node']
            self.len(2, nodes)
            prints = [mesg for mesg in mesgs if mesg[0] == 'print']
            self.len(1, prints)
            self.eq(prints[0][1].get('mesg'), 'Counted 2 nodes.')

            mesgs = await alist(core.storm('test:str=newp | count'))
            prints = [mesg for mesg in mesgs if mesg[0] == 'print']
            self.len(1, prints)
            self.eq(prints[0][1].get('mesg'), 'Counted 0 nodes.')
            nodes = [mesg for mesg in mesgs if mesg[0] == 'node']
            self.len(0, nodes)

    async def test_storm_uniq(self):
        async with self.getTestCore() as core:
            q = "[test:comp=(123, test) test:comp=(123, duck) test:comp=(123, mode)]"
            await self.agenlen(3, core.eval(q))
            nodes = await alist(core.eval('test:comp -> *'))
            self.len(3, nodes)
            nodes = await alist(core.eval('test:comp -> * | uniq | count'))
            self.len(1, nodes)

    async def test_storm_iden(self):
        async with self.getTestCore() as core:
            q = "[test:str=beep test:str=boop]"
            nodes = await alist(core.eval(q))
            self.len(2, nodes)
            idens = [node.iden() for node in nodes]

            iq = ' '.join(idens)
            # Demonstrate the iden lift does pass through previous nodes in the pipeline
            mesgs = await core.nodes(f'[test:str=hehe] | iden {iq} | count')
            self.len(3, mesgs)

            q = 'iden newp'
            with self.getLoggerStream('synapse.lib.snap', 'Failed to decode iden') as stream:
                await self.agenlen(0, core.eval(q))
                self.true(stream.wait(1))

            q = 'iden deadb33f'
            with self.getLoggerStream('synapse.lib.snap', 'iden must be 32 bytes') as stream:
                await self.agenlen(0, core.eval(q))
                self.true(stream.wait(1))

    async def test_storm_input(self):

        async with self.getTestCore() as core:

            async with await core.snap() as snap:

                node = await snap.addNode('test:str', 'woot')
                await s_common.aspin(node.storm('[ +#hehe ]'))

                await self.agenlen(1, snap.eval('#hehe'))

                await s_common.aspin(node.storm('[ -#hehe ]'))
                await self.agenlen(0, snap.eval('#hehe'))

    async def test_minmax(self):

        async with self.getTestCore() as core:

            minval = core.model.type('time').norm('2015')[0]
            midval = core.model.type('time').norm('2016')[0]
            maxval = core.model.type('time').norm('2017')[0]

            async with await core.snap() as snap:
                # Ensure each node we make has its own discrete created time.
                await asyncio.sleep(0.01)
                node = await snap.addNode('test:guid', '*', {'tick': '2015',
                                                             '.seen': '2015'})
                minc = node.get('.created')
                await asyncio.sleep(0.01)
                node = await snap.addNode('test:guid', '*', {'tick': '2016',
                                                             '.seen': '2016'})
                await asyncio.sleep(0.01)
                node = await snap.addNode('test:guid', '*', {'tick': '2017',
                                                             '.seen': '2017'})
                await asyncio.sleep(0.01)
                node = await snap.addNode('test:str', '1', {'tick': '2016'})

            # Relative paths
            nodes = await core.nodes('test:guid | max :tick')
            self.len(1, nodes)
            self.eq(nodes[0].get('tick'), maxval)

            nodes = await core.nodes('test:guid | min :tick')
            self.len(1, nodes)
            self.eq(nodes[0].get('tick'), minval)

            # Full paths
            nodes = await core.nodes('test:guid | max test:guid:tick')
            self.len(1, nodes)
            self.eq(nodes[0].get('tick'), maxval)

            nodes = await core.nodes('test:guid | min test:guid:tick')
            self.len(1, nodes)
            self.eq(nodes[0].get('tick'), minval)

            # Implicit form filtering with a full path
            nodes = await core.nodes('.created | max test:str:tick')
            self.len(1, nodes)
            self.eq(nodes[0].get('tick'), midval)

            nodes = await core.nodes('.created | min test:str:tick')
            self.len(1, nodes)
            self.eq(nodes[0].get('tick'), midval)

            # Universal prop for relative path
            nodes = await core.nodes('.created>=$minc | max .created',
                                    {'vars': {'minc': minc}})
            self.len(1, nodes)
            self.eq(nodes[0].get('tick'), midval)

            nodes = await core.nodes('.created>=$minc | min .created',
                                    {'vars': {'minc': minc}})
            self.len(1, nodes)
            self.eq(nodes[0].get('tick'), minval)

            # Universal prop for full paths
            nodes = await core.nodes('.created>=$minc  | max test:str.created',
                                    {'vars': {'minc': minc}})
            self.len(1, nodes)
            self.eq(nodes[0].get('tick'), midval)

            nodes = await core.nodes('.created>=$minc  | min test:str.created',
                                    {'vars': {'minc': minc}})
            self.len(1, nodes)
            self.eq(nodes[0].get('tick'), midval)

            # Variables nodesuated
            nodes = await core.nodes('test:guid ($tick, $tock) = .seen | min $tick')
            self.len(1, nodes)
            self.eq(nodes[0].get('tick'), minval)

            nodes = await core.nodes('test:guid ($tick, $tock) = .seen | max $tock')
            self.len(1, nodes)
            self.eq(nodes[0].get('tick'), maxval)

            text = '''[ inet:ipv4=1.2.3.4 inet:ipv4=5.6.7.8 ]
                      { +inet:ipv4=1.2.3.4 [ :asn=10 ] }
                      { +inet:ipv4=5.6.7.8 [ :asn=20 ] }
                      $asn = :asn | min $asn'''

            nodes = await core.nodes(text)
            self.len(1, nodes)
            self.eq(0x01020304, nodes[0].ndef[1])

            text = '''[ inet:ipv4=1.2.3.4 inet:ipv4=5.6.7.8 ]
                      { +inet:ipv4=1.2.3.4 [ :asn=10 ] }
                      { +inet:ipv4=5.6.7.8 [ :asn=20 ] }
                      $asn = :asn | max $asn'''

            nodes = await core.nodes(text)
            self.len(1, nodes)
            self.eq(0x05060708, nodes[0].ndef[1])

            # Sad paths where there are no nodes which match the specified values.
            self.len(0, await core.nodes('test:guid | max :newp'))
            self.len(0, await core.nodes('test:guid | min :newp'))

            # Sad path for a form, not a property; and does not exist at all
            with self.raises(s_exc.BadSyntax):
                await core.nodes('test:guid | max test:newp')
            with self.raises(s_exc.BadSyntax):
                await core.nodes('test:guid | min test:newp')

            # Ensure that max evaluates ival properties as the upper bound.
            async with await core.snap() as snap:
                node = await snap.addNode('test:guid', '*', {'tick': '2015',
                                                             '.seen': (minval, maxval)})
                await node.addTag('maxtest')
                node = await snap.addNode('test:guid', '*', {'tick': '2016',
                                                             '.seen': (midval, midval + 1)})
                await node.addTag('maxtest')

            nodes = await core.nodes('#maxtest | max .seen')
            self.len(1, nodes)
            self.eq(nodes[0].get('tick'), minval)

    async def test_getstormeval(self):

        # Use testechocmd to exercise all of Cmd.getStormEval
        async with self.getTestCore() as core:
            async with await core.snap() as snap:
                await snap.addNode('test:str', 'fancystr', {'tick': 1234, 'hehe': 'haha', '.seen': '3001'})

            q = 'test:str $foo=:tick | testechocmd $foo'
            mesgs = await core.stormlist(q)
            self.stormIsInPrint('[1234]', mesgs)

            q = 'test:str| testechocmd :tick'
            mesgs = await core.stormlist(q)
            self.stormIsInPrint('[1234]', mesgs)

            q = 'test:str| testechocmd .seen'
            mesgs = await core.stormlist(q)
            self.stormIsInPrint('[(32535216000000, 32535216000001)]', mesgs)

            q = 'test:str| testechocmd test:str'
            mesgs = await core.stormlist(q)
            self.stormIsInPrint('[fancystr]', mesgs)

            q = 'test:str| testechocmd test:str:hehe'
            mesgs = await core.stormlist(q)
            self.stormIsInPrint('[haha]', mesgs)

            q = 'test:str| testechocmd test:int'
            mesgs = await core.stormlist(q)
            self.stormIsInPrint('[None]', mesgs)

            q = 'test:str| testechocmd test:int:loc'
            mesgs = await core.stormlist(q)
            self.stormIsInPrint('[None]', mesgs)

            q = 'test:str| testechocmd test:newp'
            mesgs = await core.stormlist(q)
            errs = [m for m in mesgs if m[0] == 'err']
            self.len(1, errs)
            self.eq(errs[0][1][0], 'BadSyntax')

    async def test_scrape(self):
        async with self.getTestCore() as core:
            async with await core.snap() as snap:
                guid = s_common.guid()
                snode = await snap.addNode('inet:search:query', guid,
                                           {'text': 'what about 1.2.3.4',
                                            'time': '2019-04-04 17:03',
                                            'engine': 'google',
                                            })
                await snap.addNode('inet:banner', ('tcp://2.4.6.8:80', 'this is a test foo@bar.com'))

            q = 'inet:search:query | scrape -p :text engine'
            nodes = await core.nodes(q)
            self.len(1, nodes)
            self.eq(nodes[0].ndef, ('inet:ipv4', 0x01020304))

            q = 'inet:search:query | scrape --refs'
            nodes = await core.nodes(q)
            self.len(2, nodes)
            self.eq(nodes[0].ndef, ('inet:ipv4', 0x01020304))
            self.eq(nodes[1].ndef, ('edge:refs', (snode.ndef, nodes[0].ndef)))

            q = 'inet:search:query | scrape --join'
            nodes = await core.nodes(q)
            self.len(2, nodes)
            self.eq(nodes[0].ndef, ('inet:ipv4', 0x01020304))
            self.eq(nodes[1].ndef, snode.ndef)

            # invalid prop
            q = 'inet:search:query | scrape -p engine foobarbaz'
            await self.asyncraises(s_exc.BadOptValu, core.nodes(q))

            # different forms, same prop name
            q = 'inet:search:query inet:banner | scrape -p text'
            nodes = await core.nodes(q)
            self.len(3, nodes)

            # one has it, but the other doesn't, so boom
            q = 'inet:search:query inet:banner | scrape -p engine'
            await self.asyncraises(s_exc.BadOptValu, core.nodes(q))

            await core.nodes('[inet:search:query="*"]')
            mesgs = await core.stormlist('inet:search:query | scrape --props text')
            self.stormIsInPrint('No prop ":text" for', mesgs)

            # make sure we handle .seen(i.e. non-str reprs)
            qtxt = 'ns1.twiter-statics.info'
            async with await core.snap() as snap:
                guid = s_common.guid()
                snode = await snap.addNode('inet:search:query', guid,
                                           {'text': qtxt,
                                               'time': '2019-04-04 17:03',
                                               '.seen': ('2018/11/08 18:21:15.423', '2018/11/08 18:21:15.424'),
                                               'engine': 'google',
                                            })

            q = f'inet:search:query:text={qtxt} | scrape'
            nodes = await core.nodes(q)
            self.len(1, nodes)
            self.eq(nodes[0].ndef, ('inet:fqdn', qtxt))

    async def test_storm_tee(self):

        async with self.getTestCore() as core:

            async with await core.snap() as snap:
                guid = s_common.guid()
                await snap.addNode('edge:refs', (('media:news', guid), ('inet:ipv4', '1.2.3.4')))
                await snap.addNode('inet:dns:a', ('woot.com', '1.2.3.4'))

            await core.nodes('[ inet:ipv4=1.2.3.4 :asn=0 ]')

            nodes = await core.nodes('inet:ipv4=1.2.3.4 | tee { -> * }')
            self.len(1, nodes)
            self.eq(nodes[0].ndef, ('inet:asn', 0))

            nodes = await core.nodes('inet:ipv4=1.2.3.4 | tee --join { -> * }')
            self.len(2, nodes)
            self.eq(nodes[0].ndef, ('inet:asn', 0))
            self.eq(nodes[1].ndef, ('inet:ipv4', 0x01020304))

            q = 'inet:ipv4=1.2.3.4 | tee --join { -> * } { <- * }'
            nodes = await core.nodes(q)
            self.len(3, nodes)
            self.eq(nodes[0].ndef, ('inet:asn', 0))
            self.eq(nodes[1].ndef[0], ('inet:dns:a'))
            self.eq(nodes[2].ndef, ('inet:ipv4', 0x01020304))

            q = 'inet:ipv4=1.2.3.4 | tee --join { -> * } { <- * } { -> edge:refs:n2 :n1 -> * }'
            nodes = await core.nodes(q)
            self.len(4, nodes)
            self.eq(nodes[0].ndef, ('inet:asn', 0))
            self.eq(nodes[1].ndef[0], ('inet:dns:a'))
            self.eq(nodes[2].ndef[0], ('media:news'))
            self.eq(nodes[3].ndef, ('inet:ipv4', 0x01020304))

            # Empty queries are okay - they will just return the input node
            q = 'inet:ipv4=1.2.3.4 | tee {}'
            nodes = await core.nodes(q)
            self.len(1, nodes)
            self.eq(nodes[0].ndef, ('inet:ipv4', 0x01020304))

            # Subqueries are okay too but will just yield the input back out
            q = 'inet:ipv4=1.2.3.4 | tee {{ -> * }}'
            nodes = await core.nodes(q)
            self.len(1, nodes)
            self.eq(nodes[0].ndef, ('inet:ipv4', 0x01020304))

            # Sad path
            q = 'inet:ipv4=1.2.3.4 | tee'
            await self.asyncraises(s_exc.StormRuntimeError, core.nodes(q))

            # Runtsafe tee
            q = 'tee { inet:ipv4=1.2.3.4 } { inet:ipv4 -> * }'
            nodes = await core.nodes(q)
            self.len(2, nodes)
            exp = {
                ('inet:asn', 0),
                ('inet:ipv4', 0x01020304),
            }
            self.eq(exp, {x.ndef for x in nodes})

            q = '$foo=woot.com tee { inet:ipv4=1.2.3.4 } { inet:fqdn=$foo <- * }'
            nodes = await core.nodes(q)
            self.len(3, nodes)
            exp = {
                ('inet:ipv4', 0x01020304),
                ('inet:fqdn', 'woot.com'),
                ('inet:dns:a', ('woot.com', 0x01020304)),
            }
            self.eq(exp, {n.ndef for n in nodes})

            # Variables are scoped down into the sub runtime
            q = (
                f'$foo=5 tee '
                f'{{ [ inet:asn=3 ] }} '
                f'{{ [ inet:asn=4 ] $lib.print("made asn node: {{node}}", node=$node) }} '
                f'{{ [ inet:asn=$foo ] }}'
            )
            msgs = await core.stormlist(q)
            self.stormIsInPrint("made asn node: Node{(('inet:asn', 4)", msgs)
            podes = [m[1] for m in msgs if m[0] == 'node']
            self.eq({('inet:asn', 3), ('inet:asn', 4), ('inet:asn', 5)},
                    {p[0] for p in podes})

            # lift a non-existent node and feed to tee.
            q = 'inet:fqdn=newp.com tee { inet:ipv4=1.2.3.4 } { inet:ipv4 -> * }'
            nodes = await core.nodes(q)
            self.len(2, nodes)
            exp = {
                ('inet:asn', 0),
                ('inet:ipv4', 0x01020304),
            }
            self.eq(exp, {x.ndef for x in nodes})

            q = 'tee'
            await self.asyncraises(s_exc.StormRuntimeError, core.nodes(q))

    async def test_storm_yieldvalu(self):

        async with self.getTestCore() as core:

            nodes = await core.nodes('[ inet:ipv4=1.2.3.4 ]')

            buid0 = nodes[0].buid
            iden0 = s_common.ehex(buid0)

            nodes = await core.nodes('yield $foo', opts={'vars': {'foo': (iden0,)}})
            self.len(1, nodes)
            self.eq(nodes[0].ndef, ('inet:ipv4', 0x01020304))

            def genr():
                yield iden0

            async def agenr():
                yield iden0

            nodes = await core.nodes('yield $foo', opts={'vars': {'foo': (iden0,)}})
            self.len(1, nodes)
            self.eq(nodes[0].ndef, ('inet:ipv4', 0x01020304))

            nodes = await core.nodes('yield $foo', opts={'vars': {'foo': buid0}})
            self.len(1, nodes)
            self.eq(nodes[0].ndef, ('inet:ipv4', 0x01020304))

            nodes = await core.nodes('yield $foo', opts={'vars': {'foo': genr()}})
            self.len(1, nodes)
            self.eq(nodes[0].ndef, ('inet:ipv4', 0x01020304))

            nodes = await core.nodes('yield $foo', opts={'vars': {'foo': agenr()}})
            self.len(1, nodes)
            self.eq(nodes[0].ndef, ('inet:ipv4', 0x01020304))

            nodes = await core.nodes('yield $foo', opts={'vars': {'foo': nodes[0]}})
            self.len(1, nodes)
            self.eq(nodes[0].ndef, ('inet:ipv4', 0x01020304))

            nodes = await core.nodes('yield $foo', opts={'vars': {'foo': None}})
            self.len(0, nodes)

            with self.raises(s_exc.BadLiftValu):
                await core.nodes('yield $foo', opts={'vars': {'foo': 'asdf'}})

    async def test_storm_splicelist(self):

        async with self.getTestCoreAndProxy() as (core, prox):

            mesgs = await core.stormlist('[ test:str=foo ]')
            await asyncio.sleep(0.01)

            mesgs = await core.stormlist('[ test:str=bar ]')

            tick = mesgs[0][1]['tick']
            tickdt = datetime.datetime.utcfromtimestamp(tick / 1000.0)
            tickstr = tickdt.strftime('%Y/%m/%d %H:%M:%S.%f')

            tock = mesgs[-1][1]['tock']
            tockdt = datetime.datetime.utcfromtimestamp(tock / 1000.0)
            tockstr = tockdt.strftime('%Y/%m/%d %H:%M:%S.%f')

            await asyncio.sleep(0.01)
            mesgs = await core.stormlist('[ test:str=baz ]')

            nodes = await core.nodes(f'splice.list')
            self.len(9, nodes)

            nodes = await core.nodes(f'splice.list --mintimestamp {tick}')
            self.len(4, nodes)

            nodes = await core.nodes(f'splice.list --mintime "{tickstr}"')
            self.len(4, nodes)

            nodes = await core.nodes(f'splice.list --maxtimestamp {tock}')
            self.len(7, nodes)

            nodes = await core.nodes(f'splice.list --maxtime "{tockstr}"')
            self.len(7, nodes)

            nodes = await core.nodes(f'splice.list --mintimestamp {tick} --maxtimestamp {tock}')
            self.len(2, nodes)

            nodes = await core.nodes(f'splice.list --mintime "{tickstr}" --maxtime "{tockstr}"')
            self.len(2, nodes)

            await self.asyncraises(s_exc.StormRuntimeError, core.nodes('splice.list --mintime badtime'))
            await self.asyncraises(s_exc.StormRuntimeError, core.nodes('splice.list --maxtime nope'))

            visi = await prox.addUser('visi', passwd='secret')

            await prox.addUserRule(visi['iden'], (True, ('node', 'add')))
            await prox.addUserRule(visi['iden'], (True, ('node', 'prop', 'set')))

            async with core.getLocalProxy(user='visi') as asvisi:

                # make sure a normal user only gets their own splices
                nodes = await alist(asvisi.eval("[ test:str=hehe ]"))

                nodes = await alist(asvisi.eval("splice.list"))
                self.len(2, nodes)

                # should get all splices now as an admin
                await prox.setUserAdmin(visi['iden'], True)

                nodes = await alist(asvisi.eval("splice.list"))
                self.len(11, nodes)

    async def test_storm_spliceundo(self):

        async with self.getTestCoreAndProxy() as (core, prox):

            await core.addTagProp('risk', ('int', {'minval': 0, 'maxval': 100}), {'doc': 'risk score'})

            visi = await prox.addUser('visi', passwd='secret')

            await prox.addUserRule(visi['iden'], (True, ('node', 'add')))
            await prox.addUserRule(visi['iden'], (True, ('node', 'prop', 'set')))
            await prox.addUserRule(visi['iden'], (True, ('node', 'tag', 'add')))

            async with core.getLocalProxy(user='visi') as asvisi:

                nodes = await alist(asvisi.eval("[ test:str=foo ]"))
                await asyncio.sleep(0.01)

                mesgs = await alist(asvisi.storm("[ test:str=bar ]"))
                tick = mesgs[0][1]['tick']
                tock = mesgs[-1][1]['tock']

                mesgs = await alist(asvisi.storm("test:str=bar [ +#test.tag ]"))

                # undo a node add

                nodes = await alist(asvisi.eval("test:str=bar"))
                self.len(1, nodes)

                # undo adding a node fails without tag:del perms if is it tagged
                q = f'splice.list --mintimestamp {tick} --maxtimestamp {tock} | splice.undo'
                await self.agenraises(s_exc.AuthDeny, asvisi.eval(q))

                await prox.addUserRule(visi['iden'], (True, ('node', 'tag', 'del')))

                # undo adding a node fails without node:del perms
                q = f'splice.list --mintimestamp {tick} --maxtimestamp {tock} | splice.undo'
                await self.agenraises(s_exc.AuthDeny, asvisi.eval(q))

                await prox.addUserRule(visi['iden'], (True, ('node', 'del')))
                nodes = await alist(asvisi.eval(q))

                nodes = await alist(asvisi.eval("test:str=bar"))
                self.len(0, nodes)

                # undo a node delete

                # undo deleting a node fails without node:add perms
                await prox.delUserRule(visi['iden'], (True, ('node', 'add')))

                q = 'splice.list | limit 2 | splice.undo'
                await self.agenraises(s_exc.AuthDeny, asvisi.eval(q))

                await prox.addUserRule(visi['iden'], (True, ('node', 'add')))
                nodes = await alist(asvisi.eval(q))

                nodes = await alist(asvisi.eval("test:str=bar"))
                self.len(1, nodes)

                # undo adding a prop

                nodes = await alist(asvisi.eval("test:str=foo [ :tick=2000 ]"))
                self.nn(nodes[0][1]['props'].get('tick'))

                # undo adding a prop fails without prop:del perms
                q = 'splice.list | limit 1 | splice.undo'
                await self.agenraises(s_exc.AuthDeny, asvisi.eval(q))

                await prox.addUserRule(visi['iden'], (True, ('node', 'prop', 'del',)))
                nodes = await alist(asvisi.eval(q))

                nodes = await alist(asvisi.eval("test:str=foo"))
                self.none(nodes[0][1]['props'].get('tick'))

                # undo updating a prop

                nodes = await alist(asvisi.eval("test:str=foo [ :tick=2000 ]"))
                oldv = nodes[0][1]['props']['tick']
                self.nn(oldv)

                nodes = await alist(asvisi.eval("test:str=foo [ :tick=3000 ]"))
                self.ne(oldv, nodes[0][1]['props']['tick'])

                # undo updating a prop fails without prop:set perms
                await prox.delUserRule(visi['iden'], (True, ('node', 'prop', 'set')))

                q = 'splice.list | limit 1 | splice.undo'
                await self.agenraises(s_exc.AuthDeny, asvisi.eval(q))

                await prox.addUserRule(visi['iden'], (True, ('node', 'prop', 'set',)))
                nodes = await alist(asvisi.eval(q))

                nodes = await alist(asvisi.eval("test:str=foo"))
                self.eq(oldv, nodes[0][1]['props']['tick'])

                # undo deleting a prop

                nodes = await alist(asvisi.eval("test:str=foo [ -:tick ]"))
                self.none(nodes[0][1]['props'].get('tick'))

                # undo deleting a prop fails without prop:set perms
                await prox.delUserRule(visi['iden'], (True, ('node', 'prop', 'set')))

                q = 'splice.list | limit 1 | splice.undo'
                await self.agenraises(s_exc.AuthDeny, asvisi.eval(q))

                await prox.addUserRule(visi['iden'], (True, ('node', 'prop', 'set')))
                nodes = await alist(asvisi.eval(q))

                nodes = await alist(asvisi.eval("test:str=foo"))
                self.eq(oldv, nodes[0][1]['props']['tick'])

                # undo adding a tag

                nodes = await alist(asvisi.eval("test:str=foo [ +#rep=2000 ]"))
                tagv = nodes[0][1]['tags'].get('rep')
                self.nn(tagv)

                # undo adding a tag fails without tag:del perms
                await prox.delUserRule(visi['iden'], (True, ('node', 'tag', 'del',)))

                q = 'splice.list | limit 1 | splice.undo'
                await self.agenraises(s_exc.AuthDeny, asvisi.eval(q))

                await prox.addUserRule(visi['iden'], (True, ('node', 'tag', 'del',)))
                nodes = await alist(asvisi.eval(q))

                nodes = await alist(asvisi.eval("test:str=foo"))
                self.none(nodes[0][1]['tags'].get('rep'))

                # undo deleting a tag

                # undo deleting a tag fails without tag:add perms
                await prox.delUserRule(visi['iden'], (True, ('node', 'tag', 'add')))

                q = 'splice.list | limit 1 | splice.undo'
                await self.agenraises(s_exc.AuthDeny, asvisi.eval(q))

                await prox.addUserRule(visi['iden'], (True, ('node', 'tag', 'add')))
                nodes = await alist(asvisi.eval(q))

                nodes = await alist(asvisi.eval("test:str=foo"))
                self.eq(tagv, nodes[0][1]['tags'].get('rep'))

                # undo updating a tag
                nodes = await alist(asvisi.eval("test:str=foo [ +#rep=2000 ]"))
                oldv = nodes[0][1]['tags'].get('rep')
                nodes = await alist(asvisi.eval("test:str=foo [ +#rep=3000 ]"))
                self.ne(oldv, nodes[0][1]['tags'].get('rep'))

                q = 'splice.list | limit 1 | splice.undo'
                await alist(asvisi.eval(q))

                nodes = await alist(asvisi.eval("test:str=foo"))
                self.eq(oldv, nodes[0][1]['tags'].get('rep'))

                # undo adding a tagprop

                nodes = await alist(asvisi.eval("test:str=foo [ +#rep:risk=50 ]"))
                tagv = nodes[0][1]['tagprops']['rep'].get('risk')
                self.nn(tagv)

                # undo adding a tagprop fails without tag:del perms
                await prox.delUserRule(visi['iden'], (True, ('node', 'tag', 'del')))

                q = 'splice.list | limit 1 | splice.undo'
                await self.agenraises(s_exc.AuthDeny, asvisi.eval(q))

                await prox.addUserRule(visi['iden'], (True, ('node', 'tag', 'del')))
                nodes = await alist(asvisi.eval(q))

                nodes = await alist(asvisi.eval("test:str=foo"))
                self.none(nodes[0][1]['tagprops'].get('rep'))

                # undo deleting a tagprop

                # undo deleting a tagprop fails without tag:add perms
                await prox.delUserRule(visi['iden'], (True, ('node', 'tag', 'add')))

                q = 'splice.list | limit 1 | splice.undo'
                await self.agenraises(s_exc.AuthDeny, asvisi.eval(q))

                await prox.addUserRule(visi['iden'], (True, ('node', 'tag', 'add')))
                nodes = await alist(asvisi.eval(q))

                nodes = await alist(asvisi.eval("test:str=foo"))
                self.eq(tagv, nodes[0][1]['tagprops']['rep'].get('risk'))

                # undo updating a tagprop

                nodes = await alist(asvisi.eval("test:str=foo [ +#rep:risk=0 ]"))
                self.ne(tagv, nodes[0][1]['tagprops']['rep'].get('risk'))

                # undo updating a tagprop fails without prop:set perms
                await prox.delUserRule(visi['iden'], (True, ('node', 'tag', 'add')))

                q = 'splice.list | limit 1 | splice.undo'
                await self.agenraises(s_exc.AuthDeny, asvisi.eval(q))

                await prox.addUserRule(visi['iden'], (True, ('node', 'tag', 'add')))
                nodes = await alist(asvisi.eval(q))

                nodes = await alist(asvisi.eval("test:str=foo"))
                self.eq(tagv, nodes[0][1]['tagprops']['rep'].get('risk'))

                # sending nodes of form other than syn:splice doesn't work
                q = 'test:str | limit 1 | splice.undo'
                await self.agenraises(s_exc.StormRuntimeError, asvisi.eval(q))

                # must be admin to use --force for node deletion
                await alist(asvisi.eval('[ test:cycle0=foo :cycle1=bar ]'))
                await alist(asvisi.eval('[ test:cycle1=bar :cycle0=foo ]'))

                nodes = await alist(asvisi.eval("test:cycle0"))
                self.len(1, nodes)

                q = 'splice.list | +:type="node:add" +:form="test:cycle0" | limit 1 | splice.undo'
                await self.agenraises(s_exc.CantDelNode, asvisi.eval(q))

                q = 'splice.list | +:type="node:add" +:form="test:cycle0" | limit 1 | splice.undo --force'
                await self.agenraises(s_exc.AuthDeny, asvisi.eval(q))

                await prox.setUserAdmin(visi['iden'], True)

                nodes = await alist(asvisi.eval(q))
                nodes = await alist(asvisi.eval("test:cycle0"))
                self.len(0, nodes)
