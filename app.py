import kore

import json
import logging
import sys
import time

from xml.dom.minidom import parseString

from actor import *


class Rss(Actor):
    """
    Query site for RSS feed.
    """
    def __init__(self, site: str, url: str):
        self._site = site
        self._url  = url
        super().__init__()


    @message
    async def query(self, sender: Actor):
        try:
            client = kore.httpclient(self._url)
            status, body = await client.get()

            if status == 200:
                dom   = parseString(body.decode())
                item  = dom.getElementsByTagName('item').item(0)
                title = item.getElementsByTagName('title').item(0)
                text  = title.childNodes.item(0).wholeText

                sender.news_update(self._site, text)
                return

        except:
            logging.error(f'Error getting news from {self._site}', exc_info=True)

        sender.news_unavailable(self._site)


def now() -> float:
    """
    Get current time.
    """
    return time.clock_gettime(time.CLOCK_MONOTONIC)


class Entry:
    """
    Site state.
    """
    def __init__(self, site: str, url: str):
        self.site    = site
        self.url     = url
        self.latest  = None
        self.expires = None
        self.rss     = None
        self.pending = False
        self.waiting = []


    @classmethod
    def load(cls, path: str):
        with open(path, 'r') as fp:
            for d in json.load(fp):
                yield cls(**d)


class Model(Actor):
    """
    Data model.
    """
    TTL = 5 * 60

    def __init__(self, path: str):
        self._sites = { e.site: e for e in Entry.load(path) }
        super().__init__()


    async def stop(self):
        await super().stop()

        for entry in self._sites.values():
            if entry.rss is not None:
                await entry.rss.stop()


    @response
    def status(self) -> dict:
        return {
            e.site: {
                'latest': e.latest
            }
            for e in self._sites.values()
        }


    @response
    def reset(self):
        for e in self._sites.values():
            e.latest = None
            e.expires = None


    @message
    def latest_news(self, site: str, sender: Actor):
        """
        Request latest news.
        """
        entry = self._sites.get(site)

        if entry is None:
            sender.failure(f'unknown site {site}\n')

        elif entry.latest is not None and now() < entry.expires:
            logging.debug(f'using cached data for {site}')
            sender.success(entry.latest + '\n')

        else:
            if entry.rss is None:
                entry.rss = Rss(entry.site, entry.url)

            if not entry.pending:
                entry.pending = True
                entry.rss.query(self)

            entry.waiting.append(sender)


    @message
    def news_update(self, site: str, news: str):
        """
        Received news update from site.
        """
        entry = self._sites[site]

        entry.latest  = news
        entry.expires = now() + self.TTL
        entry.pending = False

        for sender in entry.waiting:
            sender.success(news + '\n')

        entry.waiting.clear()


    @message
    def news_unavailable(self, site: str):
        """
        Site currently unavailable.
        """
        entry = self._sites[site]

        entry.latest  = None
        entry.expires = None
        entry.pending = False

        for sender in entry.waiting:
            sender.failure('no news is bad news\n')

        entry.waiting.clear()


class Response(Actor):
    """
    Send REST response.
    """
    def __init__(self, req):
        self._req = req
        super().__init__(oneshot=True)


    @message
    def success(self, msg: str):
        self._req.response(200, msg.encode())


    @message
    def failure(self, err: str):
        self._req.response(500, err.encode())


class App:
    def configure(self, args):
        try:
            FORMAT = '%(asctime)-15s %(levelname)s: %(message)s'
            logging.basicConfig(format=FORMAT, level='DEBUG', stream=sys.stderr)

            kore.setname('News')

            self.model = Model('sites.json')

            kore.config.workers         = 1
            kore.config.deployment      = 'dev'
            kore.config.seccomp_tracing = 'yes'

            kore.server('default', ip='127.0.0.1', port='4711', tls=False)

            d = kore.domain('*', attach='default')

            d.route(r'^/news/([a-zA-Z0-9]+)$', self.rest_news, methods=['get'])
            d.route(r'^/status$', self.rest_status, methods=['get'])
            d.route(r'^/reset$', self.rest_reset, methods=['post'])
            d.route(r'^/stop$', self.rest_stop, methods=['post'])

        except Exception as e:
            logging.error('configure failed', exc_info=True)
            raise


    async def rest_news(self, req, site):
        rsp = Response(req)
        self.model.latest_news(site, rsp)
        await rsp.finished()


    async def rest_status(self, req):
        d = await self.model.status()
        req.response(200, json.dumps(d, indent=4).encode())


    async def rest_reset(self, req):
        await self.model.reset()
        req.response(200, b'Reset\n')


    async def rest_stop(self, req):
        await self.model.stop()
        req.response(200, b'Stopped\n')
        kore.shutdown()


koreapp = App()
