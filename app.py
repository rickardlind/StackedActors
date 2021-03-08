import kore

import logging
import sys

from xml.dom.minidom import parseString

from actor import *


class Rss(Actor):
    """
    Data model.
    """
    URL = {
        'lwn': 'https://lwn.net/headlines/rss',
        'bad': 'https://badnews.com/rss',
    }

    @message
    async def query(self, site: str, sender: Actor):
        try:
            url = self.URL[site]
            client = kore.httpclient(url)
            status, body = await client.get()

            if status == 200:
                dom = parseString(body.decode())
                title = dom.getElementsByTagName('item').item(0).getElementsByTagName('title').item(0).childNodes.item(0).wholeText
                sender.news_update(site, title)
                return

        except:
            logging.error(f'Error getting news from {site}', exc_info=True)

        sender.news_unavailable(site)
        
        

class Model(Actor):
    """
    Data model.
    """
    def __init__(self):
        self.rss = Rss()
        self.pending = {}
        super().__init__()

        
    @message
    def latest_news(self, site: str, sender: Actor):
        """
        Request latest news.
        """
        waiting = self.pending.setdefault(site, set())

        if not waiting:
            self.rss.query(site, self)

        waiting.add(sender)

        
    @message    
    def news_update(self, site: str, news: str):
        """
        Received news update from site.
        """
        for sender in self.pending.pop(site, []):
            sender.success(news + '\n')

        
    @message    
    def news_unavailable(self, site: str):
        """
        Site currently unavailable.
        """
        for sender in self.pending.pop(site, []):
            sender.failure('no news is bad news\n')
        

class Response(Actor):
    """
    One trick Actor.
    """
    def __init__(self, req, site: str, model: Model):
        self.req = req
        super().__init__()
        model.latest_news(site, self)


    @message
    def success(self, msg: str):
        self.req.response(200, msg.encode())
        self.die()

        
    @message
    def failure(self, err: str):
        self.req.response(500, err.encode())
        self.die()


class App:
    def configure(self, args):
        try:
            FORMAT = '%(asctime)-15s %(levelname)s: %(message)s'
            logging.basicConfig(format=FORMAT, level='DEBUG', stream=sys.stderr)

            kore.setname('News')

            self.model = Model()

            kore.config.workers         = 1
            kore.config.deployment      = 'dev'
            kore.config.seccomp_tracing = 'yes'

            kore.server('default', ip='127.0.0.1', port='4711', tls=False)

            d = kore.domain('*', attach='default')

            d.route(r'^/(lwn|bad)$', self.news, methods=['get'])

        except Exception as e:
            logging.error('configure failed', exc_info=True)
            raise


    async def news(self, req, site):
        rsp = Response(req, site, self.model)
        await rsp.dead()


koreapp = App()
