import kore

import inspect
import logging


def message(method):
    """
    Decorator for actor methods that turn them into messages.
    """
    def decorator(self, *args, **kwargs):
        if self._mailbox:
            self._mailbox.push([method, args, kwargs, None])

    return decorator


def response(method):
    """
    Decorator for actor methods that waits for a response.
    """
    async def decorator(self, *args, **kwargs):
        if self._mailbox:
            queue = kore.queue()
            self._mailbox.push([method, args, kwargs, queue])
            rv = await queue.pop()

            if isinstance(rv, Exception):
                raise rv

            return rv

    return decorator


class Cancelled(Exception):
    """
    Message cancelled.
    """
    pass


class Actor:
    """
    Base class for actors.
    """
    def __init__(self, oneshot=False):
        self._mailbox = None
        self._waiting = None
        self._cancelled = False
        self._oneshot = oneshot

        self.start()


    def start(self):
        """
        Start actor.
        """
        assert self._mailbox is None and self._waiting is None

        self._mailbox = kore.queue()
        self._waiting = []

        kore.task_create(self._dispatch(self._mailbox))


    async def stop(self, cancel=False):
        """
        Stop Actor and wait until it is finished.
        """
        if self._mailbox is not None:
            if cancel:
                self._cancelled = True
            else:
                self._mailbox.push([None, None, None, None])

            self._mailbox = None

        await self.finished()


    async def finished(self):
        """
        Wait for Actor to finish.
        """
        if self._waiting is not None:
            queue = kore.queue()
            self._waiting.append(queue)
            await queue.pop()


    async def _dispatch(self, mailbox):
        """
        Turn message back into method call.
        """
        name = self.__class__.__name__
        kore.coroname(name)

        while not self._cancelled:
            try:
                method, args, kwargs, queue = await mailbox.pop()

                if method is None:
                    break

                if inspect.iscoroutinefunction(method):
                    logging.debug(f'--> await {name}.{method.__name__}')
                    rv = await method(self, *args, **kwargs)
                else:
                    logging.debug(f'--> call {name}.{method.__name__}')
                    rv = method(self, *args, **kwargs)

                logging.debug(f'<-- {name}.{method.__name__}: rv={rv}')

            except Exception as ex:
                logging.error(f'Exception in {name}._dispatch', exc_info=True)
                rv = ex

            if queue is not None:
                queue.push(rv)

            if self._oneshot:
                break

        for queue in self._drain(mailbox):
            queue.push(Cancelled())

        for queue in self._waiting:
            queue.push(None)

        self._waiting = None


    def _drain(self, mailbox):
        """
        Drain messages from mailbox.
        """
        msg = mailbox.popnow()

        while msg is not None:
            if msg[3] is not None:
                yield msg[3]

            msg = mailbox.popnow()
