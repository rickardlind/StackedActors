import kore

import inspect
import logging


def message(method):
    """
    Decorator for actor methods that turn them into messages.
    """
    def decorator(self, *args, **kwargs):
        if self._mailbox:
            self._mailbox.push([method, args, kwargs])

    return decorator


class Actor:
    """
    Base class for actors.
    """
    def __init__(self):
        self._mailbox = kore.queue()
        self._waiting = []

        kore.task_create(self._dispatch(self._mailbox))


    def die(self):
        """
        Tell Actor do die.
        """
        if self._mailbox:
            self._mailbox.push([None, None, None])
            self._mailbox = None


    async def dead(self):
        """
        Wait until Actor is dead.
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

        while True:
            try:
                method, args, kwargs = await mailbox.pop()

                if method is None:
                    break

                if inspect.iscoroutinefunction(method):
                    logging.debug(f'--> await {name}.{method.__name__}')
                    await method(self, *args, **kwargs)
                else:
                    logging.debug(f'--> call {name}.{method.__name__}')
                    method(self, *args, **kwargs)

                logging.debug(f'<-- {name}.{method.__name__}')

            except:
                logging.error(f'Exception in {name}._dispatch', exc_info=True)


        for queue in self._waiting:
            queue.push(None)

        self._waiting = None
