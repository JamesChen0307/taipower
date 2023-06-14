import time
from functools import wraps
from queue import Queue
from threading import Thread
# from base.roles import Operator, Saas
from typing import List

# Reference:
# https://chase-seibert.github.io/blog/2013/12/17/python-decorator-optional-parameter.html


class threadMaker(Thread):
    def __init__(self, thread_name: str, q: Queue, callback, args=(), kwargs={}):
        super(threadMaker, self).__init__(name=thread_name)
        self.q = q
        self.callback = callback
        self.args = args
        self.kwargs = kwargs

    def run(self):
        while not self.q.empty():
            task = self.q.get()
            self.callback(task, *self.args, **self.kwargs)


class ThreadWorker:
    def __init__(self, tasks: List = [], thread: int = 1, args=(), kwargs={}):
        self.tasks = tasks
        self.thread = thread
        self.args = args
        self.kwargs = kwargs
        # init queue
        self.q = Queue()
        for x in tasks:
            self.q.put(x)

    def __call__(self, func):
        @wraps(func)
        def callable(*args, **kwargs):

            threads = []
            for i in range(self.thread):
                t = threadMaker(
                    f"threadMaker-{i}",
                    self.q,
                    func,
                    args=self.args,
                    kwargs=self.kwargs,
                )
                threads.append(t)
                t.start()

            for t in threads:
                t.join()

        return callable
