import time
from functools import wraps
from queue import Queue
from threading import Thread
from typing import List

class ThreadMaker(Thread):
    def __init__(self, thread_name: str, task_queue: Queue, callback, result_queue, args=(), kwargs={}):
        super(ThreadMaker, self).__init__(name=thread_name)
        self.task_queue = task_queue
        self.callback = callback
        self.result_queue = result_queue
        self.args = args
        self.kwargs = kwargs

    def run(self):
        while not self.task_queue.empty():
            task = self.task_queue.get()
            result = self.callback(task, *self.args, **self.kwargs)
            self.result_queue.put(result)


class ThreadWorker:
    def __init__(self, tasks: List = [], thread_count: int = 1, args=(), kwargs={}):
        self.tasks = tasks
        self.thread_count = thread_count
        self.args = args
        self.kwargs = kwargs
        self.task_queue = Queue()
        self.result_queue = Queue()
        for task in tasks:
            self.task_queue.put(task)

    def __call__(self, func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            threads = []
            for i in range(self.thread_count):
                t = ThreadMaker(
                    f"ThreadMaker-{i}",
                    self.task_queue,
                    func,
                    self.result_queue,
                    args=self.args,
                    kwargs=self.kwargs,
                )
                threads.append(t)
                t.start()

            for t in threads:
                t.join()

            results = []

            while not self.result_queue.empty():
                result = self.result_queue.get()
                results.append(result)

            return results

        return wrapper
