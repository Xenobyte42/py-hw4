import socket
import uuid
import pickle
import os.path
import argparse
import time


class ServerError(Exception):
    pass


class ParserError(ServerError):
    pass


class Task:

    def __init__(self, task_id, length, data, timeout):
        self.task_id = task_id
        self.length = length
        self.data = data
        self.timeout = timeout
        self.is_waiting = False
        self.start_wait_time = None

    def update(self):
        if self.start_wait_time is not None:
            if (int(time.time() - self.start_wait_time) >= self.timeout):
                self.is_waiting = False
                self.start_wait_time = None

    def start_wait(self):
        self.start_wait_time = time.time()
        self.is_waiting = True


class TaskQueue:

    def __init__(self):
        self._tasks = []

    def add_task(self, task):
        self._tasks.append(task)

    def get_task(self):
        for task in self._tasks:
            task.update()
            if not task.is_waiting:
                task.start_wait()
                return task
        return None

    def in_task(self, task_id):
        for task in self._tasks:
            if task.task_id == task_id:
                return True
        return False

    def ack_task(self, task_id):
        for idx, task in enumerate(self._tasks):
            if task.task_id == task_id:
                task.update()
                if task.is_waiting:
                    del self._tasks[idx]
                    return True
                else:
                    return False
        return False


class QueueDict:

    def __init__(self):
        self._queue_dict = {}

    def add_task(self, queue_name, task):
        if queue_name not in self._queue_dict:
            self._queue_dict[queue_name] = TaskQueue()
        self._queue_dict[queue_name].add_task(task)
        return task.task_id.encode()

    def get_task(self, queue_name):
        if queue_name not in self._queue_dict:
            return b'NONE'
        task = self._queue_dict[queue_name].get_task()
        if task:
            answer = "{} {} {}".format(task.task_id,
                                       task.length, task.data).encode()
            return answer
        else:
            return b'NONE'

    def in_task(self, queue_name, task_id):
        if queue_name not in self._queue_dict:
            return b'ERROR'
        if self._queue_dict[queue_name].in_task(task_id):
            return b'YES'
        else:
            return b'NO'

    def ack_task(self, queue_name, task_id):
        if queue_name not in self._queue_dict:
            return b'ERROR'
        if self._queue_dict[queue_name].ack_task(task_id):
            return b'YES'
        else:
            return b'NO'

    def init_from_base(self, path):
        path = os.path.join(path, 'server.log')
        if os.path.isfile(path) and os.path.getsize(path) > 0:
            with open(path, 'rb') as base:
                self._queue_dict = pickle.load(base)

    def save_to_drive(self, path):
        with open(os.path.join(path, 'server.log'), 'wb') as base:
            pickle.dump(self._queue_dict, base)
        return b'OK'


class TaskQueueServer:

    def __init__(self, ip=None, port=None, path=None,
                 timeout=None, max_connect=None):
        self._host = ip or '0.0.0.0'
        self._port = port or 5555
        self._path = path or 'server.log'
        self._timeout = timeout or 300
        self._max_connect = max_connect or 10
        self._queues = QueueDict()
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    def recv_add(self, params, conn):
        if len(params) != 4:
            raise ParserError

        while len(params[3]) < int(params[2]):
            try:
                data = conn.recv(1024).decode()
            except UnicodeDecodeError:
                raise ParserError
            params[3] += data

        task = Task(str(uuid.uuid4()), params[2], params[3], self._timeout)
        answer = self._queues.add_task(params[1], task)
        return answer

    def recv_get(self, params):
        if len(params) != 2:
            raise ParserError

        answer = self._queues.get_task(params[1])
        return answer

    def recv_ack(self, params):
        if len(params) != 3:
            raise ParserError

        answer = self._queues.ack_task(params[1], params[2])
        return answer

    def recv_in(self, params):
        if len(params) != 3:
            raise ParserError

        answer = self._queues.in_task(params[1], params[2])
        return answer

    def save_to_drive(self, params):
        if len(params) != 1:
            raise ParserError

        answer = self._queues.save_to_drive(self._path)
        return answer

    def make_answer(self, data, conn):
        params = data.split()
        answer = b'ERROR'
        if params[0] == 'ADD':
            answer = self.recv_add(params, conn)
        elif params[0] == 'GET':
            answer = self.recv_get(params)
        elif params[0] == 'ACK':
            answer = self.recv_ack(params)
        elif params[0] == 'IN':
            answer = self.recv_in(params)
        elif params[0] == 'SAVE':
            answer = self.save_to_drive(params)
        return answer

    def server_recv(self, data, conn):
        try:
            data = data.decode()
        except UnicodeDecodeError:
            return b'ERROR'

        try:
            answer = self.make_answer(data, conn)
        except ParserError:
            return b'ERROR'
        return answer

    def run(self):
        try:
            self._queues.init_from_base(self._path)
        except FileNotFoundError:
            print("BaseFile is not found!")
        self._sock.bind((self._host, self._port))
        self._sock.listen(self._max_connect)
        while True:
            try:
                conn, _ = self._sock.accept()
                data = conn.recv(1024)
                answer = self.server_recv(data, conn)
                conn.send(answer)
                conn.close()
            except KeyboardInterrupt:
                break
        self._sock.close()


def parse_args():
    parser = argparse.ArgumentParser(description='This is a simple \
                                     task queue server with custom protocol')
    parser.add_argument(
        '-p',
        action="store",
        dest="port",
        type=int,
        default=5555,
        help='Server port')
    parser.add_argument(
        '-i',
        action="store",
        dest="ip",
        type=str,
        default='0.0.0.0',
        help='Server ip adress')
    parser.add_argument(
        '-c',
        action="store",
        dest="path",
        type=str,
        default='./',
        help='Server checkpoints dir')
    parser.add_argument(
        '-t',
        action="store",
        dest="timeout",
        type=int,
        default=300,
        help='Task maximum GET timeout in seconds')
    return parser.parse_args()


if __name__ == '__main__':
    ARGS = parse_args()
    SERVER = TaskQueueServer(**ARGS.__dict__)
    SERVER.run()
