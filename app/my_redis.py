import time
import socket
import threading

from storage import Storage
from rdb_parser import RDBParser
from resp_decoder import RESPDecoder
from command_processor import CommandProcessor


class Redis:
    def __init__(self, args):
        self.storage = Storage()
        self.port = int(args.port)
        self.rdb_parser = RDBParser(args.dir, args.db_filename)
        self.role = "master" if args.master_addr is None else "slave"
        self.master_host, self.master_port = "", ""
        if args.master_addr:
            self.master_host, self.master_port = args.master_addr.split(" ")

        self.replicas = []
        self.queues = dict()
        self.updated_replica_cnt = 0
        self.any_set_cmd = False

    def connect_to_master(self):
        replica_to_master_conn = socket.create_connection(
            (
                self.master_host,
                int(self.master_port),
            )
        )
        threading.Thread(
            target=self.handle_master_connection,
            args=(replica_to_master_conn,),
            daemon=True,
        ).start()

    def start_server(self):
        server_socket = socket.create_server(
            ("localhost", int(self.port)), reuse_port=True
        )
        while True:
            conn, _ = server_socket.accept()
            threading.Thread(
                target=self.handle_connection, args=(conn,)
            ).start()

    def handle_connection(self, conn: socket.socket):
        with conn:
            cmd_processor = CommandProcessor(conn, self)

            while True:
                decoded = RESPDecoder(conn).decode()

                if decoded is None:
                    break

                if isinstance(decoded, bytes):
                    command = decoded.lower()
                    args = None
                else:
                    command = decoded[0].lower()
                    args = decoded[1:]

                ret = cmd_processor.process_command(command, args)
                if ret is not None:
                    conn.send(ret.encode())

    def handle_master_connection(self, conn: socket.socket):
        number_of_bytes_processed = 0
        decoder = RESPDecoder(conn)

        conn.send("*1\r\n$4\r\nping\r\n".encode())
        decoder.decode_simple_string()

        conn.send(
            f"*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n{self.port}\r\n".encode()
        )
        decoder.decode_simple_string()

        conn.send(
            "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n".encode()
        )
        decoder.decode_simple_string()

        conn.send("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n".encode())

        decoder.decode_simple_string()
        decoder.decode_empty_rdb_file()

        while True:
            decoded = decoder.decode()
            if decoded is None:
                break

            if isinstance(decoded, bytes):
                command = decoded.lower()
                args = None
            else:
                command = decoded[0].lower()
                args = decoded[1:]

            if command.decode() == "ping":
                number_of_bytes_processed += len(b"*1\r\n$4\r\4PING\r\n")
            elif command.decode() == "set":
                key = args[0].decode()
                val = args[1].decode()

                exp = None
                delta = None

                if len(args) > 2 and args[2].decode().lower() == "px":
                    delta = args[3].decode()
                    exp = float(delta)
                self.storage.set(key, val, exp)

                processed_cmd = f"*3\r\n$3\r\nSET\r\n${len(key)}\r\n{key}\r\n${len(val)}\r\n{val}\r\n"
                if exp:
                    processed_cmd = f"*5\r\n$3\r\nSET\r\n${len(key)}\r\n{key}\r\n${len(val)}\r\n{val}\r\n$2\r\npx\r\n${len(delta)}\r\n{delta}\r\n"

                number_of_bytes_processed += len(processed_cmd)

            elif command.decode() == "replconf":
                processed_cmd = str(number_of_bytes_processed)
                conn.send(
                    f"*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n${len(processed_cmd)}\r\n{processed_cmd}\r\n".encode()
                )

                number_of_bytes_processed += len(
                    b"*3\r\n$8\r\nreplconf\r\n$6\r\ngetack\r\n$1\r\n*\r\n"
                )
