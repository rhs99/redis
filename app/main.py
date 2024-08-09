import argparse

from my_redis import Redis


def main(args):
    redis = Redis(args)

    if redis.role == "slave":
        redis.connect_to_master()

    redis.start_server()


if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("--port", dest="port", default=6379)
    arg_parser.add_argument("--replicaof", dest="master_addr", default=None)
    arg_parser.add_argument("--dir", dest="dir", default="/tmp")
    arg_parser.add_argument(
        "--dbfilename",
        dest="db_filename",
        default="dump.rdb",
    )
    args = arg_parser.parse_args()

    main(args)
