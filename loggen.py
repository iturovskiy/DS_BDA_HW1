from datetime import datetime, timedelta
from os import path
import os
import argparse
import random
import shutil


def generate_devices(num):
    return {i: f"cpu{i}" for i in range(num)}


def generate_log_str(device: int, timestamp: int):
    return f"{device},{timestamp},{5 * random.randint(1, 100)}\n"


def generate_corrupted_log_str(device: int, timestamp: int):
    val = random.randint(0, 1000) % 3
    if val == 0:
        return "totally corrupted string"
    elif val == 1:
        return f"{(device + 1) * 10000},{timestamp},{random.randint(1, 100)}\n"
    elif val == 2:
        return f"{device},-1,{random.randint(1, 100)}\n"


def generate(args):
    try:
        shutil.rmtree(path.abspath(args.output))
    except FileNotFoundError:
        pass
    os.mkdir(path.abspath(args.output))

    now = datetime.now()
    begin = int((now - timedelta(hours=args.hours)).timestamp())
    end = int(now.timestamp())

    devices = generate_devices(args.devices)
    with open(path.abspath(path.join(args.output, "mapping")), "w+") as f:
        for i, dev in devices.items():
            f.write(f"{i},{dev}\n")

    with open(path.abspath(path.join(args.output, "input")), "w+") as f:
        for stamp in range(begin, end, args.period):
            for dev in devices:
                if random.random() < args.corrupted:
                    f.write(generate_corrupted_log_str(dev, stamp))
                    continue
                f.write(generate_log_str(dev, stamp))


def parse_args(foo):
    parser = argparse.ArgumentParser(description='Генератор логов')
    parser.add_argument("hours", type=int, help="Количество часов от текущего времени", default=1)
    parser.add_argument("-p", "--period", type=int, help="Периодичность появления новых записей, c", default=1)
    parser.add_argument("-c", "--corrupted", type=float, help="Процент некорректных записей", default=0.1)
    parser.add_argument("-d", "--devices", type=int, help="Количество устройств", default=2)
    parser.add_argument("-o", "--output", type=str, help="Директория для записи результата", default="./input")
    parser.set_defaults(run=foo)

    args = parser.parse_args()
    if not (args.hours > 0):
        raise argparse.ArgumentTypeError(f"hours must be greater than 0: now {args.hours}")
    if not (args.period > 0):
        raise argparse.ArgumentTypeError(f"period must be greater than 0: now {args.period}")
    if not (0 <= args.corrupted <= 1):
        raise argparse.ArgumentTypeError(f"corrupted must be in range 0 to 1.0: now {args.hours}")
    if not (args.devices > 0):
        raise argparse.ArgumentTypeError(f"devices number must be greater than 0: now {args.devices}")
    return args


if __name__ == '__main__':
    parsed = parse_args(generate)
    parsed.run(parsed)
