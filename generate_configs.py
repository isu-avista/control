import uuid
import yaml
import argparse
import os


def init_argparse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        usage="%(prog)s [OPTIONS] ...",
        description="Generate configuration files for Avista Control`",
        epilog="Copyright (C) 2020, 2021 Idaho State University Empirical SE Lab"
    )
    parser.add_argument("-p", "--periodicity", action="store", type=str, required=True, help="DBMS Username")
    parser.add_argument("-s", "--hostname", action="store", type=str, required=True, help="Server Host Name")
    parser.add_argument("-r", "--hostport", action="store", type=str, required=True, help="Server Host Port")
    parser.add_argument("-v", "--version", action="version", version=f'{parser.prog} version 1.0.0')

    return parser


def generate_server_config(periodicity, hostname, hostport):
    config = {
        'service': {
            'host': hostname,
            'port': hostport,
            'periodicity': periodicity,
        }
    }

    with open('conf/config.yml', 'w') as outfile:
        yaml.dump(config, outfile, default_flow_style=False)


def main() -> None:
    parser = init_argparse()
    args = parser.parse_args()
    dic = vars(args)

    periodicity = dic['periodicity']
    host = dic['hostname']
    port = dic['hostport']

    generate_server_config(periodicity, host, port)


if __name__ == '__main__':
    main()
