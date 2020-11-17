from aoidb.server import run_server
from sys import argv
run_server(argv[-1].replace("'",'"'))