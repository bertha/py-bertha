import sys
import bertha
import argparse

def parse_args():
        parser = argparse.ArgumentParser(prog='bertha',
                description="List, store and retrieve blobs on a Bertha server")
        parser.add_argument('-H', '--host', default='localhost',
                                help='Host of the Bertha server')
        parser.add_argument('-p', '--port', type=int, default=819,
                                help='Portof the Bertha server')

        subparsers = parser.add_subparsers(help='sub-commands')
        
        parser_list = subparsers.add_parser('list', help="List blobs on server")
        parser_list.set_defaults(func=do_list)

        parser_get = subparsers.add_parser('get', help="Get a blob by hash")
        parser_get.add_argument('hash', help="Hash of blob to retreive")
        parser_get.add_argument('file', default='-', nargs='?',
                        help="Destination file")
        parser_get.set_defaults(func=do_get)

        parser_put = subparsers.add_parser('put',
                        help="Put a blob on the server")
        parser_put.add_argument('file', default='-', nargs='?',
                        help="Source file")
        parser_put.set_defaults(func=do_put)
        return parser.parse_args()

def main():
        args = parse_args()
        c = bertha.BerthaClient(args.host, args.port)
        args.func(c, args)

def do_list(c, args):
        for h in c.list():
                print h
def do_get(c, args):
        close_file = True
        if args.file == '-':
                f = sys.stdout
                close_file = False
        else:
                f = open(args.file, 'w')
        f2 = c.get(args.hash)
        while True:
                buf = f2.read(4096)
                if not buf:
                        break
                f.write(buf)
        f2.close()
        if close_file:
                f.close()

def do_put(c, args):
        close_file = True
        if args.file == '-':
                f = sys.stdin
                close_file = False
        else:
                f = open(args.file, 'r')
        print c.put_file(f)
        if close_file:
                f.close()

if __name__ == '__main__':
        main()

