import wandio
from dotenv import load_dotenv, find_dotenv
import os

if __name__ == '__main__':

    files = [
        # 'swift://datasets-external-netacq-codes/country_codes.csv',
        # 'https://www.caida.org/~mingwei/',
        # 'http://data.caida.org/datasets/as-relationships/README.txt',
        # 'http://loki.caida.org:2243/data/external/as-rank-ribs/19980101/19980101.as-rel.txt.bz2',
        # '/home/mingwei/moas.1601006100.events.gz',
        'swift://bgp-hijacks-edges/year=2020/month=09/day=30/hour=11/edges.1601466300.events.gz',
    ]

    load_dotenv(find_dotenv(".limbo-cred"), override=True)
    options = {
        "auth_version": '3',
        "os_username": os.environ.get('OS_USERNAME', None),
        "os_password": os.environ.get('OS_PASSWORD', None),
        "os_project_name": os.environ.get('OS_PROJECT_NAME', None),
        "os_auth_url": os.environ.get('OS_AUTH_URL', None),
    }

    for filename in files:
        # the with statement automatically closes the file at the end
        # of the block
        try:
            with wandio.open(filename,options=options) as fh:
                line_count = 0
                word_count = 0
                for line in fh:
                    word_count += len(line.rstrip().split())
                    line_count +=1
            # print the number of lines and words in file
            print(filename)
            print(line_count, word_count)
        except IOError as err:
            print(filename)
            raise err
