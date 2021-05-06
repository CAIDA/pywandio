import wandio

if __name__ == '__main__':

    with wandio.open('http://data.caida.org/datasets/as-relationships/README.txt') as fh:
        with wandio.open('test.txt.gz', mode='w') as ofh:
            line_count = 0
            word_count = 0
            for line in fh:
                word_count += len(line.rstrip().split())
                line_count +=1
                ofh.write(line)
            print(line_count, word_count)
