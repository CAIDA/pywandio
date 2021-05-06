import wandio

if __name__ == '__main__':

    with wandio.open('test.txt.gz') as fh:
        line_count = 0
        word_count = 0
        for line in fh:
            word_count += len(line.rstrip().split())
            line_count +=1
        print(line_count, word_count)

    with wandio.open('test.txt.gz', "rb") as fh:
        line_count = 0
        word_count = 0
        for line in fh:
            word_count += len(line.rstrip().split())
            line_count +=1
        print(line_count, word_count)
