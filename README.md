# pywandio
Python bindings for libwandio (https://github.com/wanduow/wandio)

## Usage

```python
# this script should work with both python2 and python3
import wandio

files = [
         'http://data.caida.org/datasets/as-relationships/README.txt',
         'http://loki.caida.org:2243/data/external/as-rank-ribs/19980101/19980101.as-rel.txt.bz2'
         ]
for filename in files:
    # the with statement automatically closes the file at the end
    # of the block
    try:
        with wandio.open(filename) as fh:
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
```
