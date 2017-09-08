import StringIO
import sys

filelike = StringIO.StringIO(sys.stdin.read())

# Now use `filelike` as a regular open file, e.g.:
filelike.seek(2)
print filelike.read()
