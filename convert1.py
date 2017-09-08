import PythonMagick
import sys
from StringIO import StringIO


completeStdin = sys.stdin.read()
input = StringIO(completeStdin)



image = PythonMagick.Image(input)
print image.fileName()
print image.magick()
print image.size().width()
print image.size().height()
