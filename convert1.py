import PythonMagick
import sys

stdin = sys.stdin.read()
array = numpy.frombuffer(stdin, dtype='uint8')
image = PythonMagick.Image(array,1)
print image.fileName()
print image.magick()
print image.size().width()
print image.size().height()
