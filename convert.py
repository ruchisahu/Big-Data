import PythonMagick
import sys
image = PythonMagick.Image(sys.stdin)
print image.fileName()
print image.magick()
print image.size().width()
print image.size().height()
