import PythonMagick
image = PythonMagick.Image("/ruchi/sample_image.jpg")
print image.fileName()
print image.magick()
print image.size().width()
print image.size().height()
