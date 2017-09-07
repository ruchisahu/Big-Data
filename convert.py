import PythonMagick
image = PythonMagick.Image("user/root/sample_image.jpg")
print image.fileName()
print image.magick()
print image.size().width()
print image.size().height()