from .rasterizer import Rasterizer
import matplotlib.pyplot as plt


class Plot(Rasterizer):
    def to_raster(self, data_array, path):
        plt.imshow(data_array.values, cmap='gray')
        plt.show()
