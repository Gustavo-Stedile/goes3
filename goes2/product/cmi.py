from typing import Tuple
from .product import Product


class CMI:
    class Channel(Product):
        def __init__(
            self,
            channel: str,
            palette_path: str,
            range: Tuple = (0, 1)
        ):
            super().__init__(name=channel, uses=f'ABI-L2-CMIPF/{channel}')
            self._palette_path = palette_path
            self._range = range

        def create(self, data):
            cmi = data['CMI']
            colored = self.apply_palette(
                cmi,
                self._palette_path,
                self._range
            )
            return colored

    channels = {
        'C01': Channel(
            'C01', 'res/palettes/square_root_visible_enhancement.cpt'
        ),
        'C02': Channel(
            'C02', 'res/palettes/square_root_visible_enhancement.cpt'
        ),
        'C03': Channel(
            'C03', 'res/palettes/square_root_visible_enhancement.cpt'
        ),
        'C04': Channel(
            'C04', 'res/palettes/square_root_visible_enhancement.cpt'
        ),
        'C05': Channel(
            'C05', 'res/palettes/square_root_visible_enhancement.cpt'
        ),
        'C06': Channel(
            'C06', 'res/palettes/square_root_visible_enhancement.cpt'
        ),
        'C07': Channel(
            'C07', 'res/palettes/SVGAIR2_TEMP.cpt', range=(161, 330)
        ),
        'C08': Channel(
            'C08', 'res/palettes/SVGAWVX_TEMP.cpt', range=(161, 330)
        ),
        'C09': Channel(
            'C09', 'res/palettes/SVGAWVX_TEMP.cpt', range=(161, 330)
        ),
        'C10': Channel(
            'C10', 'res/palettes/SVGAWVX_TEMP.cpt', range=(161, 330)
        ),
        'C11': Channel(
            'C11', 'res/palettes/IR4AVHRR6.cpt', range=(170.15, 357.15)
        ),
        'C12': Channel(
            'C12', 'res/palettes/IR4AVHRR6.cpt', range=(170.15, 357.15)
        ),
        'C13': Channel(
            'C13', 'res/palettes/ir_realce_dsa_kelvin.cpt',
            range=(193.15, 313.15)
        ),
        'C14': Channel(
            'C14', 'res/palettes/IR4AVHRR6.cpt', range=(170.15, 357.15)
        ),
        'C15': Channel(
            'C15', 'res/palettes/IR4AVHRR6.cpt', range=(170.15, 357.15)
        ),
        'C16': Channel(
            'C16', 'res/palettes/IR4AVHRR6.cpt', range=(170.15, 357.15)
        )
    }

    @staticmethod
    def in_range(start: int, finish: int):
        return [
            CMI.channels[f'C{i:02.0f}']
            for i in range(start, finish+1)
        ]

    @staticmethod
    def of(band: str):
        return CMI.channels[band]
