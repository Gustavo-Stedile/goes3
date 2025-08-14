class GOES19:
    """
    VALORES EM RADIANOS
    """

    height: float = 35786023.0
    name: str = 'GOES19'
    crs: str = """
        +proj=geos +h=35786023.0 +a=6378137.0 +b=6356752.31414
        +f=0.00335281068119356027 +lat_0=0.0 +lon_0=-75 +sweep=x +no_defs
    """
