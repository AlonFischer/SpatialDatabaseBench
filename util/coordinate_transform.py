from pyproj import Transformer


def transform_4326_to_3857(point):
    transformer = Transformer.from_crs("epsg:4326", "epsg:3857")
    x, y = transformer.transform(point[0], point[1])
    return (x, y)
