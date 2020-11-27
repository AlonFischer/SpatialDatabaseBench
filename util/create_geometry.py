def create_polygon(points):
    wkt = "'POLYGON(("
    points_text = [f"{point[0]} {point[1]}" for point in points]
    wkt += ", ".join(points_text)
    wkt += "))'"
    return wkt

def create_point(point):
    return f"'POINT({point[0]} {point[1]})'"

def create_linestring(points):
    wkt = "'LINESTRING("
    points_text = [f"{point[0]} {point[1]}" for point in points]
    wkt += ", ".join(points_text)
    wkt += ")'"
    return wkt
