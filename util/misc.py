import decimal


def convert_decimals_to_ints_in_tuples(data):
    """data is an array of tuples"""
    modified_data = []
    for t in data:
        new_t = t
        for idx in range(len(new_t)):
            if isinstance(t[idx], decimal.Decimal):
                new_t = new_t[0:idx] + (int(new_t[idx]),) + new_t[idx+1:]
        modified_data.append(new_t)
    return modified_data


def convert_none_to_null_in_tuples(data):
    """data is an array of tuples"""
    modified_data = []
    for t in data:
        new_t = t
        for idx in range(len(new_t)):
            if t[idx] == None:
                new_t = new_t[0:idx] + ("NULL",) + new_t[idx+1:]
        modified_data.append(new_t)
    return modified_data


def tuple_to_str(tup):
    output = "("
    for val in tup:
        if val == None:
            output += "NULL, "
        elif isinstance(val, str):
            escaped_val = val.replace("'", "\\'")
            output += f"'{str(escaped_val)}', "
        else:
            output += f"{str(val)}, "
    output = output[:-2] + ")"
    return output
