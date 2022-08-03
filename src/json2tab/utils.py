import ijson
from json2tab.helpers import Stack, open_file


def get_top_keys(json_file):
    """
    Get the top-level keys of the first json line in json_file

    :param json_file:
    :return: list of top-level keys from first json line
    """
    result = []
    with open_file(json_file, mode="rb") as f:
        for (_, prefix, event, value) in parse(f, multiple_values=True):
            if prefix == '' and event == 'map_key' and value:
                result.append(value)
            elif prefix == '' and event == 'end_map' and value is None:
                f.seek(0)  # read from beginning again
                break

    return result


def parse(file, **kwargs):
    """
    Generator based on `ijson.parse` function
    Return more descriptive prefixes for arrays along with the base prefix, event and value

    eg.
    {
        "a" : ["one", "two", "three"]
    }
    The value "one" would have the prefix `a.0` instead of `a.item` as `ijson.parse` would give.
    Similarly, "two" would have the prefix `a.1` and "three" `a.2`.

    The base prefix for this would be `a`
    """

    basic_events = ijson.basic_parse(file, **kwargs)
    path = []

    # variables to compute prefix for arrays in json
    arr_indices = Stack()  # tracking indices of array elements in json, to be added to prefixes
    openings = Stack()
    current_pos = Stack()  # location of associated array index in the `path`

    for event, value in basic_events:
        if event == 'map_key':
            prefix = '.'.join(path[:-1])
            path[-1] = value
        elif event == 'start_map':
            prefix = '.'.join(path)
            path.append(None)

            if openings.peek() == "start_array":
                openings.push(event)
            elif openings.peek() == "map_parsed":
                if not arr_indices.is_empty():
                    arr_indices.setLast(arr_indices.peek() + 1)
                    path[current_pos.peek()] = str(arr_indices.peek())  # update the path
                openings.pop()

        elif event == 'end_map':
            path.pop()

            if openings.peek() == "start_map":
                openings.pop()
                openings.push("map_parsed")
            elif openings.peek() == "arr_parsed":
                openings.pop()
                openings.push("map_parsed")

            prefix = '.'.join(path)
        elif event == 'start_array':
            prefix = '.'.join(path)

            if openings.peek() == "arr_parsed":
                if not arr_indices.is_empty():
                    arr_indices.setLast(arr_indices.peek() + 1)
                    path[current_pos.peek()] = str(arr_indices.peek())  # update the path
                openings.pop()

            arr_indices.push(0)
            path.append(str(arr_indices.peek()))
            current_i = len(path) - 1
            current_pos.push(current_i)
            openings.push(event)

        elif event == 'end_array':
            path.pop()
            arr_indices.pop()
            current_pos.pop()
            if openings.peek() == 'start_array':
                openings.pop()
                if openings.peek() == 'start_array':
                    openings.push("arr_parsed")
            elif openings.peek() == 'arr_parsed':
                openings.pop()
                openings.pop()
                openings.push("arr_parsed")
            elif openings.peek() == 'map_parsed':
                openings.pop()
                openings.pop()

            prefix = '.'.join(path)

        else:  # any scalar value
            prefix = '.'.join(path)
            if openings.peek() == 'start_array':  # if array is of type [value1, value2, value3]
                arr_indices.setLast(arr_indices.peek() + 1)
                current_i = current_pos.peek()
                path[current_i] = str(arr_indices.peek())  # update the path

        base_prefix = path[0] if prefix else ""

        yield base_prefix, prefix, event, value

