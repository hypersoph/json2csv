import ijson
from stack import Stack

def parse(f):
    '''
    Based on ijson.parse function
    Return more descriptive prefixes for arrays

    eg.
    {
        "a" : ["one", "two", "three"]
    }
    the value "one" would have the prefix `a.0` instead of `a.item` as ijson.parse would give.
    Similarly, "two" would have the prefix `a.1` and "three" `a.2`.
    '''

    basic_events = ijson.basic_parse(f, multiple_values=True)
    path = []

    # variables to compute prefix for arrays in json
    arr_indices = Stack()  # tracking indices of array elements in json
    current_i = None  # index of current json array element index in the path
    # purpose of current_i is mainly to differentiate between regular arrays [1,2,3] and arrays with json nested in them

    for event, value in basic_events:
        if event == 'map_key':
            prefix = '.'.join(path[:-1])
            path[-1] = value
        elif event == 'start_map':
            prefix = '.'.join(path)
            path.append(None)

        elif event == 'end_map':
            path.pop()

            if not arr_indices.is_empty():
                arr_indices.setLast(arr_indices.peek() + 1)
                path[-1] = str(arr_indices.peek())

            prefix = '.'.join(path)
        elif event == 'start_array':
            prefix = '.'.join(path)
            arr_indices.push(0)
            path.append(str(arr_indices.peek()))
            current_i = len(path) - 1

        elif event == 'end_array':
            path.pop()
            arr_indices.pop()
            current_i = None
            prefix = '.'.join(path)

        else:  # any scalar value
            prefix = '.'.join(path)
            if not arr_indices.is_empty() and current_i == len(path) - 1:  # if array is of type [value1, value2, value3]
                arr_indices.setLast(arr_indices.peek() + 1)
                path[current_i] = str(arr_indices.peek())  # update the path

        yield prefix, event, value
