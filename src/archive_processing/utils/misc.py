#
# Take a single list and breaks it
# into a list of lists of size batch_size
#
def divide_list_into_sublists(input_list: [], batch_size: int):
    # looping till length batch_size
    for i in range(0, len(input_list), batch_size):
        yield input_list[i : i + batch_size]  # noqa (to bypass bug in flake8)
