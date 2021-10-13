def replace_all(text: str, words_dict: dict) -> str:
    """
    Remove a set of words inside a dict form a text
    :param text: input str
    :param words_dict: words to be removed and its replacement
    :return: str without words specific in the dic
    """
    for i, j in words_dict.items():
        text = text.replace(i, j)
    return text


def remove_special_chars(text: str) -> str:
    """
    Remove special chars from a text
    :param text: input text
    :return: text without special chars
    """
    for ch in ['\\', '`', '*', '0', '{', '}', '[', ']', '(', ')', '>', '#', '+', '.', '!', '$', '\'', '|', '=']:
        if ch in text:
            text = text.replace(ch, '')
    return text


def sanitize_path(path: str) -> str:
    """
    standardize the paths of the data sets
    :param path: path to be standardized
    :return: standardized path
    """
    sanitized_path = path.replace(' ', '')  # remove extra spaces
    words_to_remove = {'tenant': '', 'upper': '', 'year={year}': '', 'month={month}': '', 'day={day}': '',
                       'hour={hour}': '', 'country=': '', 'hour=00': '', 'minute=00': '',
                       '{year}': '', '{month}': '', '{day}': '', '{hour}': '',
                       '{minute}': '',
                       'date': '', '.csv': '', '.parquet': '', 'avro': '', 'part': '', '.gz': '',
                       ' ': '', '__': '_', '/GB': '/', '/PAN/': '/', '/NL/': '/', '/ZIGGO/': '/'
                       }
    sanitized_path = replace_all(sanitized_path, words_to_remove)
    sanitized_path = remove_special_chars(sanitized_path)
    sanitized_path = sanitized_path.lstrip('/').lstrip('_')
    for i in range(1, 5):  ## TODO : loop recursively
        sanitized_path = sanitized_path.replace('//', '/')
    sanitized_path = sanitized_path.replace('__', '_')
    sanitized_path = sanitized_path.rstrip('/')

    return sanitized_path
