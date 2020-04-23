import logging
import re


def quote_numeric_string(input_str):
    """
    Wraps a numeric string in double quotes. Attempts to coerce non-str to str and logs a warning.
    :param input_str: string to be modified (if numeric string - matches ^[\d]+$)
    :type input_str: str
    :return: output_str, a numeric string wrapped in quotes if input_str is numeric, or str(input_str)
    :rtype str
    """
    try:
        log
    except NameError:
        log = logging.getLogger(__name__)
    if not isinstance(input_str, str):
        log.warning(f'Expected {input_str} to be a string. Is type: {type(input_str)}. Attempting to coerce to str...')
        input_str = str(input_str)
    if re.match(r'^[\d]+[\.]?[\d]*$', input_str):
        output_str = f'"{input_str}"'
    else:
        output_str = input_str
    return output_str


def ensure_filename_safety(filename):
    """A function for removing characters that are not alphanumeric, '.', '-', or '_' from an input string.
    Args:
        filename (str): an input string
    Returns:
        str: A string without characters that are not alphanumeric, '.', '-', or '_'
    """
    
    try:
        log
    except NameError:
        log = logging.getLogger(__name__)
        
    safe_filename = re.sub(r'[^A-Za-z0-9\-\_\.]+', '', filename)
    if filename != safe_filename:
        log.info(f'Renaming {filename} to {safe_filename}')

    return safe_filename