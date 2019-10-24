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
    if re.match(r'^[\d]+$', input_str):
        output_str = f'"{input_str}"'
    else:
        output_str = input_str
    return output_str
