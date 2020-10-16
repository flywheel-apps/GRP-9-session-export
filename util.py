import hashlib
import logging
import re

from pathvalidate import sanitize_filename


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


def get_sanitized_filename(filename):
    """A function for removing characters that are not alphanumeric, '.', '-', or '_' from an input string.
        asterix following "t2" + optional space/underscore  will be replaced with "star"
    Args:
        filename (str): an input string
    Returns:
        str: A string without characters that are not alphanumeric, '.', '-', or '_'
    """
    
    try:
        log
    except NameError:
        log = logging.getLogger(__name__)
    filename = re.sub(r"(t2 ?_?)\*", r"\1star", str(filename), flags=re.IGNORECASE)
    sanitized_filename = sanitize_filename(filename)
    if filename != sanitized_filename:
        log.info(f'Renaming {filename} to {sanitized_filename}')

    return sanitized_filename


def hash_value(value, algorithm="sha256", output_format="hex", salt=None):
    """Hash a string using the given algorithm and salt, and return in the requested output_format.

    Arguments:
        value (str): The value to hash
        algorithm (str): The algorithm to use (default is sha256)
        output_format (str): The output format, one of 'hex', 'dec', or None
        salt (str): The optional salt string
    """
    hasher = hashlib.new(algorithm)
    # Work in bytes
    if salt:
        hasher.update(salt.encode("utf-8"))
    hasher.update(value.encode("utf-8"))
    if output_format == "hex":
        result = hasher.hexdigest()
    elif output_format == "dec":
        digest = hasher.digest()
        result = ""
        for atom in digest:
            result += str(atom)
    else:
        result = hasher.digest
    return result