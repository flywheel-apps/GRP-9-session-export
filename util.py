import hashlib
import logging
import re
from functools import reduce

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
        log.warning(
            f"Expected {input_str} to be a string. Is type: {type(input_str)}. Attempting to coerce to str..."
        )
        input_str = str(input_str)
    if re.match(r"^[\d]+[\.]?[\d]*$", input_str):
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
        log.info(f"Renaming {filename} to {sanitized_filename}")

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


def get_dict_list_common_dict(dict_list):
    """
    Get a dictionary containing the common key-value pairs across all dictionaries.
        dict will be empty if no common key-value pairs are found

    Args:
        dict_list: list of dictionaries from which to get a common dict

    Returns:
        dict: a dict with key-value pairs that are identical in all dictionaries
    """

    def get_common_dict(dict1, dict2):
        """get a dictionary of common key-value pairs from two dicts"""
        return {k: v for k, v in dict1.items() if dict2.get(k) == v}

    # Reduce will raise if list is empty
    if not dict_list:
        return dict()
    # Refer to https://docs.python.org/3.0/library/functools.html for more info
    return reduce(get_common_dict, dict_list)


def false_if_exc_is_timeout(exception):
    """
    function to provide to backoff decorator as giveup parameter (backoff gives
        up when function evaluates True). Returns False if exception has a
        status attribute equal to 500, 502, 504
    Args:
        exception (Exception): an exception caught by backoff

    Returns:
        bool: whether to giveup/raise
    """
    if hasattr(exception, "status"):
        if exception.status in [504, 502, 500]:
            return False
    return True


def false_if_exc_is_timeout_or_sub_exists(exception):
    """
    function to provide to backoff decorator as giveup parameter (backoff gives
        up when function evaluates True). Returns False if exception has a
        status attribute equal to 500, 502, 504 and already exists exceptions
        with status
    Args:
        exception (Exception): an exception caught by backoff

    Returns:
        bool: whether to giveup/raise
    """
    is_timeout = not false_if_exc_is_timeout(exception)
    subject_exists = False
    if hasattr(exception, "status") and getattr(exception, "detail", None):

        if exception.status in [409, 422] and "already exists" in exception.detail:
            subject_exists = True

    if is_timeout or subject_exists:
        return False
    else:
        return True
