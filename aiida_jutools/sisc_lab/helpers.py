# In here you can implement all functions you need within the notebooks
# you can import them via `from .helpers.import <function_namw>` 


def print_bold(text: str):
    """Print text in bold.

    :param text: text to print in bold.
    """
    bold_text = f"\033[1m{text}\033[1m"
    print(bold_text)
