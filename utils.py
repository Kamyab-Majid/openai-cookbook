import tokenize
from io import StringIO


def remove_comments_and_docstrings(source):
    """
    Returns 'source' minus comments and docstrings.
    """
    io_obj = StringIO(source)
    out = ""
    prev_toktype = tokenize.INDENT
    last_lineno = -1
    last_col = 0
    for tok in tokenize.generate_tokens(io_obj.readline):
        token_type = tok[0]
        token_string = tok[1]
        start_line, start_col = tok[2]
        end_line, end_col = tok[3]
        # The following two conditionals preserve indentation.
        # This is necessary because we're not using tokenize.untokenize()
        # (because it spits out code with copious amounts of oddly-placed
        # whitespace).
        if start_line > last_lineno:
            last_col = 0
        if start_col > last_col:
            out += " " * (start_col - last_col)
        # Remove comments:
        if (
            token_type != tokenize.COMMENT
            and token_type == tokenize.STRING
            and prev_toktype != tokenize.INDENT
            and prev_toktype != tokenize.NEWLINE
            and start_col > 0
            or token_type not in [tokenize.COMMENT, tokenize.STRING]
        ):
            # Unlabelled indentation means we're inside an operator
            out += token_string
        prev_toktype = token_type
        last_col = end_col
        last_lineno = end_line
    return out
