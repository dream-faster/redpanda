"""Brace/paren balance ignoring comments and double-quoted strings.

Single quotes are NOT treated as char-literal delimiters: apostrophes in
comments and prose strings ("don't", "'deletion'") otherwise swallow arbitrary
text and corrupt the count. Real char literals in this codebase never contain
braces or parens, so ignoring them is safe for balance purposes.
"""


def mask(src):
    out = list(src)
    i, n = 0, len(src)
    while i < n:
        c = src[i]
        if c == '"':
            j = i + 1
            while j < n:
                if src[j] == "\\":
                    out[j] = " "
                    if j + 1 < n:
                        out[j + 1] = " "
                    j += 2
                    continue
                if src[j] == '"':
                    break
                out[j] = " "
                j += 1
            i = j + 1
            continue
        if c == "/" and i + 1 < n and src[i + 1] == "/":
            j = src.find("\n", i)
            j = n if j == -1 else j
            for k in range(i, j):
                out[k] = " "
            i = j
            continue
        if c == "/" and i + 1 < n and src[i + 1] == "*":
            j = src.find("*/", i)
            j = n if j == -1 else j + 2
            for k in range(i, j):
                if src[k] != "\n":
                    out[k] = " "
            i = j
            continue
        i += 1
    return "".join(out)


def balance(src):
    m = mask(src)
    return (m.count("(") - m.count(")"), m.count("{") - m.count("}"))
