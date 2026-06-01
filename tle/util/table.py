import unicodedata

DISCORD_EMBED_DESCRIPTION_LIMIT = 4096

FULL_WIDTH = 1.66667
WIDTH_MAPPING = {'F': FULL_WIDTH, 'H': 1, 'W': FULL_WIDTH, 'Na': 1, 'N': 1, 'A': 1}

def width(s):
    return round(sum(WIDTH_MAPPING[unicodedata.east_asian_width(c)] for c in s))


class TableTooWideError(ValueError):
    pass


def truncate_to_width(value, max_width, marker='...'):
    value = str(value)
    if width(value) <= max_width:
        return value
    if max_width <= 0:
        return ''

    marker_width = width(marker)
    if marker_width > max_width:
        result = ''
        for c in marker:
            if width(result + c) > max_width:
                break
            result += c
        return result

    target_width = max_width - marker_width
    result = ''
    for c in value:
        if width(result + c) > target_width:
            break
        result += c
    return result + marker


class Content:
    def __init__(self, *args):
        self.data = args
    def sizes(self):
        return [width(str(x)) for x in self.data]
    def __len__(self):
        return len(self.data)

class Header(Content):
    def layout(self, style):
        return style.format_header(self.data)

class Data(Content):
    def layout(self, style):
        return style.format_body(self.data)

class Line:
    def __init__(self, c='-'):
        self.c = c
    def layout(self, style):
        self.data = ['']*style.ncols
        return style.format_line(self.c)

class Style:
    def __init__(self, body, header=None):
        self._body = body
        self._header = header or body
        self.ncols = body.count('}')

    def _pad(self, data, fmt):
        S = []
        lastc = None
        size = iter(self.sizes)
        datum = iter(data)
        for c in fmt:
            if lastc == ':':
                dstr = str(next(datum))
                sz = str(next(size) - (width(dstr) - len(dstr)))
                if c in '<>^':
                    S.append(c + sz)
                else:
                    S.append(sz + c)
            else:
                S.append(c)
            lastc = c
        return ''.join(S)

    def format_header(self, data):
        return self._pad(data, self._header).format(*data)

    def format_line(self, c):
        data = ['']*self.ncols
        return self._pad(data, self._header).replace(':', ':'+c).format(*data)

    def format_body(self, data):
        return self._pad(data, self._body).format(*data)

    def set_colwidths(self, sizes):
        self.sizes = sizes

class Table:
    def __init__(self, style):
        self.style = style
        self.rows = []

    def append(self, row):
        self.rows.append(row)
        return self
    __add__ = append

    def __repr__(self):
        sizes = [row.sizes() for row in self.rows if isinstance(row, Content)]
        max_colsize = [max(s[i] for s in sizes) for i in range(self.style.ncols)]
        self.style.set_colwidths(max_colsize)
        return '\n'.join(row.layout(self.style) for row in self.rows)
    __str__ = __repr__


def _coerce_row(row):
    return tuple(str(value) for value in row)


def _content_widths(contents, ncols):
    sizes = [content.sizes() for content in contents if isinstance(content, Content)]
    return [max(s[i] for s in sizes) for i in range(ncols)]


def _render_table_lines(style, header, rows, line_char='-'):
    header = _coerce_row(header)
    rows = [_coerce_row(row) for row in rows]
    contents = [Header(*header), *[Data(*row) for row in rows]]
    style.set_colwidths(_content_widths(contents, style.ncols))
    return (
        [Header(*header).layout(style), Line(line_char).layout(style)],
        [Data(*row).layout(style) for row in rows],
    )


def _fit_flexible_columns(style, header, rows, max_line_width, flexible_cols, line_char):
    header = _coerce_row(header)
    rows = [_coerce_row(row) for row in rows]
    flexible_cols = tuple(flexible_cols)
    if not flexible_cols:
        header_lines, row_lines = _render_table_lines(style, header, rows, line_char)
        if max(map(width, [*header_lines, *row_lines])) <= max_line_width:
            return rows
        raise TableTooWideError('Table row is too wide for an embed codeblock')

    col_limits = {}
    min_limits = {}
    for col in flexible_cols:
        values = [header[col], *[row[col] for row in rows]]
        col_limits[col] = max(width(value) for value in values)
        min_limits[col] = max(1, width(header[col]))

    for _ in range(1000):
        fitted_rows = []
        for row in rows:
            fitted = list(row)
            for col, limit in col_limits.items():
                fitted[col] = truncate_to_width(fitted[col], limit)
            fitted_rows.append(tuple(fitted))

        header_lines, row_lines = _render_table_lines(style, header, fitted_rows, line_char)
        current_width = max(map(width, [*header_lines, *row_lines]))
        if current_width <= max_line_width:
            return fitted_rows

        candidates = [col for col in flexible_cols if col_limits[col] > min_limits[col]]
        if not candidates:
            raise TableTooWideError('Table row is too wide for an embed codeblock')

        overflow = current_width - max_line_width
        col = max(candidates, key=lambda c: col_limits[c])
        new_limit = max(min_limits[col], col_limits[col] - overflow)
        if new_limit == col_limits[col]:
            new_limit -= 1
        col_limits[col] = new_limit

    raise TableTooWideError('Table row is too wide for an embed codeblock')


def _wrap_codeblock(lines, language):
    prefix = f'```{language}\n'
    suffix = '\n```'
    return prefix + '\n'.join(lines) + suffix


def _codeblock_budget(max_chars, language):
    return max_chars - len(f'```{language}\n') - len('\n```')


def _lines_len(lines):
    return len('\n'.join(lines))


def format_table_pages(style, header, rows, *, line_char='-', language='',
                       max_chars=DISCORD_EMBED_DESCRIPTION_LIMIT,
                       max_line_width=None, flexible_cols=()):
    """Return embed-description-sized codeblock pages for a table.

    Pages are split by Discord's embed description limit, not by a fixed row
    count. Header and separator rows are repeated on every page. Columns listed
    in flexible_cols may be truncated to keep a single rendered row embeddable.
    """
    content_budget = _codeblock_budget(max_chars, language)
    if content_budget <= 0:
        raise ValueError('max_chars is too small for a codeblock')

    original_rows = [_coerce_row(row) for row in rows]
    line_width = max_line_width or content_budget

    def render_with_limit(limit):
        fitted_rows = _fit_flexible_columns(
            style, header, original_rows, limit, flexible_cols, line_char)
        rendered_header, rendered_rows = _render_table_lines(
            style, header, fitted_rows, line_char)
        return fitted_rows, rendered_header, rendered_rows

    def rows_fit(rendered_header, rendered_rows):
        return all(_lines_len([*rendered_header, row]) <= content_budget
                   for row in rendered_rows)

    rows, header_lines, row_lines = render_with_limit(line_width)
    if row_lines and not rows_fit(header_lines, row_lines):
        if not flexible_cols:
            raise TableTooWideError('Table row is too wide for an embed codeblock')

        best = None
        low, high = 1, line_width - 1
        while low <= high:
            mid = (low + high) // 2
            try:
                candidate = render_with_limit(mid)
            except TableTooWideError:
                low = mid + 1
                continue

            _, candidate_header, candidate_rows = candidate
            if rows_fit(candidate_header, candidate_rows):
                best = candidate
                low = mid + 1
            else:
                high = mid - 1

        if best is None:
            raise TableTooWideError('Table row is too wide for an embed codeblock')
        rows, header_lines, row_lines = best

    base_len = _lines_len(header_lines)
    if base_len > content_budget:
        raise TableTooWideError('Table header is too wide for an embed codeblock')

    pages = []
    current = list(header_lines)
    for row_line in row_lines:
        candidate = [*current, row_line]
        if _lines_len(candidate) > content_budget:
            if current == header_lines:
                raise TableTooWideError('Table row is too wide for an embed codeblock')
            pages.append(_wrap_codeblock(current, language))
            current = [*header_lines, row_line]
            if _lines_len(current) > content_budget:
                raise TableTooWideError('Table row is too wide for an embed codeblock')
        else:
            current = candidate

    pages.append(_wrap_codeblock(current, language))
    return pages
