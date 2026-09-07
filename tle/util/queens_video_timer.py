"""Timer location and digit reading for LinkedIn Queens screen recordings.

Part of the ``;queens time`` pipeline (see ``queens_video_analyze``).  Given the
per-block change masks produced by ``queens_video_decode``, this module finds
the on-screen timer (the blocks that change at a ~1 s cadence), and reads the
``m:ss`` digits off a full-resolution strip of the timer row with templates of
LinkedIn's timer font (``queens_timer_digits.npz`` next to this file).
"""
import os

import numpy as np

SCENE_FRACTION = 0.25   # >25% of blocks changed => scene transition, not a flip
MIN_FLIPS = 3           # events in a ~1 s chain (a 2-second solve shows 0:00, 0:01, 0:02)

DIGIT_SHAPE = (24, 16)      # rows, cols of the normalised digit crops
TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                             'queens_timer_digits.npz')


def merge_runs(idx):
    """Merge consecutive frame indices into single events (take the first)."""
    if len(idx) == 0:
        return []
    events, prev = [idx[0]], idx[0]
    for i in idx[1:]:
        if i != prev + 1:
            events.append(i)
        prev = i
    return events


def longest_chain(t, tol=0.3, max_step=3):
    """Longest run of consecutive events spaced by ~integer seconds (1..max_step)."""
    if len(t) < 2:
        return 0, 0
    dt = np.diff(t)
    ok = (np.abs(dt - np.round(dt)) < tol) & (np.round(dt) >= 1) & (np.round(dt) <= max_step)
    best, cur = (0, 0), 0
    for i, g in enumerate(ok):
        cur = cur + 1 if g else 0
        if cur > best[1]:
            best = (i + 1 - cur, cur)
    return best                      # (start index, number of good intervals)


def find_timer_blocks(changed, ts, scene, min_chain=None):
    """Blocks with the longest chain of ~1 s spaced change events (the seconds digit).
    With min_chain set, return nothing unless the best chain has at least that many flips."""
    n1, hb, wb = changed.shape
    length = np.zeros((hb, wb), int)
    counts = changed.sum(axis=0)
    for y in range(hb):
        for x in range(wb):
            if counts[y, x] < MIN_FLIPS:
                continue
            idx = np.nonzero(changed[:, y, x] & ~scene)[0]
            ev = merge_runs(idx)
            if len(ev) < MIN_FLIPS:
                continue
            t = ts[np.array(ev) + 1]
            st, ln = longest_chain(t)
            if ln + 1 >= MIN_FLIPS and np.median(np.diff(t[st:st + ln + 1])) < 1.5:
                length[y, x] = ln
    best = length.max()
    if best == 0 or (min_chain is not None and best < min_chain):
        return np.zeros((hb, wb), bool)
    good = length >= max(MIN_FLIPS - 1, int(np.ceil(0.7 * best)))
    # keep only the connected cluster around the strongest block (the digit), drop stray board cells
    by, bx = np.unravel_index(np.argmax(length), length.shape)
    ys, xs = np.nonzero(good)
    near = (np.abs(ys - by) <= 3) & (np.abs(xs - bx) <= 3)
    out = np.zeros((hb, wb), bool)
    out[ys[near], xs[near]] = True
    return out


def normalise_polarity(strip):
    """Make the timer strip dark-text-on-light regardless of the phone's theme."""
    if np.median(strip[len(strip) // 2]) < 128:
        return 255 - strip
    return strip


def ink_mask(img):
    bg = float(np.median(img))
    return img < (bg + float(img.min())) / 2


def runs_of(idx):
    """[(start, end)] inclusive runs of consecutive integers in a sorted array."""
    idx = np.asarray(idx)
    if len(idx) == 0:
        return []
    cut = np.nonzero(np.diff(idx) != 1)[0]
    starts = np.concatenate([[idx[0]], idx[cut + 1]])
    ends = np.concatenate([idx[cut], [idx[-1]]])
    return list(zip(starts.tolist(), ends.tolist()))


def segment_timer(img, x_units, unit):
    """Split the timer text 'm:ss' in a strip row into digit crops (left->right).

    `unit` ~ one digit width in pixels (derived from the video width, so it scales with the
    phone's resolution). Returns list of (x0, x1, crop) for ink runs wide enough to be digits,
    ignoring the clock icon further left.
    """
    from PIL import Image
    ink = ink_mask(img)
    lo, hi = max(0, int(x_units - 3.5 * unit)), min(img.shape[1], int(x_units + 2.8 * unit))
    rows = np.nonzero(ink[:, lo:hi].any(axis=1))[0]
    if len(rows) == 0:
        return []
    r0, r1 = max(runs_of(rows), key=lambda p: p[1] - p[0])      # tallest text band
    band = ink[r0:r1 + 1]
    cols = np.nonzero(band[:, lo:hi].any(axis=0))[0] + lo
    out = []
    col_runs = runs_of(cols)
    widest = max((c1 - c0 + 1 for c0, c1 in col_runs), default=0)
    for c0, c1 in col_runs:
        if c1 - c0 + 1 < max(0.45 * widest, 0.25 * unit):
            continue                                     # colon / noise (a "1" is ~0.6 of a digit)
        sub = band[:, c0:c1 + 1]
        rr = np.nonzero(sub.any(axis=1))[0]
        crop = img[r0 + rr.min(): r0 + rr.max() + 1, c0:c1 + 1]
        im = Image.fromarray(crop).resize((DIGIT_SHAPE[1], DIGIT_SHAPE[0]), Image.BILINEAR)
        arr = np.asarray(im, dtype=np.float32)
        arr = (arr - arr.mean()) / (arr.std() + 1e-6)
        out.append((c0, c1, arr))
    return out


def load_templates(path=None):
    path = path or TEMPLATE_PATH
    if not path or not os.path.exists(path):
        return None
    z = np.load(path)
    return {int(k): z[k] for k in z.files}


def classify(arr, templates):
    best = max(templates.items(), key=lambda kv: float((arr * kv[1]).mean()))
    return best[0], float((arr * best[1]).mean())


def read_timer(img, x_units, unit, templates):
    """Read 'm:ss' from a timer strip row -> total seconds, or None."""
    segs = segment_timer(img, x_units, unit)
    if len(segs) < 3 or templates is None:
        return None
    digits = []
    for _, _, arr in segs[-3:] if len(segs) == 3 else segs:
        d, score = classify(arr, templates)
        if score < 0.5:
            return None
        digits.append(d)
    secs = digits[-2] * 10 + digits[-1]
    mins = int(''.join(str(d) for d in digits[:-2])) if len(digits) > 2 else 0
    return mins * 60 + secs
