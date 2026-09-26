/* Read-only completion extraction. Selectors/branches verified against LinkedIn's
 * public results-page/top, golden-chiclet and beginner-player-end-screen renderers.
 * This reads the owner's completed result, never a playing timer or an average.
 */
globalThis.TleOwnLinkedInResult = (() => {
  function visible(node) {
    if (!node?.getClientRects().length) return false;
    for (let parent = node; parent; parent = parent.parentElement) {
      const style = node.ownerDocument.defaultView.getComputedStyle(parent);
      if (parent.hidden || style.display === 'none' || style.visibility === 'hidden' || style.opacity === '0') return false;
    }
    return true;
  }
  function documents() {
    const docs = [document];
    for (const frame of document.querySelectorAll('iframe')) {
      try {
        if (visible(frame) && new URL(frame.src, location.href).origin === location.origin && frame.contentDocument) {
          docs.push(frame.contentDocument);
        }
      } catch { /* Ignore cross-origin frames. */ }
    }
    return docs;
  }
  function read() {
    const gamePath = globalThis.TleLeaderboard.pageGame(location.href);
    if (!gamePath || !/^\/games\/(?:view\/)?[^/]+\/results(?:\/|$)/.test(location.pathname)) return null;
    const sections = documents().flatMap(doc => [...doc.querySelectorAll('.pr-game-results__section')])
      .filter(visible);
    // Transitions can briefly render two views; wait for one unambiguous result.
    if (sections.length !== 1) return null;
    const tops = [...sections[0].querySelectorAll('.pr-top__header')].filter(visible);
    if (tops.length !== 1) return null;
    const top = tops[0];
    const edition = [...top.querySelectorAll(':scope > .pr-top__subtext')].filter(visible)
      .map(node => node.textContent.trim().replace(/\s+/g, ' '))
      .map(text => text.match(/^(.+?)\s+#(\d+(?:,\d{3})*)$/)).filter(Boolean);
    if (edition.length !== 1 || edition[0][1].toLowerCase() !== gamePath.replaceAll('-', ' ')) return null;
    const title = sections[0].querySelector('.pr-game-results__toolbar-title')?.textContent.trim().toLowerCase();
    if (title !== edition[0][1].toLowerCase()) return null;
    const headline = top.querySelector('.pr-top__headline');
    // The renderer emits these only for END_SOLVED. Failed/practice screens and
    // an in-progress timer never count as a personal completion.
    if (!visible(headline) || !['You’re crushing it!', 'See you tomorrow!', 'Next puzzle in…'].includes(headline.textContent.trim())) return null;
    const times = new Set();
    for (const node of top.querySelectorAll('.pr-golden-chiclet__text, .pr-beginner-player__better-than-chiclet-game-score, :scope > .pr-top__subtext')) {
      if (!visible(node)) continue;
      const text = node.textContent.trim();
      const value = text.replace(/^solved in\s+/i, '');
      if (globalThis.TleLeaderboard.timePattern.test(value)) times.add(value);
    }
    if (times.size !== 1) return null;
    const puzzleNumber = Number(edition[0][2].replaceAll(',', ''));
    const timeSeconds = [...times][0].split(':').map(Number).reduce((total, part) => total * 60 + part, 0);
    if (!Number.isSafeInteger(puzzleNumber) || puzzleNumber < 1 || timeSeconds > 86399) return null;
    return {gamePath, puzzleNumber, timeSeconds};
  }
  return {read, documents};
})();
