/* Pure helpers used by the page extractor. No credentials or page mutations. */
globalThis.TleLeaderboard = (() => {
  const timePattern = /^\d{1,2}:[0-5]\d(?::[0-5]\d)?$/;
  function serialize(rows) {
    return rows.map(entry => [entry.name, ...(entry.isYou && entry.name !== "You" ? ["You"] : []),
      entry.status, entry.time].filter(Boolean).join("\n")).join("\n\n");
  }
  function pageGame(url) {
    const parsed = new URL(url);
    if (parsed.protocol !== "https:" || !/(^|\.)linkedin\.com$/.test(parsed.hostname)) return null;
    return parsed.pathname.match(/^\/games\/(?:view\/)?([a-z][a-z0-9-]*)(?:\/|$)/i)?.[1].toLowerCase() || null;
  }
  function puzzleNumber(board, url, visible) {
    const labels = [...board.querySelectorAll('.pr-connections-leaderboard__header-puzzle-id')].filter(visible);
    const tabs = [...board.querySelectorAll('.pr-connections-leaderboard__tabs [role="tab"]')].filter(visible);
    let offset = 0;
    if (tabs.length) {
      const selected = tabs.filter(tab => tab.getAttribute('aria-selected') === 'true');
      if (selected.length !== 1 || !['Today', 'Yesterday'].includes(selected[0].textContent.trim())) {
        throw new Error('Could not identify the selected leaderboard day. Wait for it to finish loading.');
      }
      offset = selected[0].textContent.trim() === 'Yesterday' ? 1 : 0;
    }
    if (labels.length === 1 && !tabs.length) {
      const match = labels[0].textContent.trim().match(/^Puzzle No\.\s*(\d+(?:,\d{3})*)$/i);
      const value = match && Number(match[1].replaceAll(',', ''));
      if (Number.isSafeInteger(value) && value > 0) return value;
    }
    // The current renderer replaces the puzzle label with Today/Yesterday tabs.
    // Its route loads the URL's gameUrn; handleTabClick does not change that URL.
    // Yesterday fetches that original gameUrn with delta:1, so subtract exactly
    // once. Never derive an edition from the clock or a stale hidden header.
    const parsed = new URL(url);
    const urns = parsed.searchParams.getAll('gameUrn');
    const match = urns.length === 1 && urns[0].match(/^urn:li:fsd_game:\([^,()]+,\d+,(\d+)\)$/);
    const value = match && Number(match[1]) - offset;
    if (tabs.length && labels.length === 0 &&
        /^\/games\/(?:view\/)?[^/]+\/results\/leaderboard\/connections\/?$/.test(parsed.pathname) &&
        Number.isSafeInteger(value) && value > 0) return value;
    throw new Error('Could not read the leaderboard’s puzzle number.');
  }
  return {serialize, pageGame, timePattern, puzzleNumber};
})();
