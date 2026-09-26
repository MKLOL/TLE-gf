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
  return {serialize, pageGame, timePattern};
})();
