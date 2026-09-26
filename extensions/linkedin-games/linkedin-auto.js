/* Only observes the existing results view. No clicks, navigation, clipboard,
 * LinkedIn requests, cookies, or page storage. Credentials stay in the worker.
 */
(() => {
  if (window !== window.top || globalThis.tleLinkedInAutomatic) return;
  globalThis.tleLinkedInAutomatic = true;
  const observed = new Map(), finished = new Set(), attempts = new Map();
  let timer, sending = false;
  function schedule() {
    clearTimeout(timer);
    timer = setTimeout(scan, 300);
  }
  async function scan() {
    if (sending) return;
    if (!globalThis.TleLeaderboard.pageGame(location.href)) return;
    const result = globalThis.TleOwnLinkedInResult.read();
    if (!result) return;
    const key = JSON.stringify(result);
    if (finished.has(key) || Date.now() - (attempts.get(key) || 0) < 30000) return;
    attempts.set(key, Date.now());
    sending = true;
    try {
      const response = await chrome.runtime.sendMessage({type: 'linkedin-result', ...result});
      if (response?.ok || response?.skipped) finished.add(key);
      // Keep a bounded history even across many SPA navigations.
      if (attempts.size > 100) attempts.delete(attempts.keys().next().value);
      if (finished.size > 100) finished.delete(finished.values().next().value);
    } catch { /* A reloaded extension invalidates old content contexts. */ }
    finally { sending = false; }
  }
  function refresh() {
    const docs = globalThis.TleLeaderboard.pageGame(location.href) ? globalThis.TleOwnLinkedInResult.documents() : [];
    for (const [doc, observer] of observed) {
      if (!docs.includes(doc)) { observer.disconnect(); observed.delete(doc); }
    }
    for (const doc of docs) {
      if (observed.has(doc)) continue;
      const observer = new MutationObserver(schedule);
      observer.observe(doc, {subtree: true, childList: true, characterData: true,
        attributes: true, attributeFilter: ['class', 'style', 'hidden']});
      observed.set(doc, observer);
    }
    scan();
  }
  // Also discovers newly loaded preload frames and history.pushState navigation
  // without replacing any of LinkedIn's functions.
  refresh();
  setInterval(refresh, 2000);
})();
