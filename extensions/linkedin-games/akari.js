/* Runs only inside Daily Akari's same-origin game frame. No clicks or writes to
 * the game. The parent sends the exact loaded dailyDateKey, then the engine
 * sends animTimeBtn/timeResult/accuracy back on completion (including reloads).
 */
(() => {
  if (globalThis.tleAkariListening || window === window.parent || !['/akari', '/akari.html'].includes(location.pathname)) return;
  globalThis.tleAkariListening = true;
  if (window.parent.location.origin !== location.origin) return;
  let level, lastResult;
  addEventListener('message', event => {
    if (event.origin !== location.origin || event.source !== window.parent) return;
    const data = event.data;
    if (!data || typeof data !== 'object' || !('dailyDateKey' in data)) return;
    level = data.isArchive === false && /^\d{4}-\d{2}-\d{2}$/.test(data.dailyDateKey) ? data.dailyDateKey : null;
    lastResult = null;
  });
  window.parent.addEventListener('message', async event => {
    if (event.origin !== location.origin || event.source !== window || !level) return;
    const data = event.data;
    if (!data || !data.animTimeBtn) return;
    const gameSettings = JSON.parse(localStorage.getItem('settings') || '{}');
    if (gameSettings.showAccuracy !== true) {
      chrome.runtime.sendMessage({type: 'akari-status', error: 'Enable Pro Mode in Daily Akari to submit accuracy.'}).catch(() => {});
      return;
    }
    const parts = typeof data.timeResult === 'string' && data.timeResult.match(/^(\d{1,4}):([0-5]\d)(?::([0-5]\d))?$/);
    if (!parts || !Number.isFinite(data.accuracy) || data.accuracy < 0 || data.accuracy > 1) return;
    const seconds = parts[3] === undefined ? Number(parts[1]) * 60 + Number(parts[2]) :
      Number(parts[1]) * 3600 + Number(parts[2]) * 60 + Number(parts[3]);
    const body = {game: 'akari', puzzle_date: level,
      // This is the same published calendar anchor used by the Discord parser.
      puzzle_number: 446 + Math.round((Date.parse(level + 'T00:00:00Z') - Date.parse('2026-03-27T00:00:00Z')) / 86400000),
      time_seconds: seconds, accuracy: Math.round(data.accuracy * 100), is_perfect: data.accuracy === 1};
    const key = JSON.stringify(body);
    if (key === lastResult) return;
    lastResult = key;
    try {
      const response = await chrome.runtime.sendMessage({type: 'akari-result', body});
      if (!response?.ok) lastResult = null;
    } catch { lastResult = null; }
  });
})();
