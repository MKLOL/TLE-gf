import {request, settings} from './api.js';

const $ = selector => document.querySelector(selector);
let expectedId;
try { expectedId = decodeURIComponent(location.hash.slice(1)); } catch { expectedId = ''; }
let record, catalog, phase = 'loading', filter = 'all', expiryTimer, postAttempted = false;
let activityTimer, activityStarted, expiryTick;

function status(message, kind = '', ongoing = false) {
  $('#status').textContent = message;
  $('#status').className = `notice ${kind}`.trim();
  $('#activity').hidden = !ongoing;
  if (ongoing && !activityTimer) {
    activityStarted = Date.now(); $('#elapsed').textContent = '0s';
    activityTimer = setInterval(() => { $('#elapsed').textContent = `${Math.floor((Date.now() - activityStarted) / 1000)}s`; }, 1000);
  } else if (!ongoing) { clearInterval(activityTimer); activityTimer = null; }
}
function sameRecord(a, b) {
  return !!a && !!b && a.data?.preview_id === b.data?.preview_id &&
    a.server === b.server && a.guild === b.guild && a.user === b.user;
}
async function removeOwnPreview() {
  const {pendingImport} = await chrome.storage.session.get('pendingImport');
  if (sameRecord(pendingImport, record)) await chrome.storage.session.remove('pendingImport');
}
function empty(title, description) {
  clearTimeout(expiryTimer);
  clearInterval(expiryTick);
  $('#review-content').hidden = true;
  $('#review-footer').hidden = true;
  $('#empty-title').textContent = title;
  $('#empty-description').textContent = description;
  $('#empty-state').hidden = false;
}
async function invalidate(message, remove = false) {
  if (['done', 'cancelled', 'stale'].includes(phase)) return;
  phase = 'stale';
  status(message, 'error');
  empty('This review is no longer current.', 'Open TLE Games and read the leaderboard again to review fresh matches.');
  if (remove) await removeOwnPreview();
}
function busy(value) {
  $('#confirm').disabled = value;
  $('#cancel').disabled = value;
  $('#confirm').textContent = value ? 'Importing…' : `Yes, import all ${record.data.rows.length} results`;
}
function cell(text, className = '') {
  const element = document.createElement('td');
  element.textContent = text;
  element.className = className;
  return element;
}
function detail(parent, text) {
  const element = document.createElement('span');
  element.className = 'row-detail';
  element.textContent = text;
  parent.append(element);
}
function formatTime(seconds) {
  const hours = Math.floor(seconds / 3600), minutes = Math.floor(seconds / 60) % 60;
  const tail = String(seconds % 60).padStart(2, '0');
  return hours ? `${hours}:${String(minutes).padStart(2, '0')}:${tail}` : `${minutes}:${tail}`;
}
function renderRows() {
  const query = $('#search').value.trim().toLocaleLowerCase();
  const rows = record.data.rows.filter(entry => {
    const matchesFilter = filter === 'all' || (filter === 'matched' ? entry.registered : !entry.registered);
    const names = `${entry.name} ${entry.discord_name || ''}`.toLocaleLowerCase();
    return matchesFilter && (!query || names.includes(query));
  });
  $('#players').replaceChildren();
  for (const entry of rows) {
    const tr = document.createElement('tr');
    const player = cell(entry.name, 'player-name');
    if (entry.is_own) detail(player, 'Your LinkedIn result');
    let account;
    if (entry.registered) {
      account = cell(entry.discord_name || 'Matched Discord account', 'account-name');
      detail(account, 'Matched through server registration');
    } else if (entry.is_own) {
      account = cell('Your result · registration pending', 'account-name pending-owner');
      detail(account, 'Will be saved for you until you register');
    } else {
      account = cell('Unassigned', 'account-name unassigned');
      detail(account, 'Will be saved for later registration');
    }
    const badges = cell('');
    const list = document.createElement('div');
    list.className = 'badge-list';
    for (const label of [entry.no_hints && 'No hints', entry.no_mistakes && 'No mistakes'].filter(Boolean)) {
      const badge = document.createElement('span');
      badge.className = 'badge'; badge.textContent = label; list.append(badge);
    }
    if (!list.children.length) detail(badges, 'No clean-solve badges');
    else badges.append(list);
    const rating = cell(entry.rated ? (entry.registered ? 'Rated' : 'Eligible when linked') : 'Unrated', 'rating-label');
    if (!entry.rated) rating.classList.add('unrated');
    tr.append(player, account, cell(formatTime(entry.time_seconds), 'solve-time'), badges, rating);
    $('#players').append(tr);
  }
  $('#no-matches').hidden = rows.length !== 0;
  $('#visible-count').textContent = `Showing ${rows.length} of ${record.data.rows.length} results`;
}
function render() {
  const data = record.data;
  $('#title').textContent = `Review ${data.game_name} results`;
  $('#connection').textContent = `${catalog.guild_name} · ${catalog.user_name}`;
  $('#puzzle-meta').textContent = `Puzzle #${data.puzzle_number} · ${data.puzzle_date} · LinkedIn’s Pacific calendar`;
  $('#summary').replaceChildren();
  for (const [count, label] of [[data.rows.length, 'Completed scores'], [data.registered, 'Matched to Discord'],
                              [data.unresolved, 'Unassigned scores']]) {
    const item = document.createElement('div'); item.className = 'summary-item';
    const value = document.createElement('strong'); value.textContent = count;
    const text = document.createElement('span'); text.textContent = label;
    item.append(value, text); $('#summary').append(item);
  }
  $('#excluded-summary').textContent = [data.skipped && `${data.skipped} banned or duplicate players excluded`,
    data.unplayed && `${data.unplayed} players without a completed score`].filter(Boolean).join(' · ');
  $('#excluded-summary').hidden = !$('#excluded-summary').textContent;
  $('#confirm-scope').textContent = `Confirmation saves all ${data.rows.length} results, including those hidden by search or filters.`;
  $('#empty-state').hidden = true;
  $('#review-content').hidden = false;
  $('#review-footer').hidden = false;
  renderRows(); busy(false);
}
function expired() { return Date.now() >= record.data.expires_at * 1000; }
function scheduleExpiry() {
  clearTimeout(expiryTimer);
  clearInterval(expiryTick);
  const update = () => {
    const seconds = Math.max(0, Math.ceil(record.data.expires_at - Date.now() / 1000));
    $('#expiry').textContent = `Preview expires in ${Math.floor(seconds / 60)}:${String(seconds % 60).padStart(2, '0')}`;
  };
  update(); expiryTick = setInterval(update, 1000);
  expiryTimer = setTimeout(() => {
    if (phase === 'ready') invalidate('This preview expired. Read the leaderboard again for a fresh review.', true).catch(() => {});
  }, Math.max(0, record.data.expires_at * 1000 - Date.now()));
}
function connectionMatches(connection, current) {
  return connection.server === record.server && current.guild_id === record.guild && current.user_id === record.user;
}

$('#search').addEventListener('input', () => { if (record) renderRows(); });
for (const button of document.querySelectorAll('[data-filter]')) {
  button.addEventListener('click', () => {
    filter = button.dataset.filter;
    for (const tab of document.querySelectorAll('[data-filter]')) tab.setAttribute('aria-pressed', String(tab === button));
    if (record) renderRows();
  });
}
$('#close-window').addEventListener('click', () => window.close());
$('#retry-load').addEventListener('click', () => location.reload());
window.addEventListener('hashchange', () => location.reload());
$('#cancel').addEventListener('click', async () => {
  if (phase !== 'ready') return;
  phase = 'cancelled'; busy(true);
  try { await removeOwnPreview(); }
  catch { /* Closing this review still prevents any further action in it. */ }
  status(postAttempted ? 'Review closed. Any results already imported remain saved.' : 'Cancelled. No results were imported.');
  empty(postAttempted ? 'Review closed.' : 'Import cancelled.',
    postAttempted ? 'A previous save attempt may have completed. Read the leaderboard again to check; importing identical results is safe.' :
      'Your leaderboard was not saved. You can read it again from TLE Games whenever you are ready.');
});
$('#confirm').addEventListener('click', async () => {
  if (phase !== 'ready') return;
  phase = 'checking'; busy(true); status('Checking your connection and preview…', '', true);
  try {
    if (expired()) { await invalidate('This preview expired. Read the leaderboard again for a fresh review.', true); return; }
    const connection = await settings();
    const current = await request(connection, '/v1/games');
    if (phase !== 'checking') return;
    if (!connectionMatches(connection, current)) {
      await invalidate('Your connection changed. Read the leaderboard again to review the new matches.', true); return;
    }
    // The popup may have produced another preview while the catalog was loading.
    const {pendingImport} = await chrome.storage.session.get('pendingImport');
    if (!sameRecord(pendingImport, record)) {
      await invalidate('A newer review replaced this preview. Open the current review from TLE Games.'); return;
    }
    if (expired()) { await invalidate('This preview expired. Read the leaderboard again for a fresh review.', true); return; }
    if (phase !== 'checking') return;
    phase = 'posting'; postAttempted = true; status(`Saving all ${record.data.rows.length} preview results…`, '', true);
    const result = await request(connection, `/v1/games/imports/${encodeURIComponent(expectedId)}/confirm`, {});
    phase = 'done';
    await removeOwnPreview();
    status('Import complete.', 'success');
    empty('Your results have been imported.', `${result.registered} matched and ${result.unresolved} unassigned results saved. ${result.unchanged} identical results were already saved.`);
  } catch (error) {
    if (phase === 'stale') return;
    if ([401, 403, 404, 409].includes(error.status)) {
      await invalidate(error.message, true);
    } else {
      phase = 'ready'; status(error.message + (postAttempted ? ' You can retry this import safely.' : ''), 'error'); busy(false); scheduleExpiry();
    }
  }
});

chrome.storage.onChanged.addListener((changes, area) => {
  if (!record || !['loading', 'ready', 'checking'].includes(phase)) return;
  if (area === 'session' && changes.pendingImport && !sameRecord(changes.pendingImport.newValue, record)) {
    invalidate('This preview was replaced or closed. Open the current review from TLE Games.').catch(() => {});
  }
  if (area === 'local' && ['server', 'token'].some(key => changes[key] && changes[key].newValue !== changes[key].oldValue)) {
    invalidate('Your connection changed. Read the leaderboard again.', true).catch(() => {});
  }
});

try {
  status('Loading your preview…', '', true);
  const {pendingImport} = await chrome.storage.session.get('pendingImport');
  if (!expectedId || pendingImport?.data?.preview_id !== expectedId) {
    phase = 'stale'; status('There is no current preview for this window.');
    empty('Read a leaderboard to begin.', 'Open TLE Games on a LinkedIn leaderboard and choose Read leaderboard.');
  } else {
    record = pendingImport;
    if (!Array.isArray(record.data.rows) || !Number.isFinite(record.data.expires_at)) throw new Error('This saved preview is invalid. Read the leaderboard again.');
    if (expired()) {
      await invalidate('This preview expired. Read the leaderboard again for a fresh review.', true);
    } else {
      const connection = await settings();
      catalog = await request(connection, '/v1/games');
      const {pendingImport: currentPreview} = await chrome.storage.session.get('pendingImport');
      if (phase !== 'loading') { /* A storage event already invalidated it. */ }
      else if (!sameRecord(currentPreview, record)) await invalidate('A newer review replaced this preview.');
      else if (!connectionMatches(connection, catalog)) await invalidate('Your connection changed. Read the leaderboard again.', true);
      else if (expired()) await invalidate('This preview expired. Read the leaderboard again for a fresh review.', true);
      else {
        phase = 'ready'; render(); scheduleExpiry();
        status('Please review the matches below. Nothing is saved until you confirm.');
      }
    }
  }
} catch (error) {
  phase = 'stale'; status(error.message, 'error');
  empty('The review could not be loaded.', 'Check your connection, then reopen this review or read the leaderboard again.');
  $('#retry-load').hidden = [401, 403, 404, 409].includes(error.status);
  if ([401, 403, 404, 409].includes(error.status)) await removeOwnPreview();
}
