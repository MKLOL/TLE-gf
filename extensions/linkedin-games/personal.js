import {request, settings} from './api.js';
import {enabled, AUTOMATIC_KEYS} from './auto-config.js';
const $ = selector => document.querySelector(selector);
let pending, busy = false;
function show(value) {
  pending = value;
  $('#own-preview').hidden = !value;
  if (value) $('#own-summary').textContent = `${value.name} #${value.body.puzzle_number} · ${value.body.puzzle_date} · ${value.time}. Post as your Discord account (clean result).`;
}
async function clear() {
  show(null);
  await chrome.storage.session.remove('pendingOwn');
}
function status(text) { $('#status').textContent = text; }
function buttons(value) {
  busy = value;
  for (const id of ['#own', '#post-own', '#cancel-own', '#read', '#open-review', '#cancel']) $(id).disabled = value;
  $('#status').classList.toggle('loading', value);
  $('#read-tools').setAttribute('aria-busy', String(value));
}
$('#cancel-own').addEventListener('click', clear);
$('#own').addEventListener('click', async () => {
  if (busy) return;
  buttons(true);
  status('Connecting to your server to read your score…');
  try {
    await clear();
    const config = await settings();
    const catalog = await request(config, '/v1/games');
    status('Reading your visible LinkedIn score…');
    const [tab] = await chrome.tabs.query({active: true, currentWindow: true});
    if (!tab?.id) throw new Error('Open your LinkedIn result tab, then read it again.');
    await chrome.scripting.executeScript({target: {tabId: tab.id}, files: ['leaderboard.js', 'linkedin-result.js']});
    const completed = await chrome.scripting.executeScript({target: {tabId: tab.id},
      func: () => globalThis.TleOwnLinkedInResult.read()});
    let data = completed[0]?.result;
    if (!data) {
      await chrome.scripting.executeScript({target: {tabId: tab.id}, files: ['extract.js']});
      const leaderboard = await chrome.scripting.executeScript({target: {tabId: tab.id},
        func: () => globalThis.TleReadLeaderboard(true)});
      data = leaderboard[0]?.result;
      if (!data || data.error) throw new Error(data?.error || 'Could not read your score.');
      data.timeSeconds = data.rows[0].time.split(':').map(Number).reduce((total, part) => total * 60 + part, 0);
    }
    const game = catalog.games.find(item => item.path === data.gamePath);
    if (!game?.enabled) throw new Error('This game is not enabled in your server.');
    const date = new Date(game.anchor_date + 'T00:00:00Z');
    date.setUTCDate(date.getUTCDate() + data.puzzleNumber - game.anchor_number);
    const time = `${Math.floor(data.timeSeconds / 60)}:${String(data.timeSeconds % 60).padStart(2, '0')}`;
    const value = {server: config.server, guild: catalog.guild_id, user: catalog.user_id, name: game.name, time,
      body: {game: game.id, puzzle_date: date.toISOString().slice(0, 10), puzzle_number: data.puzzleNumber,
        time_seconds: data.timeSeconds, accuracy: 100, is_perfect: true}};
    await chrome.storage.session.set({pendingOwn: value});
    show(value);
    status('Check your score, then press Post my score.');
  } catch (error) { status(error.message); }
  finally { buttons(false); }
});
$('#post-own').addEventListener('click', async () => {
  if (busy || !pending) return;
  buttons(true);
  status('Checking your account before posting your score…');
  try {
    const config = await settings();
    const catalog = await request(config, '/v1/games');
    if (config.server !== pending.server || catalog.user_id !== pending.user || catalog.guild_id !== pending.guild) {
      await clear();
      throw new Error('Your connection changed. Read your score again.');
    }
    status('Posting your score to Discord…');
    const result = await request(config, '/v1/games/results', pending.body);
    await clear();
    status(result.duplicate ? 'Your score was already registered and posted.' : 'Your score is registered and posted in Discord.');
  } catch (error) { status(error.message); }
  finally { buttons(false); }
});
const {pendingOwn} = await chrome.storage.session.get('pendingOwn');
if (pendingOwn) show(pendingOwn);
const automatic = await chrome.storage.local.get([...AUTOMATIC_KEYS, 'akariStatus', 'linkedinStatus']);
$('#akari-status').textContent = enabled(automatic, 'autoAkari') ? (automatic.akariStatus?.text ||
  'Daily Akari: automatic posting is on. Save Settings to allow site access; Pro Mode is required.') : 'Daily Akari automatic posting is off.';
$('#linkedin-status').textContent = enabled(automatic, 'autoLinkedIn') ? (automatic.linkedinStatus?.text ||
  'LinkedIn: automatic posting of your own score is on. Save Settings to allow site access.') : 'LinkedIn automatic posting is off. Reading a leaderboard does not post your score to Discord.';
