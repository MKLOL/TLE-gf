import {request, settings} from './api.js';

async function configureOnce() {
  await chrome.storage.local.setAccessLevel({accessLevel: 'TRUSTED_CONTEXTS'});
  const {autoAkari, token} = await chrome.storage.local.get(['autoAkari', 'token']);
  const registered = await chrome.scripting.getRegisteredContentScripts({ids: ['tle-akari']});
  const allowed = autoAkari && token && await chrome.permissions.contains({origins: ['https://dailyakari.com/*']});
  if (!allowed && registered.length) await chrome.scripting.unregisterContentScripts({ids: ['tle-akari']});
  if (allowed && !registered.length) await chrome.scripting.registerContentScripts([{
    id: 'tle-akari', matches: ['https://dailyakari.com/akari', 'https://dailyakari.com/akari.html'], js: ['akari.js'],
    allFrames: true, runAt: 'document_start', persistAcrossSessions: true,
  }]);
}

let configuration = Promise.resolve();
function configure() {
  configuration = configuration.catch(() => {}).then(configureOnce);
  return configuration;
}

async function handle(message, sender) {
  if (message?.type === 'configure-akari' && sender.id === chrome.runtime.id &&
      sender.url?.split(/[?#]/, 1)[0] === chrome.runtime.getURL('options.html')) {
    await configure(); return {ok: true};
  }
  if (!['akari-result', 'akari-status'].includes(message?.type)) return;
  // Content scripts cannot choose the destination, credentials, guild or user.
  const url = new URL(sender.url || 'https://invalid/');
  if (sender.id !== chrome.runtime.id || !sender.tab || sender.frameId === 0 ||
      url.origin !== 'https://dailyakari.com' || !['/akari', '/akari.html'].includes(url.pathname)) return {ok: false};
  const {autoAkari} = await chrome.storage.local.get('autoAkari');
  if (!autoAkari) return {ok: false};
  let status;
  try {
    if (message.type === 'akari-status') throw new Error(String(message.error).slice(0, 150));
    if (message.body?.game !== 'akari') throw new Error('Invalid Akari result.');
    const result = await request(await settings(), '/v1/games/results', message.body);
    status = {ok: true, text: `Akari #${result.puzzle_number}: ${result.duplicate ? 'already registered and posted' : 'registered and posted'}.`, message_url: result.message_url};
  } catch (error) { status = {ok: false, text: error.message}; }
  await chrome.storage.local.set({akariStatus: status});
  await chrome.action.setBadgeText({text: status.ok ? '✓' : '!'});
  await chrome.action.setBadgeBackgroundColor({color: status.ok ? '#227a51' : '#9f422e'});
  return status;
}

chrome.runtime.onMessage.addListener((message, sender, respond) => {
  handle(message, sender).then(respond, error => respond({ok: false, text: error.message}));
  return true;
});
chrome.runtime.onInstalled.addListener(() => { configure().catch(() => {}); });
chrome.runtime.onStartup.addListener(() => { configure().catch(() => {}); });
chrome.storage.onChanged.addListener((changes, area) => {
  if (area === 'local' && (changes.autoAkari || changes.token)) configure().catch(() => {});
});
