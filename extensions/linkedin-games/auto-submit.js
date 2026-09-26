import {request, settings} from './api.js';
import {enabled, AKARI_ORIGIN, LINKEDIN_ORIGIN} from './auto-config.js';

export async function automaticMessage(message, sender) {
  const linkedin = message.type === 'linkedin-result';
  const url = new URL(sender.url || 'https://invalid/');
  if (sender.id !== chrome.runtime.id || !sender.tab) return {ok: false};
  if (linkedin) {
    // sender.url retains the original document URL after LinkedIn's SPA navigation.
    // Chrome's tab URL supplies the current top-level route; validate both origins.
    const current = new URL(sender.tab.url || 'https://invalid/');
    if (sender.frameId !== 0 || url.origin !== 'https://www.linkedin.com' ||
        current.origin !== url.origin || !/^\/games\/(?:view\/)?[^/]+\/results(?:\/|$)/.test(current.pathname)) return {ok: false};
    const path = current.pathname.match(/^\/games\/(?:view\/)?([^/]+)/)?.[1];
    if (message.gamePath !== path || !Number.isSafeInteger(message.puzzleNumber) || message.puzzleNumber < 1 ||
        !Number.isInteger(message.timeSeconds) || message.timeSeconds < 0 || message.timeSeconds > 86399) return {ok: false};
  } else if (sender.frameId === 0 || url.origin !== 'https://dailyakari.com' ||
             !['/akari', '/akari.html'].includes(url.pathname)) return {ok: false};
  const key = linkedin ? 'autoLinkedIn' : 'autoAkari';
  const config = await chrome.storage.local.get(['autoLinkedIn', 'autoAkari']);
  const site = linkedin ? LINKEDIN_ORIGIN : AKARI_ORIGIN;
  if (!enabled(config, key) || !await chrome.permissions.contains({origins: [site]})) return {ok: false, disabled: true};
  let status;
  try {
    if (message.type === 'akari-status') throw new Error(String(message.error).slice(0, 150));
    const connection = await settings();
    let body = message.body, name = 'Akari';
    if (linkedin) {
      const catalog = await request(connection, '/v1/games');
      const game = catalog.games.find(item => item.path === message.gamePath);
      if (!game) return {ok: false, skipped: true};
      if (!game.enabled) throw new Error(`${game.name} is not enabled in your server.`);
      const date = new Date(game.anchor_date + 'T00:00:00Z');
      date.setUTCDate(date.getUTCDate() + message.puzzleNumber - game.anchor_number);
      const puzzleDate = date.toISOString().slice(0, 10);
      if (puzzleDate !== game.today) return {ok: false, skipped: true};
      name = game.name;
      body = {game: game.id, puzzle_date: puzzleDate, puzzle_number: message.puzzleNumber,
        time_seconds: message.timeSeconds, accuracy: 100, is_perfect: true};
    } else if (body?.game !== 'akari') throw new Error('Invalid Akari result.');
    // A user may disable automatic posting or switch accounts during catalog IO.
    const current = await chrome.storage.local.get(['autoLinkedIn', 'autoAkari', 'token', 'server']);
    if (!enabled(current, key) || current.token !== connection.token ||
        (current.server && current.server !== connection.server) ||
        !await chrome.permissions.contains({origins: [site]})) return {ok: false, disabled: true};
    const result = await request(connection, '/v1/games/results', body);
    status = {ok: true, text: `${name} #${result.puzzle_number}: ${result.duplicate ? 'already registered and posted' : 'registered and posted'}.`, message_url: result.message_url};
  } catch (error) { status = {ok: false, text: error.message}; }
  await chrome.storage.local.set({[linkedin ? 'linkedinStatus' : 'akariStatus']: status});
  await chrome.action.setBadgeText({text: status.ok ? '✓' : '!'});
  await chrome.action.setBadgeBackgroundColor({color: status.ok ? '#227a51' : '#9f422e'});
  return status;
}
