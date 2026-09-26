import {configureAutomatic} from './auto-config.js';
import {automaticMessage} from './auto-submit.js';

let configuration = Promise.resolve();
function configure() {
  configuration = configuration.catch(() => {}).then(configureAutomatic);
  return configuration;
}

async function handle(message, sender) {
  if (['configure-akari', 'configure-auto'].includes(message?.type) && sender.id === chrome.runtime.id &&
      sender.url?.split(/[?#]/, 1)[0] === chrome.runtime.getURL('options.html')) {
    await configure(); return {ok: true};
  }
  if (!['akari-result', 'akari-status', 'linkedin-result'].includes(message?.type)) return;
  return automaticMessage(message, sender);
}

chrome.runtime.onMessage.addListener((message, sender, respond) => {
  handle(message, sender).then(respond, error => respond({ok: false, text: error.message}));
  return true;
});
chrome.runtime.onInstalled.addListener(() => { configure().catch(() => {}); });
chrome.runtime.onStartup.addListener(() => { configure().catch(() => {}); });
chrome.storage.onChanged.addListener((changes, area) => {
  if (area === 'local' && (changes.autoAkari || changes.autoLinkedIn || changes.token)) configure().catch(() => {});
});
chrome.permissions.onAdded.addListener(() => { configure().catch(() => {}); });
chrome.permissions.onRemoved.addListener(() => { configure().catch(() => {}); });
