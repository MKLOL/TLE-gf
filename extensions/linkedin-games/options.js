import {DEFAULT_SERVER, originPermission, request, serverURL} from "./api.js";

import {enabled, AKARI_ORIGIN, LINKEDIN_ORIGIN} from "./auto-config.js";

const status = document.querySelector("#status");
const server = document.querySelector("#server");
const token = document.querySelector("#token");
await chrome.storage.local.setAccessLevel({accessLevel: "TRUSTED_CONTEXTS"});
const stored = await chrome.storage.local.get(["server", "token", "autoAkari", "autoLinkedIn"]);
server.value = stored.server || DEFAULT_SERVER;
token.value = stored.token || "";
document.querySelector("#auto-akari").checked = enabled(stored, "autoAkari");

document.querySelector("#auto-linkedin").checked = enabled(stored, "autoLinkedIn");

// Turning posting off is local and must work even if the API/token is unavailable.
let settingsRevision = 0;
for (const input of [server, token]) {
  input.addEventListener('input', () => {
    settingsRevision++;
    status.textContent = 'Save to apply the changed connection.';
  });
}
for (const [selector, key, name] of [['#auto-akari', 'autoAkari', 'Akari'], ['#auto-linkedin', 'autoLinkedIn', 'LinkedIn']]) {
  document.querySelector(selector).addEventListener('change', async event => {
    settingsRevision++;
    if (event.target.checked) { status.textContent = 'Save to enable automatic posting.'; return; }
    try {
      await chrome.storage.local.set({[key]: false});
      const activation = await chrome.runtime.sendMessage({type: 'configure-auto'});
      if (!activation?.ok) throw new Error(activation?.text || 'Could not update game capture.');
      status.textContent = `Automatic ${name} posting is off.`;
    } catch (error) { status.textContent = error.message; }
  });
}

document.querySelector("#settings").addEventListener("submit", async event => {
  event.preventDefault();
  const revision = ++settingsRevision;
  try {
    const config = {server: serverURL(server.value), token: token.value.trim(), autoAkari: document.querySelector("#auto-akari").checked,
      autoLinkedIn: document.querySelector("#auto-linkedin").checked};
    if (!/^tlegames_[A-Za-z0-9_-]{43}$/.test(config.token)) {
      throw new Error("Use the games token from ;make-games-token.");
    }
    // Request optional access directly within the user's Save gesture.
    const granted = await chrome.permissions.request({origins: [originPermission(config.server), ...(config.autoAkari ? [AKARI_ORIGIN] : []),
      ...(config.autoLinkedIn ? [LINKEDIN_ORIGIN] : [])]});
    if (revision !== settingsRevision) return;
    if (!granted) throw new Error("Server access was not granted.");
    status.textContent = "Checking connection…";
    const catalog = await request(config, "/v1/games");
    if (revision !== settingsRevision) return;
    await chrome.storage.local.set(config);
    const activation = await chrome.runtime.sendMessage({type: "configure-auto"});
    if (!activation?.ok) throw new Error(activation?.text || "Could not activate game capture. Save again.");
    await chrome.storage.session.remove(["pendingImport", "pendingOwn"]);
    if (revision === settingsRevision) status.textContent = `Connected to ${catalog.guild_name} as ${catalog.user_name}.`;
  } catch (error) {
    if (revision === settingsRevision) status.textContent = error.message;
  }
});

document.querySelector("#forget").addEventListener("click", async () => {
  settingsRevision++;
  await chrome.storage.local.remove("token");
  await chrome.storage.session.remove(["pendingImport", "pendingOwn"]);
  token.value = "";
  status.textContent = "Token removed from Chrome. Use ;revoke-games-token in Discord to revoke it.";
});
