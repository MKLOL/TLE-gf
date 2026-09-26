import {DEFAULT_SERVER, originPermission, request, serverURL} from "./api.js";

const status = document.querySelector("#status");
const server = document.querySelector("#server");
const token = document.querySelector("#token");
await chrome.storage.local.setAccessLevel({accessLevel: "TRUSTED_CONTEXTS"});
const stored = await chrome.storage.local.get(["server", "token", "autoAkari"]);
server.value = stored.server || DEFAULT_SERVER;
token.value = stored.token || "";
document.querySelector("#auto-akari").checked = stored.autoAkari === true;

document.querySelector("#settings").addEventListener("submit", async event => {
  event.preventDefault();
  try {
    const config = {server: serverURL(server.value), token: token.value.trim(), autoAkari: document.querySelector("#auto-akari").checked};
    if (!/^tlegames_[A-Za-z0-9_-]{43}$/.test(config.token)) {
      throw new Error("Use the games token from ;make-games-token.");
    }
    // Request optional access directly within the user's Save gesture.
    const granted = await chrome.permissions.request({origins: [originPermission(config.server), ...(config.autoAkari ? ["https://dailyakari.com/*"] : [])]});
    if (!granted) throw new Error("Server access was not granted.");
    status.textContent = "Checking connection…";
    const catalog = await request(config, "/v1/games");
    await chrome.storage.local.set(config);
    const activation = await chrome.runtime.sendMessage({type: "configure-akari"});
    if (!activation?.ok) throw new Error(activation?.text || "Could not activate game capture. Save again.");
    await chrome.storage.session.remove(["pendingImport", "pendingOwn"]);
    status.textContent = `Connected to ${catalog.guild_name} as ${catalog.user_name}.`;
  } catch (error) {
    status.textContent = error.message;
  }
});

document.querySelector("#forget").addEventListener("click", async () => {
  await chrome.storage.local.remove("token");
  await chrome.storage.session.remove(["pendingImport", "pendingOwn"]);
  token.value = "";
  status.textContent = "Token removed from Chrome. Use ;revoke-games-token in Discord to revoke it.";
});
