import {request, settings} from "./api.js";
import {openReview} from "./review-window.js";

const $ = selector => document.querySelector(selector);
let config, catalog, pending;
let busy = false, waitTimer;
function progress(message, buttonLabel) {
  $("#status").textContent = message;
  $("#status").classList.add("loading");
  $("#read").textContent = buttonLabel;
}
function setBusy(value) {
  busy = value;
  for (const selector of ["#read", "#open-review", "#cancel", "#date", "#own", "#post-own", "#cancel-own"]) {
    $(selector).disabled = value;
  }
  $("#read-tools").setAttribute("aria-busy", String(value));
  clearInterval(waitTimer);
  $("#wait-note").hidden = true;
  if (value) {
    const started = Date.now();
    waitTimer = setInterval(() => {
      const seconds = Math.floor((Date.now() - started) / 1000);
      if (seconds < 5) return;
      $("#wait-note").textContent = `Still working · ${seconds}s elapsed. Please keep this popup open.`;
      $("#wait-note").hidden = false;
    }, 1000);
  } else {
    $("#status").classList.remove("loading");
    $("#read").textContent = "Read leaderboard";
  }
}
function showPreview(data) {
  pending = data;
  $("#title").textContent = `${data.game_name} #${data.puzzle_number}`;
  $("#summary").textContent = `${data.puzzle_date} · ${data.registered} matched · ${data.unresolved} unassigned`;
  $("#preview").hidden = false;
}
async function clearPreview() {
  const previewId = pending?.preview_id;
  pending = null;
  $("#preview").hidden = true;
  const {pendingImport} = await chrome.storage.session.get("pendingImport");
  if (pendingImport && pendingImport.data.preview_id === previewId) {
    await chrome.storage.session.remove("pendingImport");
  }
}
function sameConnection(saved) {
  return saved?.server === config.server && saved.guild === catalog.guild_id && saved.user === catalog.user_id;
}
$("#settings").addEventListener("click", () => chrome.runtime.openOptionsPage());
$("#cancel").addEventListener("click", async () => {
  await clearPreview();
  $("#status").textContent = "Preview discarded. No results imported.";
});
$("#open-review").addEventListener("click", async () => {
  if (busy || !pending) return;
  setBusy(true);
  progress("Opening your leaderboard review…", "Opening…");
  try {
    if (pending.expires_at * 1000 <= Date.now()) {
      await clearPreview();
      throw new Error("This preview expired. Read the leaderboard for a fresh preview.");
    }
    await openReview(pending.preview_id);
    $("#status").textContent = "Review opened in a larger window. Nothing is imported until you confirm there.";
  } catch (error) { $("#status").textContent = error.message; }
  finally { setBusy(false); }
});
$("#read").addEventListener("click", async () => {
  if (busy) return;
  setBusy(true);
  progress("1/3 · Connecting to your Discord server…", "Connecting…");
  try {
    await clearPreview();
    config = await settings();
    catalog = await request(config, "/v1/games");
    $("#connection").textContent = `${catalog.guild_name} · ${catalog.user_name}`;
    progress("2/3 · Reading the visible LinkedIn leaderboard…", "Reading…");
    const [tab] = await chrome.tabs.query({active: true, currentWindow: true});
    if (!tab?.id) throw new Error("Open your LinkedIn leaderboard tab, then read it again.");
    // Inject no credentials into LinkedIn. These scripts only read visible DOM.
    const result = await chrome.scripting.executeScript({
      target: {tabId: tab.id}, files: ["leaderboard.js", "extract.js"],
    });
    const extracted = result[0]?.result;
    if (!extracted || extracted.error) throw new Error(extracted?.error || "Could not read this page.");
    const game = catalog.games.find(item => item.path === extracted.gamePath);
    if (!game) throw new Error("This game is not supported by your bot yet.");
    if (!game.enabled) throw new Error(`${game.name} is not enabled in your server.`);
    if (!game.can_import) throw new Error("Your account needs the server’s Admin or Moderator role.");
    const date = new Date(game.anchor_date + "T00:00:00Z");
    date.setUTCDate(date.getUTCDate() + extracted.puzzleNumber - game.anchor_number);
    $("#date").value = date.toISOString().slice(0, 10);
    progress("3/3 · Matching players with Discord accounts…", "Matching…");
    const response = await request(config, "/v1/games/imports/preview", {
      game: game.id, puzzle_date: $("#date").value,
      puzzle_number: extracted.puzzleNumber, leaderboard: extracted.leaderboard,
    });
    const data = {...response, unplayed: extracted.unplayed || 0};
    // The toolbar popup may close as soon as the review window gains focus.
    // Persist everything before opening it, and bind its URL to this preview.
    await chrome.storage.session.set({pendingImport: {
      server: config.server, guild: catalog.guild_id, user: catalog.user_id,
      guildName: catalog.guild_name, userName: catalog.user_name, data,
    }});
    showPreview(data);
    progress("Opening the full leaderboard review…", "Opening…");
    await openReview(data.preview_id);
    $("#status").textContent = "Review opened in a larger window. Check the matches and confirm there.";
  } catch (error) { $("#status").textContent = error.message; }
  finally { setBusy(false); }
});

try {
  setBusy(true);
  progress("Connecting to your Discord server…", "Connecting…");
  config = await settings();
  catalog = await request(config, "/v1/games");
  $("#read").hidden = !catalog.games.some(game => game.can_import);
  $("#connection").textContent = `${catalog.guild_name} · ${catalog.user_name}`;
  $("#date").value = catalog.games[0]?.today || "";
  const {pendingImport} = await chrome.storage.session.get("pendingImport");
  if (sameConnection(pendingImport) && pendingImport.data.expires_at * 1000 > Date.now()) {
    showPreview(pendingImport.data);
    $("#status").textContent = "Your leaderboard preview is ready to review in a larger window.";
  } else {
    if (pendingImport) {
      pending = pendingImport.data;
      await clearPreview();
    }
    $("#status").textContent = "Ready. Read a leaderboard to review all matches in a larger window.";
  }
} catch (error) { $("#status").textContent = error.message; }
finally { setBusy(false); }
