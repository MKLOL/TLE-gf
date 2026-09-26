import {request, settings} from "./api.js";

const $ = selector => document.querySelector(selector);
let config, catalog, pending;
let busy = false;
function setBusy(value) {
  busy = value;
  for (const selector of ["#read", "#confirm", "#cancel", "#date"]) $(selector).disabled = value;
}
function showPreview(data) {
  pending = data;
  $("#date").value = data.puzzle_date;
  $("#title").textContent = `${data.game_name} #${data.puzzle_number}`;
  $("#count").textContent = `${data.rows.length} players`;
  $("#summary").textContent = `${data.puzzle_date} · ${data.registered} registered · ${data.unresolved} unlinked` +
    (data.skipped ? ` · ${data.skipped} banned or duplicate players skipped` : "");
  $("#players").replaceChildren();
  for (const entry of data.rows) {
    const tr = document.createElement("tr");
    const name = document.createElement("td");
    name.textContent = entry.name;
    const detail = document.createElement("small");
    detail.textContent = entry.registered ? entry.discord_name : "Unlinked · stored for later";
    name.append(detail);
    const time = document.createElement("td");
    time.textContent = `${Math.floor(entry.time_seconds / 60)}:${String(entry.time_seconds % 60).padStart(2, "0")}`;
    const flags = document.createElement("td");
    flags.textContent = [entry.no_hints ? "No hints" : "", entry.no_mistakes ? "No mistakes" : "",
      !entry.rated ? "Unrated" : ""].filter(Boolean).join(" · ") || "—";
    tr.append(name, time, flags);
    $("#players").append(tr);
  }
  $("#preview").hidden = false;
}
async function clearPreview() {
  pending = null;
  $("#preview").hidden = true;
  await chrome.storage.session.remove("pendingImport");
}
$("#settings").addEventListener("click", () => chrome.runtime.openOptionsPage());
$("#cancel").addEventListener("click", async () => {
  await clearPreview();
  $("#status").textContent = "Cancelled. No results imported.";
});
$("#read").addEventListener("click", async () => {
  if (busy) return;
  setBusy(true);
  try {
    await clearPreview();
    config = await settings();
    catalog = await request(config, "/v1/games");
    const [tab] = await chrome.tabs.query({active: true, currentWindow: true});
    // Inject no token, URL setting, or other privileged data into LinkedIn.
    const result = await chrome.scripting.executeScript({
      target: {tabId: tab.id}, files: ["leaderboard.js", "extract.js"],
    });
    const extracted = result[0]?.result;
    if (!extracted || extracted.error) throw new Error(extracted?.error || "Could not read this page.");
    const game = catalog.games.find(item => item.path === extracted.gamePath);
    if (!game) throw new Error("This game is not supported by your bot yet.");
    if (!game.enabled) throw new Error(`${game.name} is not enabled in your server.`);
    if (!game.can_import) throw new Error(`Your account needs the server’s Admin or Moderator role.`);
    if (extracted.puzzleNumber !== null) {
      const date = new Date(game.anchor_date + "T00:00:00Z");
      date.setUTCDate(date.getUTCDate() + extracted.puzzleNumber - game.anchor_number);
      $("#date").value = date.toISOString().slice(0, 10);
    }
    $("#status").textContent = "Matching players with your server…";
    const data = await request(config, "/v1/games/imports/preview", {
      game: game.id, puzzle_date: $("#date").value || game.today,
      puzzle_number: extracted.puzzleNumber, leaderboard: extracted.leaderboard,
    });
    await chrome.storage.session.set({pendingImport: {server: config.server, data}});
    showPreview(data);
    $("#status").textContent = "Check the players and date, then confirm. This preview expires in 10 minutes.";
  } catch (error) { $("#status").textContent = error.message; }
  finally { setBusy(false); }
});
$("#confirm").addEventListener("click", async () => {
  if (busy || !pending) return;
  setBusy(true);
  try {
    if (Date.now() >= pending.expires_at * 1000) throw new Error("Preview expired. Read the leaderboard again.");
    config = await settings();
    const result = await request(config, `/v1/games/imports/${encodeURIComponent(pending.preview_id)}/confirm`, {});
    await clearPreview();
    $("#status").textContent = `Imported ${result.registered} registered and ${result.unresolved} unlinked results. ${result.unchanged} unchanged.`;
  } catch (error) { $("#status").textContent = error.message; }
  finally { setBusy(false); }
});

try {
  setBusy(true);
  config = await settings();
  catalog = await request(config, "/v1/games");
  $("#connection").textContent = `${catalog.guild_name} · ${catalog.user_name}`;
  $("#date").value = catalog.games[0]?.today || "";
  const {pendingImport} = await chrome.storage.session.get("pendingImport");
  if (pendingImport?.server === config.server && pendingImport.data.expires_at * 1000 > Date.now()) {
    showPreview(pendingImport.data);
    $("#status").textContent = "Your pending preview is ready to confirm.";
  }
} catch (error) { $("#status").textContent = error.message; }
finally { setBusy(false); }
