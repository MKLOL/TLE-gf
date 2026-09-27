import {request, settings} from "./api.js";

const $ = selector => document.querySelector(selector);
let config, catalog, pending, pendingConnection;
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
  $("#summary").textContent = `${data.puzzle_date} · ${data.registered} matched · ${data.unresolved} unassigned` +
    (data.skipped ? ` · ${data.skipped} banned or duplicate players skipped` : "") +
    (data.unplayed ? ` · ${data.unplayed} without a completed score` : "");
  $("#players").replaceChildren();
  for (const entry of data.rows) {
    const tr = document.createElement("tr");
    const name = document.createElement("td");
    name.textContent = entry.name;
    const detail = document.createElement("small");
    detail.textContent = entry.registered ? `Matches Discord: ${entry.discord_name}` :
      entry.is_own ? "Your result · will be saved until you register" : "Unassigned · will be saved for later";
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
  $("#read-tools").hidden = true;
  document.body.classList.add("reviewing");
  $("#preview-question").focus();
}
async function clearPreview() {
  pending = null;
  pendingConnection = null;
  $("#preview").hidden = true;
  $("#read-tools").hidden = false;
  document.body.classList.remove("reviewing");
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
    const response = await request(config, "/v1/games/imports/preview", {
      game: game.id, puzzle_date: $("#date").value || game.today,
      puzzle_number: extracted.puzzleNumber, leaderboard: extracted.leaderboard,
    });
    const data = {...response, unplayed: extracted.unplayed || 0};
    pendingConnection = {server: config.server, guild: catalog.guild_id, user: catalog.user_id};
    await chrome.storage.session.set({pendingImport: {...pendingConnection, data}});
    showPreview(data);
    $("#status").textContent = "Please review the matches before importing.";
  } catch (error) { $("#status").textContent = error.message; }
  finally { setBusy(false); }
});
$("#confirm").addEventListener("click", async () => {
  if (busy || !pending) return;
  setBusy(true);
  try {
    if (Date.now() >= pending.expires_at * 1000) {
      await clearPreview();
      throw new Error("This preview expired. Read the leaderboard again for a fresh preview.");
    }
    config = await settings();
    const current = await request(config, "/v1/games");
    if (config.server !== pendingConnection?.server || current.guild_id !== pendingConnection.guild ||
        current.user_id !== pendingConnection.user) {
      await clearPreview();
      throw new Error("Your connection changed. Read the leaderboard again to review the new matches.");
    }
    const result = await request(config, `/v1/games/imports/${encodeURIComponent(pending.preview_id)}/confirm`, {});
    await clearPreview();
    $("#status").textContent = `Imported ${result.registered} registered and ${result.unresolved} unlinked results. ${result.unchanged} unchanged.`;
  } catch (error) {
    if ([401, 403, 404, 409].includes(error.status)) await clearPreview();
    $("#status").textContent = error.message;
  }
  finally { setBusy(false); }
});

try {
  setBusy(true);
  config = await settings();
  catalog = await request(config, "/v1/games");
  $("#read").hidden = !catalog.games.some(game => game.can_import);
  $("#connection").textContent = `${catalog.guild_name} · ${catalog.user_name}`;
  $("#date").value = catalog.games[0]?.today || "";
  const {pendingImport} = await chrome.storage.session.get("pendingImport");
  if (pendingImport?.server === config.server && pendingImport.guild === catalog.guild_id &&
      pendingImport.user === catalog.user_id && pendingImport.data.expires_at * 1000 > Date.now()) {
    pendingConnection = {server: config.server, guild: catalog.guild_id, user: catalog.user_id};
    showPreview(pendingImport.data);
    $("#status").textContent = "Your preview is ready. Please review the matches before importing.";
  } else if (pendingImport) {
    await clearPreview();
  }
} catch (error) { $("#status").textContent = error.message; }
finally { setBusy(false); }
