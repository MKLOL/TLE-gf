export const DEFAULT_SERVER = "http://51.81.82.26:8080";

export function serverURL(value) {
  const url = new URL(value.trim());
  if (!["http:", "https:"].includes(url.protocol) || url.username || url.password ||
      url.search || url.hash) throw new Error("Enter an HTTP or HTTPS server URL.");
  return url.href.replace(/\/+$/, "");
}

export function originPermission(server) {
  const url = new URL(server);
  // Chrome match patterns apply to every port on a host.
  return `${url.protocol}//${url.hostname}/*`;
}

export async function settings() {
  await chrome.storage.local.setAccessLevel({accessLevel: "TRUSTED_CONTEXTS"});
  const config = await chrome.storage.local.get(["server", "token"]);
  if (!config.token) throw new Error("Open Settings and paste your games token first.");
  return {...config, server: serverURL(config.server || DEFAULT_SERVER)};
}

export async function request(config, route, body) {
  if (!await chrome.permissions.contains({origins: [originPermission(config.server)]})) {
    throw new Error("Open Settings and save to allow access to your server.");
  }
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 25000);
  try {
    const response = await fetch(config.server + route, {
      method: body === undefined ? "GET" : "POST",
      headers: {Authorization: `Bearer ${config.token}`, "Content-Type": "application/json"},
      ...(body === undefined ? {} : {body: JSON.stringify(body)}),
      credentials: "omit", redirect: "error", cache: "no-store", signal: controller.signal,
    });
    const data = await response.json();
    if (!response.ok) throw new Error(data.error || `Server returned ${response.status}.`);
    return data;
  } catch (error) {
    if (error.name === "AbortError") throw new Error("Server timed out. You can retry Confirm safely.");
    if (error instanceof TypeError) throw new Error("Cannot reach the server. Check its URL and connection.");
    throw error;
  } finally {
    clearTimeout(timer);
  }
}
