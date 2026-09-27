/* Open a durable extension page; losing toolbar focus must not lose a review. */
export async function openReview(previewId) {
  const pageURL = chrome.runtime.getURL("review.html");
  const url = pageURL + "#" + encodeURIComponent(previewId);
  const {reviewWindow} = await chrome.storage.session.get("reviewWindow");
  try {
    // tabs.get hides even our own page's URL without broad tabs permission.
    // Contexts identify only extension pages and survive toolbar popup closure.
    const contexts = await chrome.runtime.getContexts?.({contextTypes: ["TAB"]});
    const context = contexts?.find(item => item.documentUrl?.split("#")[0] === pageURL);
    const tab = context ? {id: context.tabId, windowId: context.windowId, url: context.documentUrl} :
      reviewWindow?.tabId !== undefined ? await chrome.tabs.get(reviewWindow.tabId) : null;
    // Older Chromium can lack getContexts. Never reuse a tab unless its URL
    // proves this is our review; an old tab ID alone is not sufficient.
    if (tab?.url?.split("#")[0] === pageURL) {
      if (tab.url !== url) await chrome.tabs.update(tab.id, {url});
      await chrome.tabs.update(tab.id, {active: true});
      await chrome.windows.update(tab.windowId, {focused: true});
      await chrome.storage.session.set({reviewWindow: {id: tab.windowId, tabId: tab.id, previewId}});
      return;
    }
  } catch { /* The previous review was closed; create a new one. */ }
  let created;
  try {
    created = await chrome.windows.create({url, type: "popup", width: 1080, height: 800});
  } catch {
    try {
      const tab = await chrome.tabs.create({url, active: true});
      created = {id: tab.windowId, tabs: [tab]};
    } catch {
      throw new Error("Could not open the review window. Your preview is saved; try Review matches again.");
    }
  }
  if (created?.tabs?.[0]?.id !== undefined) {
    await chrome.storage.session.set({reviewWindow: {
      id: created.id, tabId: created.tabs[0].id, previewId,
    }});
  }
}
