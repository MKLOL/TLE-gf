/* Verified LinkedIn connections leaderboard selectors; no credentials or clicks.
 * LinkedIn can host the Ember games UI in a same-origin /preload/ frame over
 * its main feed. Never scrape the feed or treat reactions as solve badges.
 */
globalThis.TleReadLeaderboard = (ownOnly = false) => {
  try {
    const helpers = globalThis.TleLeaderboard;
    const gamePath = helpers.pageGame(location.href);
    if (!gamePath) throw new Error("Open a LinkedIn game and its connections leaderboard first.");
    const documents = [document];
    for (const frame of document.querySelectorAll('iframe')) {
      try {
        if (visible(frame) && frame.contentDocument && new URL(frame.src, location.href).origin === location.origin) {
          documents.push(frame.contentDocument);
        }
      } catch { /* Cross-origin frames are irrelevant to the leaderboard. */ }
    }
    function visible(node) {
      if (!node || !node.getClientRects().length) return false;
      const view = node.ownerDocument.defaultView;
      for (let parent = node; parent; parent = parent.parentElement) {
        const style = view.getComputedStyle(parent);
        if (parent.hidden || style.display === 'none' || style.visibility === 'hidden') return false;
      }
      return true;
    }
    const boards = documents.flatMap(doc => [...doc.querySelectorAll('.pr-connections-leaderboard__section')])
      .filter(visible);
    if (boards.length !== 1) throw new Error(boards.length ?
      "More than one leaderboard is open. Open just the one to import." :
      "No connections leaderboard found. Open it after solving, then try again.");
    const board = boards[0];
    const title = board.querySelector('.pr-connections-leaderboard__toolbar-title')?.textContent.trim() || '';
    if (!title.toLowerCase().startsWith(gamePath + ' ')) {
      throw new Error("The displayed leaderboard does not match this game. Wait for it to finish loading.");
    }
    if ([...board.querySelectorAll('.artdeco-loader, [role="progressbar"], [aria-busy="true"]')].some(visible)) {
      throw new Error('The leaderboard is still loading. Wait for it to finish, then try again.');
    }
    const puzzleNumber = helpers.puzzleNumber(board, location.href, visible);
    const leaderboardDay = helpers.selectedDay(board, visible);
    const rows = [], pinned = [];
    let unplayed = 0;
    for (const container of board.querySelectorAll('.pr-connections-leaderboard-player__container')) {
      if (!visible(container)) continue;
      const column = container.querySelector('.pr-connections-leaderboard-player__content-column');
      const isYou = [...(column?.querySelectorAll('*') || [])].some(element =>
        visible(element) && !element.children.length && element.textContent.trim() === 'You');
      if (ownOnly && !isYou) continue;
      if (container.classList.contains('pr-connections-leaderboard-player__container-blur')) {
        throw new Error("LinkedIn has hidden some results. Open a leaderboard with visible results first.");
      }
      const name = container.querySelector('.pr-connections-leaderboard-player__name-text')?.textContent.trim();
      const score = container.querySelector('.pr-connections-leaderboard-player__score')?.textContent.trim();
      if (!score && container.querySelector('.pr-connections-leaderboard-player__nudge-button')) {
        unplayed++; continue;
      }
      if (['–', '—', '-', '-:--'].includes(score)) { unplayed++; continue; }
      if (!name || !score || !helpers.timePattern.test(score)) {
        throw new Error("A leaderboard row has no readable solve time. This game or layout is not supported yet.");
      }
      if (!column) throw new Error("Could not identify the player’s result badges.");
      // Only badge text in the identity/result column counts. Reaction controls
      // elsewhere in the row may themselves contain 🤓 or 💎.
      const badgeText = [];
      for (const element of column.querySelectorAll('.pr-connections-leaderboard-player__subtitle *')) {
        if (!visible(element) || element.closest('.pr-connections-leaderboard-player__subtitle-insight-tag')) continue;
        if (!element.children.length) badgeText.push(element.textContent.trim());
        badgeText.push(element.getAttribute('aria-label') || '', element.getAttribute('alt') || '',
          element.getAttribute('title') || '');
      }
      const status = badgeText.filter(value => /\bno hints?\b|\bno mistakes?\b|🤓|💎/i.test(value)).join(' ');
      const destination = container.closest('.pr-connections-leaderboard__sticky-section') ? pinned : rows;
      destination.push({name, isYou, time: score, status});
    }
    // Yesterday repeats You in a sticky footer. Prefer the ordinary row's name
    // and badges; the footer can contain an unrelated "Top 1% today" insight.
    for (const entry of pinned) {
      const owners = rows.filter(row => row.isYou);
      if (entry.isYou && owners.length === 1) {
        if (entry.time !== owners[0].time) {
          throw new Error('Your pinned score and leaderboard disagree. Wait for the selected day to finish loading, then read again.');
        }
        continue;
      }
      rows.push(entry);
    }
    if (!rows.length) throw new Error("No completed timed results are visible in this leaderboard.");
    if (rows.length > 200) throw new Error("This leaderboard has more than 200 loaded results.");
    if (ownOnly && rows.length !== 1) throw new Error("Could not identify exactly one completed You row.");
    const leaderboard = helpers.serialize(rows);
    if (leaderboard.length > 12000) throw new Error("This leaderboard is too large to import.");
    return {gamePath, puzzleNumber, leaderboardDay, leaderboard, count: rows.length, unplayed, rows};
  } catch (error) {
    return {error: error.message};
  }
};
globalThis.TleReadLeaderboard();
