// Older versions saved autoLinkedIn=true as part of default-on setup. Require
// fresh, explicit consent so upgrading also stops those background submissions.
export const LINKEDIN_CONSENT_VERSION = 1;
export const AUTOMATIC_KEYS = ['autoAkari', 'autoLinkedIn', 'autoLinkedInConsentVersion'];
export const enabled = (config, key) => key === 'autoLinkedIn'
  ? config.autoLinkedIn === true && config.autoLinkedInConsentVersion === LINKEDIN_CONSENT_VERSION
  : config[key] !== false;
export const AKARI_ORIGIN = 'https://dailyakari.com/*';
export const LINKEDIN_ORIGIN = 'https://www.linkedin.com/*';

export async function configureAutomatic() {
  await chrome.storage.local.setAccessLevel({accessLevel: 'TRUSTED_CONTEXTS'});
  const config = await chrome.storage.local.get([...AUTOMATIC_KEYS, 'token']);
  const definitions = [
    {key: 'autoAkari', origin: AKARI_ORIGIN, script: {
      id: 'tle-akari', matches: ['https://dailyakari.com/akari', 'https://dailyakari.com/akari.html'],
      js: ['akari.js'], allFrames: true,
    }},
    {key: 'autoLinkedIn', origin: LINKEDIN_ORIGIN, script: {
      // LinkedIn can enter games from its feed without a document navigation.
      // The reader stays idle outside /games/ and never reads feed content.
      id: 'tle-linkedin', matches: [LINKEDIN_ORIGIN],
      js: ['leaderboard.js', 'linkedin-result.js', 'linkedin-auto.js'], allFrames: false,
    }},
  ];
  for (const {key, origin, script} of definitions) {
    const registered = await chrome.scripting.getRegisteredContentScripts({ids: [script.id]});
    const allowed = config.token && enabled(config, key) && await chrome.permissions.contains({origins: [origin]});
    if (!allowed && registered.length) await chrome.scripting.unregisterContentScripts({ids: [script.id]});
    if (allowed && !registered.length) await chrome.scripting.registerContentScripts([{
      ...script, runAt: 'document_start', persistAcrossSessions: true,
    }]);
  }
}
