// fetch-data.js — runs every 10 min via GitHub Actions
import fetch from 'node-fetch';
import Papa from 'papaparse';

const GW2          = 'https://api.guildwars2.com/v2';
const BIN_KEY      = process.env.JSONBIN_KEY;
const BIN_ID       = process.env.JSONBIN_BIN_ID;
const BIN_HIST_ID  = process.env.JSONBIN_HIST_ID;
const BIN_URL      = `https://api.jsonbin.io/v3/b/${BIN_ID}`;
const BIN_HIST_URL = `https://api.jsonbin.io/v3/b/${BIN_HIST_ID}`;

const GITHUB_TOKEN = process.env.GITHUB_TOKEN;
const GITHUB_REPO  = process.env.GITHUB_REPOSITORY; // e.g. "user/repo"
const CACHE_FILE   = 'guild_id_cache.json';
const CACHE_URL    = `https://raw.githubusercontent.com/${process.env.GITHUB_REPOSITORY}/main/${CACHE_FILE}`;

const MAX_SNAPSHOTS  = 12;
const SHEET_URL      = 'https://docs.google.com/spreadsheets/d/1Txjpcet-9FDVek6uJ0N3OciwgbpE0cfWozUK7ATfWx4/export?format=csv&gid=1120510750';
const SOLO_SHEET_URL     = 'https://docs.google.com/spreadsheets/d/1Txjpcet-9FDVek6uJ0N3OciwgbpE0cfWozUK7ATfWx4/export?format=csv&gid=768688698';
const SCHEDULE_SHEET_URL = 'https://docs.google.com/spreadsheets/d/15mLFZNS_DriY4OVFvdCsu_tlo8E8G89WMbFR6PI4zXM/export?format=csv';
const MY_ALLIANCE_ID = '4F2CA889-AA1F-EF11-81AB-F50023EE1BF3';

const WVW_TEAM_NAMES = {
  "11001":"Moogooloo","11002":"Rall's Rest","11003":"Domain of Torment",
  "11004":"Yohlon Haven","11005":"Tombs of Drascir","11006":"Hall of Judgment",
  "11007":"Throne of Balthazar","11008":"Dwayna's Temple","11009":"Abaddon's Prison",
  "11010":"Cathedral of Blood","11011":"Lutgardis Conservatory","11012":"Mosswood",
  "12001":"Skrittsburgh","12002":"Fortune's Vale","12003":"Silent Woods",
  "12004":"Ettin's Back","12005":"Domain of Anguish","12006":"Palawadan",
  "12007":"Bloodstone Gulch","12008":"Frost Citadel","12009":"Dragrimmar",
  "12010":"Grenth's Door","12011":"Mirror of Lyssa","12012":"Melandru's Dome",
  "12013":"Kormir's Library","12014":"Great House Aviary","12015":"Bava Nisos",
};

async function gw2(path) {
  const r = await fetch(GW2 + path);
  if (!r.ok) throw new Error(`GW2 API error: ${path} -> ${r.status}`);
  return r.json();
}

function parseCSV(text) {
  return Papa.parse(text, { skipEmptyLines: true }).data;
}

function parseSheet(csvText) {
  const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
  const rows = parseCSV(csvText);
  const alliances = [];
  for (const cols of rows) {
    if (!cols || cols.length < 3) continue;
    const uuidIdx = cols.findIndex(c => UUID_RE.test(c.trim()));
    if (uuidIdx === -1) continue;
    const allianceId = cols[uuidIdx].trim().toUpperCase();
    const worldName = cols.slice(uuidIdx + 1)
      .map(c => c.trim()).filter(c => c && !/^(TRUE|FALSE)$/i.test(c)).join(' ').trim();
    if (!worldName) continue;
    const allianceName = cols[0].trim();
    if (!allianceName || /^(TRUE|FALSE|\(+)/.test(allianceName)) continue;
    if (allianceName.toLowerCase().includes('alliance:') ||
        allianceName.toLowerCase().includes('lockout') ||
        (allianceName.toLowerCase().startsWith('alliance') && allianceName.length < 12)) continue;
    const guildsRaw = (cols[2] || '').trim();
    const memberGuilds = guildsRaw ? guildsRaw.split(/\r?\n/).map(g => g.trim()).filter(g => g) : [];
    alliances.push({ allianceId, allianceName, memberGuilds, worldName });
  }
  return alliances;
}


// Parses the Solo Guilds sheet (gid=768688698)
// Returns an array of { guildName, guildId, worldName } — one entry per guild.
// Skips rows where API Mismatch = TRUE or World is empty.
function parseSoloGuilds(csvText) {
  const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
  const rows = Papa.parse(csvText, { skipEmptyLines: true }).data;
  const guilds = [];

  for (const cols of rows) {
    if (!cols || cols.length < 20) continue;

    const guildName = cols[0].trim();
    // Skip header rows, empty names, or section title rows
    if (!guildName || /^(solo guilds|𝐒𝐨𝐥𝐨)/i.test(guildName)) continue;
    // Skip rows where name starts with TRUE/FALSE (junk rows)
    if (/^(TRUE|FALSE)$/i.test(guildName)) continue;

    // Col 18 = Guild API ID
    const guildId = (cols[18] || '').trim();
    if (!UUID_RE.test(guildId)) continue;

    // Col 19 = API Mismatch — skip if TRUE
    const apiMismatch = (cols[19] || '').trim().toUpperCase();
    if (apiMismatch === 'TRUE') continue;

    // Col 21 = World name — skip if empty
    const worldName = (cols[21] || '').trim();
    if (!worldName) continue;

    guilds.push({ guildName, guildId: guildId.toUpperCase(), worldName });
  }

  return guilds;
}

// Parses the Run Schedule sheet
// Returns array of { guild, tag, schedule: { Sun: {utc, brt}, ... }, contacts: [] }
function parseRunSchedule(csvText) {
  const DAYS_EN  = ['Sun','Mon','Tue','Wed','Thu','Fri','Sat'];
  const TIME_RE  = /^\d{2}:\d{2}\s*-\s*\d{2}:\d{2}$/;

  function toBRT(t) {
    const m = t.match(/^(\d{2}):(\d{2})\s*-\s*(\d{2}):(\d{2})$/);
    if (!m) return t;
    return `${(+m[1]-3+24)%24}`.padStart(2,'0') + ':' + m[2] + ' - ' +
           `${(+m[3]-3+24)%24}`.padStart(2,'0') + ':' + m[4];
  }

  const rows = Papa.parse(csvText, { skipEmptyLines: true }).data;
  const guilds = [];

  for (let i = 1; i < rows.length; i++) { // skip header row
    const cols = rows[i];
    if (!cols || !cols[0]?.trim()) continue;

    const fullName = cols[0].trim();
    const tagMatch = fullName.match(/\[(.+?)\]/);
    const tag      = tagMatch ? tagMatch[1] : '';
    const name     = fullName.replace(/\s*\[.+?\]\s*/, '').trim();

    const schedule = {};
    for (let d = 0; d < 7; d++) {
      const val = (cols[d + 1] || '').trim();
      if (TIME_RE.test(val)) {
        schedule[DAYS_EN[d]] = { utc: val, brt: toBRT(val) };
      } else if (val.toLowerCase() === 'check') {
        schedule[DAYS_EN[d]] = { utc: 'TBC', brt: 'TBC' };
      }
      // x or empty = no run that day
    }

    guilds.push({ guild: name, tag, schedule, contacts: [], guildId: null });
  }

  return guilds;
}

// ── GitHub cache helpers ──────────────────────────────────────
async function loadGuildIdCache() {
  try {
    const r = await fetch(CACHE_URL + '?t=' + Date.now());
    if (!r.ok) return {};
    return await r.json();
  } catch { return {}; }
}

async function saveGuildIdCache(cache) {
  try {
    // Get current file SHA (required by GitHub API to update a file)
    const apiUrl = `https://api.github.com/repos/${GITHUB_REPO}/contents/${CACHE_FILE}`;
    const meta = await fetch(apiUrl, {
      headers: { Authorization: `Bearer ${GITHUB_TOKEN}`, 'User-Agent': 'wvw-monitor' }
    }).then(r => r.ok ? r.json() : null);

    const sha     = meta?.sha;
    const content = Buffer.from(JSON.stringify(cache, null, 2)).toString('base64');

    const r = await fetch(apiUrl, {
      method: 'PUT',
      headers: {
        Authorization: `Bearer ${GITHUB_TOKEN}`,
        'Content-Type': 'application/json',
        'User-Agent': 'wvw-monitor'
      },
      body: JSON.stringify({
        message: 'chore: update guild_id_cache.json',
        content,
        ...(sha ? { sha } : {})
      })
    });

    if (r.ok) console.log('guild_id_cache.json saved to GitHub');
    else { const t = await r.text(); console.error('GitHub cache save failed:', t); }
  } catch(e) {
    console.error('GitHub cache save error:', e.message);
  }
}

async function binGet(url) {
  try {
    const r = await fetch(url + '/latest', { headers: { 'X-Master-Key': BIN_KEY } });
    if (!r.ok) return null;
    return (await r.json()).record || null;
  } catch { return null; }
}

async function binPut(url, data, retries = 3) {
  const body = JSON.stringify(data);
  console.log(`PUT ${url} (${body.length} bytes)`);
  for (let attempt = 1; attempt <= retries; attempt++) {
    const r = await fetch(url, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json', 'X-Master-Key': BIN_KEY, 'X-Bin-Versioning': 'false' },
      body,
    });
    if (r.ok) return;
    const t = await r.text();
    console.error(`JSONBin PUT attempt ${attempt}/${retries} failed: ${r.status} ${t}`);
    if (attempt < retries) await new Promise(res => setTimeout(res, 2000 * attempt));
    else throw new Error(`JSONBin PUT failed after ${retries} attempts: ${r.status} ${t}`);
  }
}

(async () => {
  console.log('Fetching data...');
  console.log('BIN_ID:', BIN_ID);
  console.log('BIN_HIST_ID:', BIN_HIST_ID);
  const now = Date.now();

  // 1. Load history
  const histRecord = await binGet(BIN_HIST_URL);
  const snapshots  = histRecord?.snapshots || [];
  const oldSnap    = snapshots.length > 0 ? snapshots[0] : null;
  const oldMinutes = oldSnap ? Math.round((now - oldSnap.timestamp) / 60000) : 0;
  console.log(`History: ${snapshots.length} snapshots, oldest ~${oldMinutes}min ago`);

  // 2. Fetch GW2 data
  const [allMatchIds, worldsRaw, naWvWGuilds, csvText, soloCsvText, scheduleCsvText, ddbrGuildData] = await Promise.all([
    gw2('/wvw/matches'),
    gw2('/worlds?ids=all'),
    gw2('/wvw/guilds/na'),
    fetch(SHEET_URL).then(r => r.text()),
    fetch(SOLO_SHEET_URL).then(r => r.text()),
    fetch(SCHEDULE_SHEET_URL).then(r => r.text()),
    fetch(`${GW2}/guild/${MY_ALLIANCE_ID}`).then(r => r.ok ? r.json() : null).catch(() => null),
  ]);

  const naIds   = allMatchIds.filter(id => id.startsWith('1-')).sort();
  const matches = await Promise.all(naIds.map(id => gw2(`/wvw/matches/${id}`)));

  // 3. World names + team mapping
  const worldNames = {};
  worldsRaw.forEach(w => { worldNames[String(w.id)] = w.name; });

  const worldIdToTeam = {};
  matches.forEach(m => {
    ['red','blue','green'].forEach(color => {
      (m.all_worlds[color] || []).forEach(wid => {
        worldIdToTeam[String(wid)] = { matchId: m.id, color };
      });
    });
  });

  // 4. Parse alliances
  const allianceData = parseSheet(csvText);
  const norm = s => s.toLowerCase().replace(/[^a-z0-9]/g, '');
  const tierAllianceMap = {};
  naIds.forEach(id => { tierAllianceMap[id] = { red:[], blue:[], green:[] }; });

  allianceData.forEach(a => {
    let team = null;
    if (naWvWGuilds?.[a.allianceId]) team = worldIdToTeam[String(naWvWGuilds[a.allianceId])];
    if (!team && a.worldName) {
      const normTarget = norm(a.worldName);
      const matchedWid = Object.keys(worldNames).find(wid => norm(worldNames[wid]) === normTarget);
      if (matchedWid) team = worldIdToTeam[matchedWid];
    }
    if (team && tierAllianceMap[team.matchId]) {
      const wid = naWvWGuilds?.[a.allianceId];
      if (wid && worldNames[String(wid)]) a.worldName = worldNames[String(wid)];
      tierAllianceMap[team.matchId][team.color].push(a);
    }
  });

  // 4b. Parse and place Solo Guilds
  const soloGuilds = parseSoloGuilds(soloCsvText);
  console.log(`Solo guilds parsed: ${soloGuilds.length}`);

  // Group solo guilds by world → build one "[Solo Guilds]" alliance entry per team/color
  // Key: matchId|color → array of guild names
  const soloByTeam = {};
  naIds.forEach(id => { soloByTeam[id] = { red:[], blue:[], green:[] }; });

  const norm2 = s => s.toLowerCase().replace(/[^a-z0-9]/g, '');
  soloGuilds.forEach(g => {
    // Try to resolve via naWvWGuilds API first
    let team = null;
    const apiWorld = naWvWGuilds?.[g.guildId.toUpperCase()]
                  || naWvWGuilds?.[g.guildId.toLowerCase()]
                  || naWvWGuilds?.[g.guildId];
    if (apiWorld) team = worldIdToTeam[String(apiWorld)];

    // Fall back to worldName column
    if (!team && g.worldName) {
      const normTarget = norm2(g.worldName);
      const matchedWid = Object.keys(worldNames).find(wid => norm2(worldNames[wid]) === normTarget);
      if (matchedWid) team = worldIdToTeam[matchedWid];
    }

    if (team && soloByTeam[team.matchId]) {
      soloByTeam[team.matchId][team.color].push(g.guildName);
    }
  });

  // Inject one "[Solo Guilds]" alliance block per team that has at least one guild
  naIds.forEach(matchId => {
    ['red','blue','green'].forEach(color => {
      const guilds = soloByTeam[matchId][color];
      if (!guilds.length) return;
      tierAllianceMap[matchId][color].push({
        allianceId:   `SOLO_${matchId}_${color}`,
        allianceName: '[Solo Guilds]',
        memberGuilds: guilds,
        worldName:    '',
        isSolo:       true,
      });
    });
  });

  const ddbrWorldId = naWvWGuilds?.[MY_ALLIANCE_ID.toUpperCase()]
                   || naWvWGuilds?.[MY_ALLIANCE_ID.toLowerCase()]
                   || naWvWGuilds?.[MY_ALLIANCE_ID];
  if (ddbrWorldId) {
    const ddbrTeam = worldIdToTeam[String(ddbrWorldId)];
    if (ddbrTeam && tierAllianceMap[ddbrTeam.matchId]) {
      const ddbrEntry = {
        allianceId: MY_ALLIANCE_ID,
        allianceName: ddbrGuildData ? `[DDBR] ${ddbrGuildData.name}` : '[DDBR]',
        memberGuilds: [], worldName: worldNames[String(ddbrWorldId)] || '', isDDBR: true,
      };
      const existing = tierAllianceMap[ddbrTeam.matchId][ddbrTeam.color];
      if (!existing.find(a => a.isDDBR)) existing.unshift(ddbrEntry);
    }
  }

  // 4c. Parse run schedule + resolve guild IDs (cached weekly)
  const scheduleGuilds = parseRunSchedule(scheduleCsvText);
  console.log(`Schedule guilds parsed: ${scheduleGuilds.length}`);

  // Decide whether to re-resolve guild IDs (Fridays 17-19 UTC or no cache yet)
  const nowDate       = new Date(now);
  const isFriday      = nowDate.getUTCDay() === 5;
  const isUpdateHour  = nowDate.getUTCHours() >= 17 && nowDate.getUTCHours() < 19;
  const shouldResolve = isFriday && isUpdateHour;

  // Load cached guild ID map from history record
  let guildIdCache = await loadGuildIdCache();
  console.log(`Guild ID cache loaded: ${Object.keys(guildIdCache).length} entries`);

  if (shouldResolve || Object.keys(guildIdCache).length === 0) {
    console.log('Resolving guild IDs from GW2 API...');
    for (const g of scheduleGuilds) {
      const key = g.guild.toLowerCase();
      if (guildIdCache[key]) continue; // already cached
      try {
        const ids = await fetch(`${GW2}/guild/search?name=${encodeURIComponent(g.guild)}`)
          .then(r => r.ok ? r.json() : []);
        if (ids && ids.length > 0) {
          guildIdCache[key] = ids[0]; // take first match
          console.log(`  Resolved: ${g.guild} → ${ids[0]}`);
        } else {
          console.log(`  Not found: ${g.guild}`);
        }
      } catch(e) {
        console.error(`  Error resolving ${g.guild}: ${e.message}`);
      }
    }
    // Save updated cache back into history
    await saveGuildIdCache(guildIdCache);
    console.log(`Guild ID cache updated: ${Object.keys(guildIdCache).length} entries`);
  } else {
    console.log(`Using cached guild IDs (${Object.keys(guildIdCache).length} entries), next refresh: Friday 17-19 UTC`);
  }

  // Attach resolved IDs and determine which team/color each guild belongs to
  scheduleGuilds.forEach(g => {
    const key = g.guild.toLowerCase();
    g.guildId = guildIdCache[key] || null;

    let team = null;
    if (g.guildId) {
      const worldId = naWvWGuilds?.[g.guildId.toUpperCase()]
                   || naWvWGuilds?.[g.guildId.toLowerCase()]
                   || naWvWGuilds?.[g.guildId];
      if (worldId) team = worldIdToTeam[String(worldId)];
    }
    g.matchId = team?.matchId || null;
    g.color   = team?.color   || null;
  });

  // Build scheduleByMatch: { matchId: { red: [...], blue: [...], green: [...] } }
  const scheduleByMatch = {};
  naIds.forEach(id => { scheduleByMatch[id] = { red:[], blue:[], green:[] }; });
  scheduleGuilds.forEach(g => {
    if (g.matchId && g.color && scheduleByMatch[g.matchId]) {
      scheduleByMatch[g.matchId][g.color].push({
        guild:    g.guild,
        tag:      g.tag,
        guildId:  g.guildId,
        schedule: g.schedule,
        contacts: g.contacts,
      });
    }
  });

  // 5. Current kills/deaths
  const nowKills  = {};
  const nowDeaths = {};
  matches.forEach(m => {
    nowKills[m.id]  = { red: m.kills?.red||0,  blue: m.kills?.blue||0,  green: m.kills?.green||0  };
    nowDeaths[m.id] = { red: m.deaths?.red||0, blue: m.deaths?.blue||0, green: m.deaths?.green||0 };
  });

  // 6. K/D delta against oldest snapshot (~2h)
  // Use oldSnap as long as it exists (even if < 10min), so the UI always shows something.
  // The UI already labels the window with actual minutes, so stale data is visible to the user.
  const kdDelta = {};
  if (oldSnap) {
    matches.forEach(m => {
      kdDelta[m.id] = {};
      ['red','blue','green'].forEach(color => {
        const kills  = Math.max(0, (nowKills[m.id][color]  || 0) - (oldSnap.kills[m.id]?.[color]  || 0));
        const deaths = Math.max(0, (nowDeaths[m.id][color] || 0) - (oldSnap.deaths[m.id]?.[color] || 0));
        kdDelta[m.id][color] = { kills, deaths, minutes: Math.max(1, oldMinutes) };
      });
    });
  }

  // 7. Build KD chart series — delta between each consecutive snapshot pair
  // Each point = { t: minutesAgo, kd: { red, blue, green } }
  const kdSeries = {};
  naIds.forEach(id => { kdSeries[id] = []; });

  const allSnaps = [...snapshots, { timestamp: now, kills: nowKills, deaths: nowDeaths }];
  for (let i = 1; i < allSnaps.length; i++) {
    const prev = allSnaps[i - 1];
    const curr = allSnaps[i];
    const point = { t: Math.round((now - curr.timestamp) / 60000) };
    naIds.forEach(matchId => {
      point[matchId] = {};
      ['red','blue','green'].forEach(color => {
        const k = Math.max(0, (curr.kills[matchId]?.[color]  || 0) - (prev.kills[matchId]?.[color]  || 0));
        const d = Math.max(0, (curr.deaths[matchId]?.[color] || 0) - (prev.deaths[matchId]?.[color] || 0));
        point[matchId][color] = d > 0 ? parseFloat((k / d).toFixed(2)) : k > 0 ? 3.0 : null;
      });
    });
    naIds.forEach(id => kdSeries[id].push(point));
  }

  // 8. Update history
  const updatedSnapshots = [...snapshots, { timestamp: now, kills: nowKills, deaths: nowDeaths }].slice(-MAX_SNAPSHOTS);
  await binPut(BIN_HIST_URL, { snapshots: updatedSnapshots });
  console.log(`History saved: ${updatedSnapshots.length}/${MAX_SNAPSHOTS}`);

  // 9. Pre-resolve primary world names
  const primaryWorlds = {};
  matches.forEach(m => {
    primaryWorlds[m.id] = {};
    ['red','blue','green'].forEach(color => {
      const alliances = (tierAllianceMap[m.id] || {})[color] || [];
      let name = null;
      for (const a of alliances) {
        const teamId = naWvWGuilds?.[a.allianceId?.toUpperCase()]
                    || naWvWGuilds?.[a.allianceId?.toLowerCase()]
                    || naWvWGuilds?.[a.allianceId];
        if (teamId && WVW_TEAM_NAMES[String(teamId)]) { name = WVW_TEAM_NAMES[String(teamId)]; break; }
      }
      if (!name) {
        const wids = m.worlds[color] ? [m.worlds[color]] : (m.all_worlds[color] || []);
        name = worldNames[String(wids[0])] || '\u2014';
      }
      primaryWorlds[m.id][color] = name;
    });
  });

  // 10. Save main payload
  await binPut(BIN_URL, {
    timestamp: now,
    kdDelta,
    kdSeries,
    matches: matches.map(m => ({
      id: m.id, start_time: m.start_time, end_time: m.end_time,
      scores: m.scores, kills: m.kills, deaths: m.deaths, victory_points: m.victory_points,
    })),
    tierAllianceMap,
    primaryWorlds,
    scheduleByMatch,
  });

  console.log(`Done. ${naIds.length} matches, ${allianceData.length} alliances, ${soloGuilds.length} solo guilds, K/D window ~${oldMinutes}min`);
})();
