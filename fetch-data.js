// fetch-data.js — runs every 15 min via GitHub Actions
import fetch from 'node-fetch';
import Papa from 'papaparse';

const GW2          = 'https://api.guildwars2.com/v2';
const BIN_KEY      = process.env.JSONBIN_KEY;
const BIN_ID       = process.env.JSONBIN_BIN_ID;
const BIN_HIST_ID  = process.env.JSONBIN_HIST_ID;
const BIN_URL      = `https://api.jsonbin.io/v3/b/${BIN_ID}`;
const BIN_HIST_URL = `https://api.jsonbin.io/v3/b/${BIN_HIST_ID}`;

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

    guilds.push({ guild: name, tag, schedule, contacts: [] });
  }

  return guilds;
}

// Normalise a string for fuzzy comparison: lowercase, strip non-alphanumeric
const norm = s => s.toLowerCase().replace(/[^a-z0-9]/g, '');

// Extract tag from a string like "Guild Name [TAG]" → "TAG" (uppercased), or ""
function extractTag(s) {
  const m = s.match(/\[(.+?)\]/);
  return m ? m[1].trim().toUpperCase() : '';
}

// Strip tag bracket from a string: "Guild Name [TAG]" → "Guild Name"
function stripTag(s) {
  return s.replace(/\s*\[.+?\]\s*/, '').trim();
}

/**
 * Given the fully-built tierAllianceMap and the list of schedule guilds,
 * resolve each guild's matchId + color by looking up its name inside
 * the memberGuilds arrays of every alliance already on the page.
 *
 * Matching strategy (in order of priority):
 *   1. norm(memberName with tag stripped)  === norm(g.guild)   [name-only match]
 *   2. norm(memberName full)               === norm(g.guild)   [in case sheet has no tag]
 *   3. extractTag(memberName)              === g.tag           [tag-only match, last resort]
 *
 * Multiple keys are stored per memberName so any of the above can hit.
 *
 * Returns array with same order as scheduleGuilds, each enriched with
 * { matchId, color, allianceName } or nulls if not found.
 */
function resolveScheduleGuildsByName(scheduleGuilds, tierAllianceMap) {
  // Two lookup maps:
  //   byName: norm(name-without-tag) → hit
  //   byTag:  UPPER(tag)             → hit  (only used as last-resort fallback)
  const byName = new Map();
  const byTag  = new Map();

  for (const [matchId, colorMap] of Object.entries(tierAllianceMap)) {
    for (const [color, alliances] of Object.entries(colorMap)) {
      for (const alliance of alliances) {
        if (alliance.isSolo) continue;
        const hit = { matchId, color, allianceName: alliance.allianceName };

        // Also index the alliance itself by its own name and tag
        // e.g. allianceName = "[BGC] Bag Gangsta Crew" -> name key + tag key
        const aNameOnly = norm(stripTag(alliance.allianceName));
        if (aNameOnly && !byName.has(aNameOnly)) byName.set(aNameOnly, hit);
        const aFull = norm(alliance.allianceName);
        if (aFull && !byName.has(aFull)) byName.set(aFull, hit);
        const aTag = extractTag(alliance.allianceName);
        if (aTag && !byTag.has(aTag)) byTag.set(aTag, hit);

        for (const memberName of (alliance.memberGuilds || [])) {
          if (!memberName) continue;

          // Key 1: name without tag, normalised
          const nameOnly = norm(stripTag(memberName));
          if (nameOnly && !byName.has(nameOnly)) byName.set(nameOnly, hit);

          // Key 2: full string normalised (covers case where there is no tag in memberName)
          const full = norm(memberName);
          if (full && !byName.has(full)) byName.set(full, hit);

          // Key 3: tag (upper) stored separately, used only as fallback
          const tag = extractTag(memberName);
          if (tag && !byTag.has(tag)) byTag.set(tag, hit);
        }
      }
    }
  }

  // Print lookup size to help diagnose
  console.log(`Alliance name lookup: ${byName.size} name keys, ${byTag.size} tag keys`);

  return scheduleGuilds.map(g => {
    // Strategy 1 & 2: match by normalised guild name
    const nameKey = norm(g.guild);
    let hit = byName.get(nameKey);

    // Strategy 3: match by tag (only if tag exists and name didn't match)
    if (!hit && g.tag) {
      hit = byTag.get(g.tag.toUpperCase());
      if (hit) console.log(`  [tag-match] "${g.guild}" [${g.tag}] → ${hit.allianceName} (${hit.color})`);
    }

    if (!hit) {
      console.log(`  [UNKNOWN] "${g.guild}" [${g.tag}] — not found in any alliance`);
    }

    return hit
      ? { ...g, matchId: hit.matchId, color: hit.color, allianceName: hit.allianceName }
      : { ...g, matchId: null, color: null, allianceName: null };
  });
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

  const norm2 = s => s.toLowerCase().replace(/[^a-z0-9]/g, '');
  const soloByTeam = {};
  naIds.forEach(id => { soloByTeam[id] = { red:[], blue:[], green:[] }; });

  soloGuilds.forEach(g => {
    let team = null;
    const apiWorld = naWvWGuilds?.[g.guildId.toUpperCase()]
                  || naWvWGuilds?.[g.guildId.toLowerCase()]
                  || naWvWGuilds?.[g.guildId];
    if (apiWorld) team = worldIdToTeam[String(apiWorld)];

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
        memberGuilds: ['Dragons of Draezor [DD]', 'Core Trinta [CORE]'], worldName: worldNames[String(ddbrWorldId)] || '', isDDBR: true,
      };
      const existing = tierAllianceMap[ddbrTeam.matchId][ddbrTeam.color];
      if (!existing.find(a => a.isDDBR)) existing.unshift(ddbrEntry);
    }
  }

  // 4c. Parse run schedule and resolve teams by guild name lookup in the page data
  const scheduleGuildsRaw = parseRunSchedule(scheduleCsvText);
  console.log(`Schedule guilds parsed: ${scheduleGuildsRaw.length}`);
  // Diagnostic: show first 5 schedule guild names/tags so we can verify format
  console.log('Schedule sample:', scheduleGuildsRaw.slice(0, 5).map(g => `"${g.guild}" [${g.tag}]`).join(' | '));

  // Diagnostic: show first 20 memberGuild entries across all alliances so we can verify format
  const sampleMembers = [];
  for (const colorMap of Object.values(tierAllianceMap)) {
    for (const alliances of Object.values(colorMap)) {
      for (const a of alliances) {
        for (const mg of (a.memberGuilds || [])) {
          sampleMembers.push(mg);
          if (sampleMembers.length >= 20) break;
        }
        if (sampleMembers.length >= 20) break;
      }
      if (sampleMembers.length >= 20) break;
    }
    if (sampleMembers.length >= 20) break;
  }
  console.log('Alliance memberGuilds sample:', sampleMembers.map(s => `"${s}"`).join(' | '));

  // Resolve each schedule guild's team by finding its name inside tierAllianceMap.memberGuilds.
  // No API calls needed — the page already knows which alliance (and therefore which team/color)
  // each guild belongs to. Guilds not found in any alliance go to "unknown".
  const scheduleGuilds = resolveScheduleGuildsByName(scheduleGuildsRaw, tierAllianceMap);

  const resolved   = scheduleGuilds.filter(g => g.matchId);
  const unresolved = scheduleGuilds.filter(g => !g.matchId);
  console.log(`Schedule guilds resolved: ${resolved.length} / unknown: ${unresolved.length}`);
  if (unresolved.length) {
    console.log('Unresolved guilds:', unresolved.map(g => g.guild).join(', '));
  }

  // Build scheduleByMatch: { matchId: { red: [...], blue: [...], green: [...] } }
  // plus a special "unknown" bucket for guilds not found on the page.
  const scheduleByMatch = {};
  naIds.forEach(id => { scheduleByMatch[id] = { red:[], blue:[], green:[] }; });
  scheduleByMatch['unknown'] = { unknown: [] }; // single bucket for all unresolved guilds

  scheduleGuilds.forEach(g => {
    const entry = {
      guild:    g.guild,
      tag:      g.tag,
      schedule: g.schedule,
      contacts: g.contacts,
    };
    if (g.matchId && g.color && scheduleByMatch[g.matchId]) {
      scheduleByMatch[g.matchId][g.color].push(entry);
    } else {
      scheduleByMatch['unknown'].unknown.push(entry);
    }
  });

  // Remove the unknown bucket if it's empty (keep payload clean)
  if (!scheduleByMatch['unknown'].unknown.length) {
    delete scheduleByMatch['unknown'];
  }

  // 5. Current kills/deaths
  const nowKills  = {};
  const nowDeaths = {};
  matches.forEach(m => {
    nowKills[m.id]  = { red: m.kills?.red||0,  blue: m.kills?.blue||0,  green: m.kills?.green||0  };
    nowDeaths[m.id] = { red: m.deaths?.red||0, blue: m.deaths?.blue||0, green: m.deaths?.green||0 };
  });

  // 6. K/D delta against oldest snapshot (~2h)
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

  // 7. Build KD chart series
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
        name = worldNames[String(wids[0])] || '—';
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
