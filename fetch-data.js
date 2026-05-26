// fetch-data.js — runs every 4h via GitHub Actions
// Storage: GitHub Gist (free, no request limits)
// Data: WvW matchups, alliances, solo guilds, run schedule only (no K/D)
import fetch from 'node-fetch';
import Papa from 'papaparse';

const GW2           = 'https://api.guildwars2.com/v2';
const GIST_TOKEN    = process.env.GIST_TOKEN;
const GIST_ID       = process.env.GIST_ID;
const GIST_FILENAME = 'wvw-data.json';
const GIST_URL      = `https://api.github.com/gists/${GIST_ID}`;

const SHEET_URL          = 'https://docs.google.com/spreadsheets/d/1Txjpcet-9FDVek6uJ0N3OciwgbpE0cfWozUK7ATfWx4/export?format=csv&gid=1120510750';
const SOLO_SHEET_URL     = 'https://docs.google.com/spreadsheets/d/1Txjpcet-9FDVek6uJ0N3OciwgbpE0cfWozUK7ATfWx4/export?format=csv&gid=768688698';
const SCHEDULE_SHEET_URL = 'https://docs.google.com/spreadsheets/d/15mLFZNS_DriY4OVFvdCsu_tlo8E8G89WMbFR6PI4zXM/export?format=csv';
const MY_ALLIANCE_ID     = '4F2CA889-AA1F-EF11-81AB-F50023EE1BF3';

async function gw2(path) {
  const r = await fetch(GW2 + path);
  if (!r.ok) throw new Error(`GW2 API error: ${path} -> ${r.status}`);
  return r.json();
}

function parseCSV(text) {
  return Papa.parse(text, { skipEmptyLines: true }).data;
}

// Parses the Alliances sheet
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

// Parses the Solo Guilds sheet
function parseSoloGuilds(csvText) {
  const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
  const rows = Papa.parse(csvText, { skipEmptyLines: true }).data;
  const guilds = [];
  for (const cols of rows) {
    if (!cols || cols.length < 20) continue;
    const guildName = cols[0].trim();
    if (!guildName || /^(solo guilds|𝐒𝐨𝐥𝐨)/i.test(guildName)) continue;
    if (/^(TRUE|FALSE)$/i.test(guildName)) continue;
    const guildId = (cols[18] || '').trim();
    if (!UUID_RE.test(guildId)) continue;
    const apiMismatch = (cols[19] || '').trim().toUpperCase();
    if (apiMismatch === 'TRUE') continue;
    const worldName = (cols[21] || '').trim();
    if (!worldName) continue;
    guilds.push({ guildName, guildId: guildId.toUpperCase(), worldName });
  }
  return guilds;
}

// Parses the Run Schedule sheet
function parseRunSchedule(csvText) {
  const DAYS_EN = ['Sun','Mon','Tue','Wed','Thu','Fri','Sat'];
  const TIME_RE = /^\d{2}:\d{2}\s*-\s*\d{2}:\d{2}$/;

  function toBRT(t) {
    const m = t.match(/^(\d{2}):(\d{2})\s*-\s*(\d{2}):(\d{2})$/);
    if (!m) return t;
    return `${(+m[1]-3+24)%24}`.padStart(2,'0') + ':' + m[2] + ' - ' +
           `${(+m[3]-3+24)%24}`.padStart(2,'0') + ':' + m[4];
  }

  const rows = Papa.parse(csvText, { skipEmptyLines: true }).data;
  const guilds = [];
  for (let i = 1; i < rows.length; i++) {
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
    }
    guilds.push({ guild: name, tag, schedule, contacts: [] });
  }
  return guilds;
}

const norm = s => s.toLowerCase().replace(/[^a-z0-9]/g, '');
const extractTag = s => { const m = s.match(/\[(.+?)\]/); return m ? m[1].trim().toUpperCase() : ''; };
const stripTag   = s => s.replace(/\s*\[.+?\]\s*/, '').trim();

// Resolve schedule guilds' match/color by looking up memberGuilds in tierAllianceMap
function resolveScheduleGuildsByName(scheduleGuilds, tierAllianceMap) {
  const byName = new Map();
  const byTag  = new Map();

  for (const [matchId, colorMap] of Object.entries(tierAllianceMap)) {
    for (const [color, alliances] of Object.entries(colorMap)) {
      for (const alliance of alliances) {
        if (alliance.isSolo) continue;
        const hit = { matchId, color, allianceName: alliance.allianceName };

        const aNameOnly = norm(stripTag(alliance.allianceName));
        if (aNameOnly && !byName.has(aNameOnly)) byName.set(aNameOnly, hit);
        const aFull = norm(alliance.allianceName);
        if (aFull && !byName.has(aFull)) byName.set(aFull, hit);
        const aTag = extractTag(alliance.allianceName);
        if (aTag && !byTag.has(aTag)) byTag.set(aTag, hit);

        for (const memberName of (alliance.memberGuilds || [])) {
          if (!memberName) continue;
          const nameOnly = norm(stripTag(memberName));
          if (nameOnly && !byName.has(nameOnly)) byName.set(nameOnly, hit);
          const full = norm(memberName);
          if (full && !byName.has(full)) byName.set(full, hit);
          const tag = extractTag(memberName);
          if (tag && !byTag.has(tag)) byTag.set(tag, hit);
        }
      }
    }
  }

  console.log(`Alliance name lookup: ${byName.size} name keys, ${byTag.size} tag keys`);

  return scheduleGuilds.map(g => {
    const nameKey = norm(g.guild);
    let hit = byName.get(nameKey);
    if (!hit && g.tag) {
      hit = byTag.get(g.tag.toUpperCase());
      if (hit) console.log(`  [tag-match] "${g.guild}" [${g.tag}] → ${hit.allianceName} (${hit.color})`);
    }
    if (!hit) console.log(`  [UNKNOWN] "${g.guild}" [${g.tag}]`);
    return hit
      ? { ...g, matchId: hit.matchId, color: hit.color, allianceName: hit.allianceName }
      : { ...g, matchId: null, color: null, allianceName: null };
  });
}

// Save data to GitHub Gist
async function gistPut(data, retries = 3) {
  const body = JSON.stringify({
    description: 'WvW matchup data',
    files: {
      [GIST_FILENAME]: { content: JSON.stringify(data) }
    }
  });
  console.log(`PATCH Gist ${GIST_ID} (${body.length} bytes)`);
  for (let attempt = 1; attempt <= retries; attempt++) {
    const r = await fetch(GIST_URL, {
      method: 'PATCH',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${GIST_TOKEN}`,
        'X-GitHub-Api-Version': '2022-11-28',
      },
      body,
    });
    if (r.ok) { console.log('Gist updated OK'); return; }
    const t = await r.text();
    console.error(`Gist PATCH attempt ${attempt}/${retries} failed: ${r.status} ${t}`);
    if (attempt < retries) await new Promise(res => setTimeout(res, 2000 * attempt));
    else throw new Error(`Gist PATCH failed after ${retries} attempts: ${r.status} ${t}`);
  }
}

(async () => {
  console.log('Fetching WvW data...');
  const now = Date.now();

  // Fetch all data in parallel
  const [allMatchIds, worldsRaw, naWvWGuilds, csvText, soloCsvText, scheduleCsvText] = await Promise.all([
    gw2('/wvw/matches'),
    gw2('/worlds?ids=all'),
    gw2('/wvw/guilds/na'),
    fetch(SHEET_URL).then(r => r.text()),
    fetch(SOLO_SHEET_URL).then(r => r.text()),
    fetch(SCHEDULE_SHEET_URL).then(r => r.text()),
  ]);

  const naIds   = allMatchIds.filter(id => id.startsWith('1-')).sort();
  const matches = await Promise.all(naIds.map(id => gw2(`/wvw/matches/${id}`)));

  // World names + team mapping
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

  // Parse alliances
  const allianceData = parseSheet(csvText);
  const tierAllianceMap = {};
  naIds.forEach(id => { tierAllianceMap[id] = { red:[], blue:[], green:[] }; });

  allianceData.forEach(a => {
    if (a.allianceId.toUpperCase() === MY_ALLIANCE_ID.toUpperCase()) a.isDDBR = true;
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

  // Parse and place Solo Guilds
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

  // Parse run schedule and resolve teams
  const scheduleGuildsRaw = parseRunSchedule(scheduleCsvText);
  console.log(`Schedule guilds parsed: ${scheduleGuildsRaw.length}`);

  const scheduleGuilds = resolveScheduleGuildsByName(scheduleGuildsRaw, tierAllianceMap);

  const resolved   = scheduleGuilds.filter(g => g.matchId);
  const unresolved = scheduleGuilds.filter(g => !g.matchId);
  console.log(`Schedule guilds resolved: ${resolved.length} / unknown: ${unresolved.length}`);
  if (unresolved.length) {
    console.log('Unresolved guilds:', unresolved.map(g => g.guild).join(', '));
  }

  const scheduleByMatch = {};
  naIds.forEach(id => { scheduleByMatch[id] = { red:[], blue:[], green:[] }; });
  scheduleByMatch['unknown'] = { unknown: [] };

  scheduleGuilds.forEach(g => {
    const entry = { guild: g.guild, tag: g.tag, schedule: g.schedule, contacts: g.contacts };
    if (g.matchId && g.color && scheduleByMatch[g.matchId]) {
      scheduleByMatch[g.matchId][g.color].push(entry);
    } else {
      scheduleByMatch['unknown'].unknown.push(entry);
    }
  });

  if (!scheduleByMatch['unknown'].unknown.length) delete scheduleByMatch['unknown'];

  // Pre-resolve primary world names
  const primaryWorlds = {};
  matches.forEach(m => {
    primaryWorlds[m.id] = {};
    ['red','blue','green'].forEach(color => {
      const wids = m.worlds[color] ? [m.worlds[color]] : (m.all_worlds[color] || []);
      primaryWorlds[m.id][color] = worldNames[String(wids[0])] || '—';
    });
  });

  // Save to Gist
  await gistPut({
    timestamp: now,
    matches: matches.map(m => ({
      id: m.id, start_time: m.start_time, end_time: m.end_time,
      scores: m.scores, victory_points: m.victory_points,
    })),
    tierAllianceMap,
    primaryWorlds,
    scheduleByMatch,
  });

  console.log(`Done. ${naIds.length} matches, ${allianceData.length} alliances, ${soloGuilds.length} solo guilds.`);
})();
