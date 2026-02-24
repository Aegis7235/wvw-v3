// fetch-data.js — runs every 10 min via GitHub Actions
// Fetches GW2 API + alliance spreadsheet → saves to JSONBin

import fetch from 'node-fetch';
import Papa from 'papaparse';

const GW2       = 'https://api.guildwars2.com/v2';
const BIN_ID    = process.env.JSONBIN_BIN_ID;
const BIN_KEY   = process.env.JSONBIN_KEY;
const BIN_URL   = `https://api.jsonbin.io/v3/b/${BIN_ID}`;

const SHEET_URL = 'https://docs.google.com/spreadsheets/d/1Txjpcet-9FDVek6uJ0N3OciwgbpE0cfWozUK7ATfWx4/export?format=csv&gid=1120510750';
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
  if (!r.ok) throw new Error(`GW2 API error: ${path} → ${r.status}`);
  return r.json();
}

// ── Parse CSV (RFC 4180)
function parseCSV(text) {
  const result = Papa.parse(text, { skipEmptyLines: true });
  return result.data;
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
      .map(c => c.trim())
      .filter(c => c && !/^(TRUE|FALSE)$/i.test(c))
      .join(' ').trim();
    if (!worldName) continue;
    const allianceName = cols[0].trim();
    if (!allianceName || /^(TRUE|FALSE|\(+)/.test(allianceName)) continue;
    if (allianceName.toLowerCase().includes('alliance:') ||
        allianceName.toLowerCase().includes('lockout') ||
        (allianceName.toLowerCase().startsWith('alliance') && allianceName.length < 12)) continue;
    const guildsRaw = (cols[2] || '').trim();
    const memberGuilds = guildsRaw
      ? guildsRaw.split(/\r?\n/).map(g => g.trim()).filter(g => g)
      : [];
    alliances.push({ allianceId, allianceName, memberGuilds, worldName });
  }
  return alliances;
}

// ── Load previous snapshot from JSONBin (for K/D delta)
async function loadSnapshot() {
  try {
    const r = await fetch(BIN_URL + '/latest', {
      headers: { 'X-Master-Key': BIN_KEY }
    });
    if (!r.ok) return null;
    const data = await r.json();
    return data.record || null;
  } catch { return null; }
}

// ── Save full payload to JSONBin
async function savePayload(payload) {
  const r = await fetch(BIN_URL, {
    method: 'PUT',
    headers: {
      'Content-Type': 'application/json',
      'X-Master-Key': BIN_KEY,
      'X-Bin-Versioning': 'false',
    },
    body: JSON.stringify(payload),
  });
  if (!r.ok) {
    const t = await r.text();
    throw new Error(`JSONBin save failed: ${r.status} ${t}`);
  }
  console.log('✓ Saved to JSONBin');
}

// ── Main
(async () => {
  console.log('Fetching data…');

  // 1. Load previous snapshot for K/D delta
  const prev = await loadSnapshot();
  const prevTimestamp = prev?.timestamp || 0;
  const prevKills     = prev?.kills     || {};
  const prevDeaths    = prev?.deaths    || {};

  // 2. Fetch in parallel
  const [allMatchIds, worldsRaw, naWvWGuilds, csvText, ddbrGuildData] = await Promise.all([
    gw2('/wvw/matches'),
    gw2('/worlds?ids=all'),
    gw2('/wvw/guilds/na'),
    fetch(SHEET_URL).then(r => r.text()),
    fetch(`${GW2}/guild/${MY_ALLIANCE_ID}`).then(r => r.ok ? r.json() : null).catch(() => null),
  ]);

  const naIds   = allMatchIds.filter(id => id.startsWith('1-')).sort();
  const matches = await Promise.all(naIds.map(id => gw2(`/wvw/matches/${id}`)));

  // 3. World names
  const worldNames = {};
  worldsRaw.forEach(w => { worldNames[String(w.id)] = w.name; });

  // 4. World → team mapping
  const worldIdToTeam = {};
  matches.forEach(m => {
    ['red','blue','green'].forEach(color => {
      (m.all_worlds[color] || []).forEach(wid => {
        worldIdToTeam[String(wid)] = { matchId: m.id, color };
      });
    });
  });

  // 5. Parse alliances
  const allianceData = parseSheet(csvText);
  const norm = s => s.toLowerCase().replace(/[^a-z0-9]/g, '');

  const tierAllianceMap = {};
  naIds.forEach(id => { tierAllianceMap[id] = { red:[], blue:[], green:[] }; });

  allianceData.forEach(a => {
    let team = null;
    if (naWvWGuilds?.[a.allianceId]) {
      const wid = String(naWvWGuilds[a.allianceId]);
      team = worldIdToTeam[wid];
    }
    if (!team && a.worldName) {
      const normTarget = norm(a.worldName);
      const matchedWid = Object.keys(worldNames).find(wid =>
        norm(worldNames[wid]) === normTarget
      );
      if (matchedWid) team = worldIdToTeam[matchedWid];
    }
    if (team && tierAllianceMap[team.matchId]) {
      const wid = naWvWGuilds?.[a.allianceId];
      if (wid && worldNames[String(wid)]) a.worldName = worldNames[String(wid)];
      tierAllianceMap[team.matchId][team.color].push(a);
    }
  });

  // DDBR entry
  const ddbrWorldId = naWvWGuilds?.[MY_ALLIANCE_ID.toUpperCase()]
                   || naWvWGuilds?.[MY_ALLIANCE_ID.toLowerCase()]
                   || naWvWGuilds?.[MY_ALLIANCE_ID];
  if (ddbrWorldId) {
    const ddbrTeam = worldIdToTeam[String(ddbrWorldId)];
    if (ddbrTeam && tierAllianceMap[ddbrTeam.matchId]) {
      const ddbrName = ddbrGuildData ? `[DDBR] ${ddbrGuildData.name}` : '[DDBR]';
      const ddbrEntry = {
        allianceId: MY_ALLIANCE_ID,
        allianceName: ddbrName,
        memberGuilds: [],
        worldName: worldNames[String(ddbrWorldId)] || '',
        isDDBR: true,
      };
      const existing = tierAllianceMap[ddbrTeam.matchId][ddbrTeam.color];
      if (!existing.find(a => a.isDDBR)) existing.unshift(ddbrEntry);
    }
  }

  // 6. Build kills/deaths maps for snapshot
  const nowKills  = {};
  const nowDeaths = {};
  matches.forEach(m => {
    nowKills[m.id]  = { red: m.kills?.red||0,  blue: m.kills?.blue||0,  green: m.kills?.green||0  };
    nowDeaths[m.id] = { red: m.deaths?.red||0, blue: m.deaths?.blue||0, green: m.deaths?.green||0 };
  });

  // 7. Compute K/D delta per match/color
  const now = Date.now();
  const minutes = prevTimestamp ? Math.round((now - prevTimestamp) / 60000) : 0;
  const kdDelta = {};

  if (minutes >= 10) {
    matches.forEach(m => {
      kdDelta[m.id] = {};
      ['red','blue','green'].forEach(color => {
        const kills  = Math.max(0, (nowKills[m.id][color]  || 0) - (prevKills[m.id]?.[color]  || 0));
        const deaths = Math.max(0, (nowDeaths[m.id][color] || 0) - (prevDeaths[m.id]?.[color] || 0));
        kdDelta[m.id][color] = { kills, deaths, minutes };
      });
    });
  }

  // 8. Pre-resolve primary world name per match/color (so HTML doesn't need naWvWGuilds)
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

  // 9. Slim down match data
  const matchesSlim = matches.map(m => ({
    id:             m.id,
    start_time:     m.start_time,
    end_time:       m.end_time,
    scores:         m.scores,
    kills:          m.kills,
    deaths:         m.deaths,
    victory_points: m.victory_points,
  }));

  // 10. Save to JSONBin — lean payload only
  const payload = {
    timestamp:       now,
    kills:           nowKills,
    deaths:          nowDeaths,
    kdDelta,
    matches:         matchesSlim,
    tierAllianceMap,
    primaryWorlds,
  };

  await savePayload(payload);
  console.log(`✓ Done. ${naIds.length} matches, ${allianceData.length} alliances, delta ${minutes}min`);
})();
