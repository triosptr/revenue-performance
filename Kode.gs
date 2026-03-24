/********************************************************
 * ========== KONFIGURASI ==========
 ********************************************************/
const GOOGLE_TARGET = "DATABASE_GOOGLE";
const GOOGLE_SOURCE_LINK_SHEET = "SOURCE_LINK";

const META_TARGET = "DATABASE_METAADS";
const META_SOURCE_SHEET = "Data API MetaAds"; // A=MetaAccount, B=API, C=Secret, D=AppID

const DEFAULT_SINCE = "2026-02-01";
const META_CURSOR_PROPERTY_KEY = "META_ACCOUNT_CURSOR_INDEX";
const AUTO_PROCESS_ACTIVE_KEY = "AUTO_PROCESS_ACTIVE";
const AUTO_PROCESS_TYPE_KEY = "AUTO_PROCESS_TYPE";
const META_AUTO_PROCESSED_KEY = "META_AUTO_PROCESSED_COUNT";
const META_AUTO_TOTAL_KEY = "META_AUTO_TOTAL_COUNT";
const MAX_EXECUTION_TIME = 300000; // 5 minutes in milliseconds


/********************************************************
 * MENU
 ********************************************************/
function onOpen() {
  const ui = SpreadsheetApp.getUi();

  ui.createMenu("Add New Report")
    .addItem("Generate Reports", "generateReportsFromList")
    .addToUi();

  ui.createMenu("Google Data")
    .addItem("Update Google Earning", "updateAllGoogleAuto")
    .addToUi();
}


/********************************************************
 * ========== GENERATE NEW REPORT ==========
 * Ambil nama sheet baru dari:
 * - Sheet: report generate
 * - Kolom: A2:A
 *
 * Cara kerja:
 * - Copy penuh sheet "Template Report"
 * - Rename sesuai nama di kolom A
 * - Isi A1 dengan nama sheet
 ********************************************************/
function generateReportsFromList() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const ui = SpreadsheetApp.getUi();

  const TEMPLATE_SHEET_NAME = "Template Report";
  const SOURCE_SHEET_NAME = "report generate";
  const START_ROW = 2;

  const templateSheet = ss.getSheetByName(TEMPLATE_SHEET_NAME);
  const sourceSheet = ss.getSheetByName(SOURCE_SHEET_NAME);

  if (!templateSheet) {
    ui.alert('Sheet template "' + TEMPLATE_SHEET_NAME + '" tidak ditemukan.');
    return;
  }

  if (!sourceSheet) {
    ui.alert('Sheet sumber "' + SOURCE_SHEET_NAME + '" tidak ditemukan.');
    return;
  }

  const lastRow = sourceSheet.getLastRow();
  if (lastRow < START_ROW) {
    ui.alert('Tidak ada data nama sheet di "' + SOURCE_SHEET_NAME + '" kolom A.');
    return;
  }

  const names = sourceSheet.getRange(START_ROW, 1, lastRow - START_ROW + 1, 1).getValues();

  let createdCount = 0;
  let skippedCount = 0;
  const logMessages = [];

  names.forEach((row, index) => {
    const rowNumber = START_ROW + index;
    const newSheetName = String(row[0] || "").trim();

    if (!newSheetName) {
      skippedCount++;
      logMessages.push("Row " + rowNumber + ": dilewati karena nama sheet kosong.");
      return;
    }

    if (ss.getSheetByName(newSheetName)) {
      skippedCount++;
      logMessages.push('Row ' + rowNumber + ': sheet "' + newSheetName + '" sudah ada, dilewati.');
      return;
    }

    try {
      const newSheet = templateSheet.copyTo(ss).setName(newSheetName);

      ss.setActiveSheet(newSheet);
      ss.moveActiveSheet(ss.getSheets().length);

      newSheet.getRange("A1").setValue(newSheetName);

      SpreadsheetApp.flush();

      createdCount++;
      logMessages.push('Row ' + rowNumber + ': sheet "' + newSheetName + '" berhasil dibuat.');
    } catch (error) {
      skippedCount++;
      logMessages.push('Row ' + rowNumber + ': gagal membuat sheet "' + newSheetName + '" -> ' + error.message);
    }
  });

  ui.alert(
    "Proses selesai.\n\n" +
    "Berhasil dibuat: " + createdCount + "\n" +
    "Dilewati / gagal: " + skippedCount + "\n\n" +
    logMessages.join("\n")
  );
}


/********************************************************
 * ========== GOOGLE UPDATE (FAST DATABASE_GOOGLE) ==========
 * Output DATABASE_GOOGLE:
 * A Site
 * B Date
 * C Clicks
 * D CPC
 * E Earning
 * F Source(Account)
 *
 * NOTE:
 * - Kolom G TIDAK disentuh karena berisi formula
 *
 * Logic:
 * - Jika DATABASE_GOOGLE kosong: ambil dari DEFAULT_SINCE s/d hari ini
 * - Jika sudah ada data: ambil 2 hari kebelakang s/d hari ini
 * - Pakai model FAST:
 *   baca target sekali, filter di memori, lalu tulis ulang
 ********************************************************/
function updateGoogleEarningData() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const targetSheet = ss.getSheetByName(GOOGLE_TARGET);
  const logSheet = getOrCreateGoogleLogSheet_();

  if (!targetSheet) {
    appendGoogleLog_(logSheet, "SYSTEM", "error", "ERROR", "Sheet target tidak ditemukan: " + GOOGLE_TARGET);
    throw new Error("Sheet target tidak ditemukan: " + GOOGLE_TARGET);
  }

  ensureGoogleTargetHeader_(targetSheet);

  const sources = getGoogleSourceLinks_();
  if (sources.length === 0) {
    appendGoogleLog_(logSheet, "SYSTEM", "error", "ERROR", "Tidak ada link valid di SOURCE_LINK kolom B2:B.");
    throw new Error("Tidak ada link valid di SOURCE_LINK kolom B2:B.");
  }

  appendGoogleLog_(logSheet, "SYSTEM", "start", "INFO", `Memulai update Google data dari ${sources.length} sumber`);

  const range = getGoogleFetchRange_(targetSheet);
  appendGoogleLog_(logSheet, "SYSTEM", "range", "INFO", `Periode: ${range.since} s/d ${range.until}`);

  const pulled = fetchGoogleRowsByRange_(sources, range.since, range.until);

  if (pulled.length === 0) {
    appendGoogleLog_(logSheet, "SYSTEM", "skip", "INFO", "Tidak ada data yang masuk dari semua sumber");
    return;
  }

  appendGoogleLog_(logSheet, "SYSTEM", "process", "INFO", `Memproses ${pulled.length} rows...`);

  const deduped = dedupeGoogleRowsKeepLast_(pulled);
  const result = overwriteGoogleRowsByRangeFast_(targetSheet, deduped, range.since, range.until);

  appendGoogleLog_(logSheet, "SYSTEM", "complete", "SUCCESS",
    `Update Google selesai. Rows baru: ${deduped.length}, Keep lama: ${result.keptOldRows}, Buang range lama: ${result.removedOldRows}`
  );
}

/**
 * One-click function to update all Google sources automatically
 */
function updateAllGoogleAuto() {
  const logSheet = getOrCreateGoogleLogSheet_();
  const sources = getGoogleSourceLinks_();

  if (sources.length === 0) {
    appendGoogleLog_(logSheet, "SYSTEM", "auto_start", "ERROR", "Tidak ada sumber Google yang tersedia.");
    return;
  }

  setAutoProcessActive_(true, "google");

  appendGoogleLog_(logSheet, "SYSTEM", "auto_start", "INFO",
    `🚀 Memulai proses otomatis untuk ${sources.length} sumber Google`
  );

  processAllGoogleSources_();
}

/**
 * Process all Google sources
 */
function processAllGoogleSources_() {
  if (!isAutoProcessActive_()) {
    return;
  }

  try {
    updateGoogleEarningData();
    completeGoogleAutoProcess_();
  } catch (error) {
    handleGoogleAutoProcessError_(error);
  }
}

function completeGoogleAutoProcess_() {
  setAutoProcessActive_(false);
  deleteTriggers_();

  const logSheet = getOrCreateGoogleLogSheet_();
  appendGoogleLog_(logSheet, "SYSTEM", "complete", "SUCCESS", "🎉 Semua sumber Google berhasil diproses!");
}

function handleGoogleAutoProcessError_(error) {
  const logSheet = getOrCreateGoogleLogSheet_();
  appendGoogleLog_(logSheet, "SYSTEM", "error", "ERROR", `Proses otomatis Google error: ${error.message}`);

  setAutoProcessActive_(false);
  deleteTriggers_();
}

/**
 * GOOGLE RANGE:
 * - Jika DATABASE_GOOGLE kosong -> full dari DEFAULT_SINCE
 * - Jika sudah ada data -> hanya H-2 sampai hari ini
 */
function getGoogleFetchRange_(targetSheet) {
  const tz = Session.getScriptTimeZone();
  const today = new Date();
  const until = Utilities.formatDate(today, tz, "yyyy-MM-dd");

  const lastRow = targetSheet.getLastRow();

  if (lastRow < 2) {
    return { since: DEFAULT_SINCE, until };
  }

  const sinceDate = new Date();
  sinceDate.setDate(sinceDate.getDate() - 2);

  const minDate = new Date(DEFAULT_SINCE + "T00:00:00");
  const effective = sinceDate < minDate ? minDate : sinceDate;

  return {
    since: Utilities.formatDate(effective, tz, "yyyy-MM-dd"),
    until
  };
}

function fetchGoogleRowsByRange_(sources, since, until) {
  let out = [];
  const sinceMs = new Date(since + "T00:00:00").getTime();
  const untilMs = new Date(until + "T23:59:59").getTime();

  const sourceBatches = batchArray_(sources, 5);

  sourceBatches.forEach(batch => {
    const batchResults = processGoogleSourceBatch_(batch, sinceMs, untilMs);
    out = out.concat(batchResults);
  });

  return out;
}

/**
 * Process a batch of Google sources more efficiently
 */
function processGoogleSourceBatch_(sources, sinceMs, untilMs) {
  const results = [];

  sources.forEach(src => {
    try {
      const sourceSS = SpreadsheetApp.openById(src.id);
      const sourceAccount = src.account || "";
      const sheets = sourceSS.getSheets();

      sheets.forEach(sh => {
        const values = sh.getDataRange().getValues();
        if (!values || values.length < 2) return;

        const headers = values[0].map(h => normalizeHeader_(h));

        const siteCol = findColumn(headers, [
          "subdomain", "domain", "site", "hostname", "host name", "website", "url"
        ]);

        const dateCol = findColumn(headers, ["date", "tanggal", "day"]);
        const clicksCol = findColumn(headers, ["click", "clicks", "klik"]);
        const cpcCol = findColumn(headers, ["cpc", "cost per click", "cost_per_click", "cpc rp", "cpc (rp)"]);
        const earningCol = findColumn(headers, [
          "earning", "earnings", "revenue",
          "estimated earnings", "estimated revenue",
          "estimated_earnings", "estimated_revenue",
          "pendapatan"
        ]);

        if (dateCol === -1 || clicksCol === -1 || cpcCol === -1 || earningCol === -1) return;

        const fallbackSite = normalizeSite_(sh.getName());

        for (let i = 1; i < values.length; i++) {
          const row = values[i];

          const siteRaw = (siteCol === -1) ? fallbackSite : row[siteCol];
          const site = normalizeSite_(siteRaw);
          const dateVal = row[dateCol];

          if (!isNonEmpty_(site) || !isNonEmpty_(dateVal)) continue;

          const d = new Date(dateVal);
          if (isNaN(d.getTime())) continue;

          const ms = d.getTime();
          if (ms < sinceMs || ms > untilMs) continue;

          const clicks = row[clicksCol];
          const cpc = row[cpcCol];
          const earning = row[earningCol];

          if (!isNumberOrZero_(clicks) || !isNumberOrZero_(cpc) || !isNumberOrZero_(earning)) continue;

          results.push([
            site,
            Utilities.formatDate(d, Session.getScriptTimeZone(), "yyyy-MM-dd"),
            Number(String(clicks).replace(/[^\d.-]/g, "")),
            Number(String(cpc).replace(/[^\d.-]/g, "")),
            Number(String(earning).replace(/[^\d.-]/g, "")),
            sourceAccount
          ]);
        }
      });
    } catch (error) {
      console.error(`Error processing Google source ${src.id}: ${error.message}`);
    }
  });

  return results;
}

function dedupeGoogleRowsKeepLast_(rows) {
  const map = {};
  rows.forEach(r => {
    const key = buildGoogleRowKey_(r[0], r[1], r[5]);
    if (key) map[key] = r;
  });
  return Object.keys(map).map(k => map[k]);
}

function buildGoogleRowKey_(site, dateVal, sourceAccount) {
  const s = String(site || "").trim().toLowerCase();
  const d = normalizeDateKey_(dateVal);
  const a = String(sourceAccount || "").trim().toLowerCase();

  if (!s || !d) return "";
  return s + "|" + d + "|" + a;
}

function ensureGoogleTargetHeader_(sheet) {
  const headers = ["Site", "Date", "Clicks", "CPC", "Earning", "Source(Account)"];
  sheet.getRange(1, 1, 1, headers.length).setValues([headers]);
}

/**
 * FAST overwrite untuk DATABASE_GOOGLE:
 * - baca seluruh data lama sekali dari A:F
 * - buang row dalam range tanggal target di memori
 * - gabungkan dengan row baru
 * - clear isi A:F saja
 * - tulis ulang ke A:F saja
 * - kolom G tetap aman
 */
function overwriteGoogleRowsByRangeFast_(sheet, newRows, since, until) {
  ensureGoogleTargetHeader_(sheet);

  const lastRow = sheet.getLastRow();
  const oldRows = lastRow > 1
    ? sheet.getRange(2, 1, lastRow - 1, 6).getValues()
    : [];

  const sinceMs = new Date(since + "T00:00:00").getTime();
  const untilMs = new Date(until + "T23:59:59").getTime();

  const keptRows = [];
  let removedOldRows = 0;

  for (let i = 0; i < oldRows.length; i++) {
    const row = oldRows[i];
    const d = new Date(row[1]);
    if (isNaN(d.getTime())) {
      keptRows.push(row);
      continue;
    }

    const ms = d.getTime();
    if (ms >= sinceMs && ms <= untilMs) {
      removedOldRows++;
    } else {
      keptRows.push(row);
    }
  }

  const finalRows = keptRows.concat(newRows);
  finalRows.sort(compareGoogleRows_);

  clearGoogleDataSheet_(sheet);

  if (finalRows.length > 0) {
    sheet.getRange(2, 1, finalRows.length, 6).setValues(finalRows);
  }

  return {
    keptOldRows: keptRows.length,
    removedOldRows: removedOldRows,
    addedNewRows: newRows.length
  };
}

/**
 * Bersihkan isi DATABASE_GOOGLE tanpa menghapus header
 * HANYA A:F
 * Kolom G tidak disentuh
 */
function clearGoogleDataSheet_(sheet) {
  const lastRow = sheet.getLastRow();

  if (lastRow > 1) {
    sheet.getRange(2, 1, lastRow - 1, 6).clearContent();
  }

  ensureGoogleTargetHeader_(sheet);
}

/**
 * Comparator untuk sort data Google:
 * 1. Date
 * 2. Site
 * 3. Source(Account)
 */
function compareGoogleRows_(a, b) {
  const da = normalizeDateKey_(a[1]);
  const db = normalizeDateKey_(b[1]);

  if (da < db) return -1;
  if (da > db) return 1;

  const sa = String(a[0] || "").toLowerCase();
  const sb = String(b[0] || "").toLowerCase();
  if (sa < sb) return -1;
  if (sa > sb) return 1;

  const aa = String(a[5] || "").toLowerCase();
  const ab = String(b[5] || "").toLowerCase();
  if (aa < ab) return -1;
  if (aa > ab) return 1;

  return 0;
}

function getGoogleSourceLinks_() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sh = ss.getSheetByName(GOOGLE_SOURCE_LINK_SHEET);

  if (!sh) throw new Error("Sheet tidak ditemukan: " + GOOGLE_SOURCE_LINK_SHEET);

  const last = sh.getLastRow();
  if (last < 2) return [];

  const rows = sh.getRange(2, 1, last - 1, 2).getValues();
  const sources = [];

  rows.forEach(r => {
    const account = r[0] ? String(r[0]).trim() : "";
    const linkOrId = r[1] ? String(r[1]).trim() : "";
    if (!linkOrId) return;

    const id = extractSpreadsheetId_(linkOrId);
    if (!id) return;

    sources.push({ id, account });
  });

  return sources;
}

function extractSpreadsheetId_(input) {
  const s = String(input || "").trim();

  if (/^[a-zA-Z0-9-_]{20,}$/.test(s) && !s.includes("http")) return s;

  const m = s.match(/\/spreadsheets\/d\/([a-zA-Z0-9-_]+)/);
  if (m && m[1]) return m[1];

  const m2 = s.match(/([a-zA-Z0-9-_]{20,})/);
  return (m2 && m2[1]) ? m2[1] : "";
}


/********************************************************
 * ========== META ADS UPDATE 1 ACCOUNT PER RUN ==========
 * Logic:
 * - proses 1 source account per sekali jalan
 * - jika account belum ada data -> tarik dari DEFAULT_SINCE s/d hari ini
 * - jika account sudah ada data -> tarik 3 hari kebelakang s/d hari ini
 * - update baris lama + append baris baru
 * - tetap terkumpul di 1 sheet DATABASE_METAADS
 ********************************************************/
function updateMetaAdsData() {
  updateAllMetaAdsAuto();
}

function updateMetaAdsDataSingle_() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const targetSheet = ss.getSheetByName(META_TARGET);
  const logSheet = getOrCreateMetaLogSheet_();

  if (!targetSheet) {
    appendMetaLog_(logSheet, "SYSTEM", "error", "ERROR", "Sheet target tidak ditemukan: " + META_TARGET);
    throw new Error("Sheet target tidak ditemukan: " + META_TARGET);
  }

  const sources = getMetaApiSources_();
  if (sources.length === 0) {
    appendMetaLog_(logSheet, "SYSTEM", "error", "ERROR", "Tidak ada akun valid di sheet: " + META_SOURCE_SHEET);
    throw new Error("Tidak ada akun valid di sheet: " + META_SOURCE_SHEET);
  }

  ensureMetaTargetHeader_(targetSheet);

  const cursor = getMetaCursorIndex_();
  const safeCursor = cursor >= sources.length ? 0 : cursor;
  const src = sources[safeCursor];
  const progressInfo = getProgressInfo_();

  try {
    appendMetaLog_(logSheet, src.accountName, "start", "INFO",
      `Memulai proses untuk akun: ${src.accountName} [${progressInfo.current}/${progressInfo.total}]`,
      progressInfo.current, progressInfo.total
    );

    const range = getMetaFetchRangeForAccount_(targetSheet, src.accountName);
    appendMetaLog_(logSheet, src.accountName, "range", "INFO",
      `Mengambil data dari ${range.since} s/d ${range.until}`,
      progressInfo.current, progressInfo.total
    );

    const pulled = fetchMetaAdsRowsForSingleSource_(src, range.since, range.until);

    if (pulled.length === 0) {
      appendMetaLog_(logSheet, src.accountName, "skip", "INFO",
        `Tidak ada data untuk akun ${src.accountName}, skip ke akun berikutnya`,
        progressInfo.current, progressInfo.total
      );

      const nextCursor = (safeCursor + 1) % sources.length;
      setMetaCursorIndex_(nextCursor);

      if (progressInfo.remaining > 0) {
        appendMetaLog_(logSheet, sources[nextCursor].accountName, "next", "INFO",
          `Beralih ke akun berikutnya: ${sources[nextCursor].accountName}`,
          progressInfo.current + 1, progressInfo.total
        );
      } else {
        appendMetaLog_(logSheet, "SYSTEM", "complete", "SUCCESS",
          "Semua akun telah diproses", progressInfo.total, progressInfo.total
        );
      }
      return;
    }

    appendMetaLog_(logSheet, src.accountName, "process", "INFO",
      `Memproses ${pulled.length} rows...`,
      progressInfo.current, progressInfo.total
    );

    const result = mergeMetaAdsIntoSheet_(targetSheet, pulled);

    const nextCursor = (safeCursor + 1) % sources.length;
    setMetaCursorIndex_(nextCursor);

    appendMetaLog_(logSheet, src.accountName, "complete", "SUCCESS",
      `Update selesai. Rows: ${pulled.length}, Baru: ${result.added}, Update: ${result.updated}. ` +
      `Sisa ${progressInfo.remaining} akun lagi`,
      progressInfo.current, progressInfo.total
    );

  } catch (error) {
    appendMetaLog_(logSheet, src.accountName, "error", "ERROR",
      `Error: ${error.message}`, progressInfo.current, progressInfo.total
    );
    throw error;
  }
}

/**
 * One-click function to update all Meta Ads accounts automatically
 * This will continue processing until all accounts are done
 */
function updateAllMetaAdsAuto() {
  const logSheet = getOrCreateMetaLogSheet_();
  const sources = getMetaApiSources_();

  if (sources.length === 0) {
    appendMetaLog_(logSheet, "SYSTEM", "auto_start", "ERROR", "Tidak ada akun Meta Ads yang tersedia.");
    return;
  }

  setMetaCursorIndex_(0);
  setMetaAutoState_(0, sources.length);
  setAutoProcessActive_(true, "meta");

  appendMetaLog_(logSheet, "SYSTEM", "auto_start", "INFO",
    `🚀 Memulai proses otomatis untuk ${sources.length} akun Meta Ads`
  );

  processNextMetaAccount_();
}

/**
 * Process next Meta account with auto-continue
 */
function processNextMetaAccount_() {
  if (!isAutoProcessActive_()) {
    return;
  }

  const startedAt = new Date().getTime();
  const state = getMetaAutoState_();
  let processed = state.processed;
  const total = state.total;
  const logSheet = getOrCreateMetaLogSheet_();

  try {
    while (processed < total && isAutoProcessActive_()) {
      const elapsed = new Date().getTime() - startedAt;
      if (elapsed > MAX_EXECUTION_TIME - 15000) {
        appendMetaLog_(logSheet, "SYSTEM", "schedule", "INFO", `Menjadwalkan lanjutan. Progress ${processed}/${total}`);
        setMetaAutoState_(processed, total);
        scheduleNextRun_();
        return;
      }

      updateMetaAdsDataSingle_();
      processed++;
      setMetaAutoState_(processed, total);

      if (processed < total) {
        Utilities.sleep(1000);
      }
    }

    if (processed >= total) {
      completeAutoProcess_();
    }
  } catch (error) {
    handleAutoProcessError_(error);
  }
}

function resetMetaAdsQueue() {
  setMetaCursorIndex_(0);
  const logSheet = getOrCreateMetaLogSheet_();
  appendMetaLog_(logSheet, "SYSTEM", "reset", "INFO", "Queue MetaAds direset ke akun pertama.");
  SpreadsheetApp.getUi().alert("Queue MetaAds direset ke akun pertama.");
}

/**
 * Show current progress status
 */
function showProgressStatus() {
  const progressInfo = getProgressInfo_();
  const autoState = getMetaAutoState_();
  const logSheet = getOrCreateMetaLogSheet_();

  const lastRow = logSheet.getLastRow();
  const lastEntries = lastRow > 1 ? logSheet.getRange(Math.max(2, lastRow - 4), 1, Math.min(5, lastRow - 1), 7).getValues() : [];

  let statusMessage = `📊 STATUS PROGRESS META ADS\n\n`;
  statusMessage += `Progress: ${progressInfo.current}/${progressInfo.total} akun\n`;
  statusMessage += `Akun saat ini: ${progressInfo.currentAccount}\n`;
  statusMessage += `Sisa: ${progressInfo.remaining} akun\n\n`;

  const isActive = isAutoProcessActive_();
  statusMessage += `Status Auto: ${isActive ? '🟢 AKTIF' : '🔴 NONAKTIF'}\n\n`;
  statusMessage += `Auto Progress: ${autoState.processed}/${autoState.total}\n\n`;

  if (lastEntries.length > 0) {
    statusMessage += `📋 5 Log Terakhir:\n`;
    lastEntries.reverse().forEach(entry => {
      const timestamp = entry[0];
      const account = entry[1];
      const step = entry[2];
      const status = entry[3];
      const message = entry[4];
      statusMessage += `• [${timestamp}] ${account}: ${step} (${status}) - ${message}\n`;
    });
  }

  SpreadsheetApp.getUi().alert(statusMessage);
}

/**
 * Auto process management functions
 */
function setAutoProcessActive_(active, type) {
  const props = PropertiesService.getDocumentProperties();
  props.setProperty(AUTO_PROCESS_ACTIVE_KEY, active ? "true" : "false");
  if (type) {
    props.setProperty(AUTO_PROCESS_TYPE_KEY, type);
  }
}

function isAutoProcessActive_() {
  const props = PropertiesService.getDocumentProperties();
  const active = props.getProperty(AUTO_PROCESS_ACTIVE_KEY);
  return active === "true";
}

function getAutoProcessType_() {
  const props = PropertiesService.getDocumentProperties();
  return props.getProperty(AUTO_PROCESS_TYPE_KEY) || "meta";
}

function setMetaAutoState_(processed, total) {
  const props = PropertiesService.getDocumentProperties();
  props.setProperty(META_AUTO_PROCESSED_KEY, String(processed));
  props.setProperty(META_AUTO_TOTAL_KEY, String(total));
}

function getMetaAutoState_() {
  const props = PropertiesService.getDocumentProperties();
  const processed = Number(props.getProperty(META_AUTO_PROCESSED_KEY) || "0");
  const total = Number(props.getProperty(META_AUTO_TOTAL_KEY) || "0");
  return {
    processed: isNaN(processed) || processed < 0 ? 0 : processed,
    total: isNaN(total) || total < 0 ? 0 : total
  };
}

function clearMetaAutoState_() {
  const props = PropertiesService.getDocumentProperties();
  props.deleteProperty(META_AUTO_PROCESSED_KEY);
  props.deleteProperty(META_AUTO_TOTAL_KEY);
}

function stopAutoProcess() {
  setAutoProcessActive_(false);
  clearMetaAutoState_();
  const logSheet = getOrCreateMetaLogSheet_();
  appendMetaLog_(logSheet, "SYSTEM", "stopped", "INFO", "Proses otomatis dihentikan oleh user");
}

function scheduleNextRun_() {
  const logSheet = getOrCreateMetaLogSheet_();
  appendMetaLog_(logSheet, "SYSTEM", "schedule", "INFO", "Melebihi batas waktu, menjadwalkan lanjutan...");

  deleteTriggers_();

  ScriptApp.newTrigger("continueAutoProcess_")
    .timeBased()
    .after(1 * 60 * 1000)
    .create();

  appendMetaLog_(logSheet, "SYSTEM", "scheduled", "INFO", "Lanjutan dijadwalkan 1 menit lagi");
}

function deleteTriggers_() {
  const triggers = ScriptApp.getProjectTriggers();
  triggers.forEach(trigger => {
    if (trigger.getHandlerFunction() === "continueAutoProcess_") {
      ScriptApp.deleteTrigger(trigger);
    }
  });
}

function continueAutoProcess_() {
  if (!isAutoProcessActive_()) {
    return;
  }

  const logSheet = getOrCreateMetaLogSheet_();
  appendMetaLog_(logSheet, "SYSTEM", "resume", "INFO", "Melanjutkan proses otomatis...");

  deleteTriggers_();

  const type = getAutoProcessType_();
  if (type === "meta") {
    processNextMetaAccount_();
  } else if (type === "google") {
    processAllGoogleSources_();
  }
}

function completeAutoProcess_() {
  setAutoProcessActive_(false);
  clearMetaAutoState_();
  deleteTriggers_();

  const logSheet = getOrCreateMetaLogSheet_();
  appendMetaLog_(logSheet, "SYSTEM", "complete", "SUCCESS", "🎉 Semua akun berhasil diproses!");
}

function handleAutoProcessError_(error) {
  const logSheet = getOrCreateMetaLogSheet_();
  appendMetaLog_(logSheet, "SYSTEM", "error", "ERROR", `Proses otomatis error: ${error.message}`);

  setAutoProcessActive_(false);
  clearMetaAutoState_();
  deleteTriggers_();
}

function getMetaApiSources_() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sh = ss.getSheetByName(META_SOURCE_SHEET);

  if (!sh) throw new Error("Sheet sumber MetaAds tidak ditemukan: " + META_SOURCE_SHEET);

  const lastRow = sh.getLastRow();
  if (lastRow < 2) return [];

  const rows = sh.getRange(2, 1, lastRow - 1, 4).getValues();
  const sources = [];

  rows.forEach((r, idx) => {
    const accountName = String(r[0] || "").trim();
    const token = String(r[1] || "").trim();
    const secretKey = String(r[2] || "").trim();
    const appId = String(r[3] || "").trim();

    if (!accountName || !token) return;

    sources.push({
      accountName,
      token,
      secretKey,
      appId,
      rowNumber: idx + 2
    });
  });

  return sources;
}

function getMetaFetchRangeForAccount_(targetSheet, accountName) {
  const today = new Date();
  const until = Utilities.formatDate(today, Session.getScriptTimeZone(), "yyyy-MM-dd");

  const lastRow = targetSheet.getLastRow();
  if (lastRow < 2) return { since: DEFAULT_SINCE, until };

  const values = targetSheet.getRange(2, 1, lastRow - 1, 7).getValues();
  let found = false;

  for (let i = 0; i < values.length; i++) {
    const source = String(values[i][6] || "").trim();
    if (source === accountName) {
      found = true;
      break;
    }
  }

  if (!found) return { since: DEFAULT_SINCE, until };

  const sinceDate = new Date();
  sinceDate.setDate(sinceDate.getDate() - 3);
  const minDate = new Date(DEFAULT_SINCE);
  const effective = sinceDate < minDate ? minDate : sinceDate;

  return {
    since: Utilities.formatDate(effective, Session.getScriptTimeZone(), "yyyy-MM-dd"),
    until
  };
}

function fetchMetaAdsRowsForSingleSource_(src, since, until) {
  const out = [];
  const logSheet = getOrCreateMetaLogSheet_();

  try {
    const testUrl = "https://graph.facebook.com/v19.0/me?access_token=" + encodeURIComponent(src.token);
    const testRes = fetchJsonMeta_(testUrl);

    if (!testRes.ok) {
      appendMetaLog_(logSheet, src.accountName, "token_check", "ERROR", "Token tidak valid: " + testRes.message);
      return [];
    }

    const rows = fetchMetaRawCampaignRowsByToken_(src.token, since, until);
    if (!rows.length) {
      appendMetaLog_(logSheet, src.accountName, "campaigns", "INFO", "Tidak ada data raw pada range tanggal.");
      return [];
    }

    rows.forEach(i => {
      out.push([
        i.date_start || "",
        i.campaign_name || "",
        toMetaNumber_(i.total_budget),
        toMetaNumber_(i.spend),
        toMetaNumber_(i.cpc),
        toMetaNumber_(i.clicks),
        src.accountName
      ]);
    });

    appendMetaLog_(
      logSheet,
      src.accountName,
      "complete",
      "SUCCESS",
      `Data raw Meta berhasil ditarik. Range ${since} s/d ${until}, Rows: ${out.length}`
    );
  } catch (error) {
    appendMetaLog_(logSheet, src.accountName, "error", "ERROR", error.message);
    throw error;
  }

  return out;
}

function fetchMetaRawCampaignRowsByToken_(token, since, until) {
  const accounts = fetchMetaAdAccountsByToken_(token);
  if (!accounts.length) return [];

  const rows = [];
  const campaignIds = {};

  accounts.forEach(acc => {
    let url =
      "https://graph.facebook.com/v19.0/act_" + acc.account_id + "/insights" +
      "?level=campaign" +
      "&fields=date_start,campaign_id,campaign_name,spend,clicks,cpc" +
      "&time_range[since]=" + encodeURIComponent(since) +
      "&time_range[until]=" + encodeURIComponent(until) +
      "&time_increment=1" +
      "&limit=1000" +
      "&access_token=" + encodeURIComponent(token);

    while (url) {
      const res = fetchJsonMeta_(url);
      if (!res.ok) {
        throw new Error("Gagal mengambil raw insights act_" + acc.account_id + ": " + res.message);
      }

      const data = (res.json && res.json.data) ? res.json.data : [];
      data.forEach(r => {
        const campaignId = String(r.campaign_id || "");
        if (campaignId) campaignIds[campaignId] = true;
        rows.push({
          date_start: r.date_start || "",
          campaign_id: campaignId,
          campaign_name: r.campaign_name || "",
          total_budget: 0,
          spend: r.spend || 0,
          clicks: r.clicks || 0,
          cpc: r.cpc || 0
        });
      });

      url = (res.json && res.json.paging && res.json.paging.next) ? res.json.paging.next : "";
    }
  });

  const budgetMap = fetchMetaCampaignBudgetMapByIds_(Object.keys(campaignIds), token);
  rows.forEach(r => {
    r.total_budget = budgetMap[r.campaign_id] || 0;
  });

  return rows;
}

function fetchMetaAdAccountsByToken_(token) {
  const accounts = [];
  let nextUrl =
    "https://graph.facebook.com/v19.0/me/adaccounts" +
    "?fields=account_id,name" +
    "&limit=200" +
    "&access_token=" + encodeURIComponent(token);

  while (nextUrl) {
    const res = fetchJsonMeta_(nextUrl);
    if (!res.ok) {
      throw new Error("Gagal mengambil ad accounts: " + res.message);
    }

    const data = (res.json && res.json.data) ? res.json.data : [];
    data.forEach(acc => {
      if (acc && acc.account_id) accounts.push(acc);
    });

    nextUrl = (res.json && res.json.paging && res.json.paging.next) ? res.json.paging.next : "";
  }

  return accounts;
}

function fetchMetaCampaignBudgetMapByIds_(campaignIds, token) {
  const budgetMap = {};
  for (let i = 0; i < campaignIds.length; i++) {
    const campaignId = String(campaignIds[i] || "").trim();
    if (!campaignId) continue;
    const url =
      "https://graph.facebook.com/v19.0/" + campaignId +
      "?fields=id,daily_budget,lifetime_budget" +
      "&access_token=" + encodeURIComponent(token);
    const res = fetchJsonMeta_(url);
    if (!res.ok) {
      continue;
    }
    const c = res.json || {};
    const totalBudget = c.lifetime_budget || c.daily_budget || 0;
    budgetMap[campaignId] = toMetaNumber_(totalBudget);
  }

  return budgetMap;
}

function toMetaNumber_(v) {
  const n = Number(String(v === null || v === undefined ? 0 : v).replace(/[^\d.-]/g, ""));
  return isNaN(n) ? 0 : n;
}

function mergeMetaAdsIntoSheet_(sheet, newRows) {
  const lastRow = sheet.getLastRow();
  const existing = lastRow > 1
    ? sheet.getRange(2, 1, lastRow - 1, 7).getValues()
    : [];

  const keyToRowIndex = {};
  for (let i = 0; i < existing.length; i++) {
    const row = existing[i];
    const key = buildMetaRowKey_(row[0], row[1], row[6]);
    if (key) keyToRowIndex[key] = i + 2;
  }

  let added = 0;
  let updated = 0;
  const rowsToAppend = [];
  const updates = [];

  newRows.forEach(r => {
    const key = buildMetaRowKey_(r[0], r[1], r[6]);
    if (!key) return;

    if (keyToRowIndex[key]) {
      updates.push({ row: keyToRowIndex[key], values: r });
      updated++;
    } else {
      rowsToAppend.push(r);
      added++;
    }
  });

  updates.forEach(u => {
    sheet.getRange(u.row, 1, 1, 7).setValues([u.values]);
  });

  if (rowsToAppend.length > 0) {
    sheet.getRange(sheet.getLastRow() + 1, 1, rowsToAppend.length, 7).setValues(rowsToAppend);
  }

  sortMetaTargetSheet_(sheet);

  return { added, updated };
}

function buildMetaRowKey_(dateVal, campaignName, sourceAccount) {
  const d = normalizeDateKey_(dateVal);
  const c = String(campaignName || "").trim().toLowerCase();
  const s = String(sourceAccount || "").trim().toLowerCase();

  if (!d || !c || !s) return "";
  return d + "|" + c + "|" + s;
}

function normalizeDateKey_(v) {
  if (!v) return "";
  const d = new Date(v);
  if (isNaN(d.getTime())) return "";
  return Utilities.formatDate(d, Session.getScriptTimeZone(), "yyyy-MM-dd");
}

function fetchMetaCampaignsByToken_(token) {
  let allCampaigns = [];
  let nextUrl =
    "https://graph.facebook.com/v19.0/me/adaccounts" +
    "?fields=account_id,name" +
    "&limit=200" +
    "&access_token=" + encodeURIComponent(token);

  const accounts = [];

  while (nextUrl) {
    const res = fetchJsonMeta_(nextUrl);

    if (!res.ok) {
      throw new Error("Gagal mengambil ad accounts: " + res.message);
    }

    const data = (res.json && res.json.data) ? res.json.data : [];
    data.forEach(acc => {
      if (acc && acc.account_id) accounts.push(acc);
    });

    nextUrl = (res.json && res.json.paging && res.json.paging.next) ? res.json.paging.next : "";
  }

  accounts.forEach(acc => {
    let campaignUrl =
      "https://graph.facebook.com/v19.0/act_" + acc.account_id + "/campaigns" +
      "?fields=id,name,daily_budget" +
      "&limit=1000" +
      "&access_token=" + encodeURIComponent(token);

    while (campaignUrl) {
      const campRes = fetchJsonMeta_(campaignUrl);

      if (!campRes.ok) {
        throw new Error("Gagal mengambil campaigns untuk act_" + acc.account_id + ": " + campRes.message);
      }

      const campaigns = (campRes.json && campRes.json.data) ? campRes.json.data : [];
      campaigns.forEach(c => allCampaigns.push(c));

      campaignUrl = (campRes.json && campRes.json.paging && campRes.json.paging.next) ? campRes.json.paging.next : "";
    }
  });

  return allCampaigns;
}

function fetchMetaInsightsByCampaign_(campaignId, token, since, until) {
  const url =
    "https://graph.facebook.com/v19.0/" + campaignId + "/insights" +
    "?fields=date_start,spend,inline_link_clicks,cost_per_inline_link_click" +
    "&time_range[since]=" + encodeURIComponent(since) +
    "&time_range[until]=" + encodeURIComponent(until) +
    "&time_increment=1" +
    "&access_token=" + encodeURIComponent(token);

  const res = fetchJsonMeta_(url);

  if (!res.ok) {
    throw new Error("Gagal mengambil insights campaign " + campaignId + ": " + res.message);
  }

  return (res.json && res.json.data) ? res.json.data : [];
}

function fetchJsonMeta_(url) {
  return fetchJsonMetaWithRetry_(url, 3);
}

/**
 * Fetch JSON with retry mechanism
 */
function fetchJsonMetaWithRetry_(url, maxRetries) {
  let lastError = null;

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      const response = UrlFetchApp.fetch(url, {
        muteHttpExceptions: true,
        timeout: 30000
      });

      const code = response.getResponseCode();
      const text = response.getContentText() || "";

      let json = null;
      try {
        json = JSON.parse(text);
      } catch (e) {}

      const message =
        (json && json.error && json.error.message)
          ? json.error.message
          : ("HTTP " + code);

      if (code >= 200 && code < 300) {
        return {
          ok: true,
          code,
          text,
          json,
          message
        };
      }

      if (code === 429 || code === 408 || code >= 500) {
        const waitTime = Math.pow(2, attempt) * 1000;
        console.log(`Rate limited, waiting ${waitTime}ms before retry ${attempt + 1}`);
        Utilities.sleep(waitTime);
        continue;
      }

      lastError = {
        ok: false,
        code,
        text,
        json,
        message
      };

    } catch (error) {
      lastError = {
        ok: false,
        code: 0,
        text: "",
        json: null,
        message: error.message
      };
    }

    if (attempt < maxRetries) {
      const waitTime = Math.pow(2, attempt) * 1000;
      console.log(`Waiting ${waitTime}ms before retry ${attempt + 1}`);
      Utilities.sleep(waitTime);
    }
  }

  return lastError || {
    ok: false,
    code: 0,
    text: "",
    json: null,
    message: "Unknown error after retries"
  };
}

function ensureMetaTargetHeader_(sheet) {
  const headers = [
    "Date",
    "Campaign Name",
    "Total Budget",
    "Total Spend",
    "CPC",
    "Click",
    "Source Account"
  ];

  sheet.getRange(1, 1, 1, headers.length).setValues([headers]);
}

function sortMetaTargetSheet_(sheet) {
  const lastRow = sheet.getLastRow();
  if (lastRow <= 2) return;

  sheet.getRange(2, 1, lastRow - 1, 7).sort([
    { column: 1, ascending: true },
    { column: 7, ascending: true },
    { column: 2, ascending: true }
  ]);
}

function getMetaCursorIndex_() {
  const props = PropertiesService.getDocumentProperties();
  const raw = props.getProperty(META_CURSOR_PROPERTY_KEY);
  const n = Number(raw);
  return isNaN(n) || n < 0 ? 0 : n;
}

function setMetaCursorIndex_(index) {
  const props = PropertiesService.getDocumentProperties();
  props.setProperty(META_CURSOR_PROPERTY_KEY, String(index));
}

function getOrCreateMetaLogSheet_() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const name = "Log_MetaAds";
  const sh = ss.getSheetByName(name) || ss.insertSheet(name);

  if (sh.getLastRow() === 0) {
    sh.getRange(1, 1, 1, 7).setValues([[
      "Timestamp",
      "Source Account",
      "Step",
      "Status",
      "Message",
      "Account Index",
      "Total Accounts"
    ]]);
  }

  return sh;
}

function getOrCreateGoogleLogSheet_() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const name = "Log_Google";
  const sh = ss.getSheetByName(name) || ss.insertSheet(name);

  if (sh.getLastRow() === 0) {
    sh.getRange(1, 1, 1, 5).setValues([[
      "Timestamp",
      "Source",
      "Step",
      "Status",
      "Message"
    ]]);
  }

  return sh;
}

function appendMetaLog_(logSheet, accountName, step, status, message, accountIndex, totalAccounts) {
  logSheet.appendRow([
    Utilities.formatDate(new Date(), Session.getScriptTimeZone(), "yyyy-MM-dd HH:mm:ss"),
    accountName,
    step,
    status,
    message,
    accountIndex || "",
    totalAccounts || ""
  ]);
}

function appendGoogleLog_(logSheet, source, step, status, message) {
  logSheet.appendRow([
    Utilities.formatDate(new Date(), Session.getScriptTimeZone(), "yyyy-MM-dd HH:mm:ss"),
    source,
    step,
    status,
    message
  ]);
}

/**
 * Get current progress info for display
 */
function getProgressInfo_() {
  const sources = getMetaApiSources_();
  const cursor = getMetaCursorIndex_();
  const currentAccount = sources[cursor] ? sources[cursor].accountName : "Unknown";

  return {
    total: sources.length,
    current: cursor + 1,
    currentAccount: currentAccount,
    remaining: sources.length - cursor - 1
  };
}


/********************************************************
 * BATCH & PERFORMANCE HELPERS
 ********************************************************/
function batchArray_(array, batchSize) {
  const batches = [];
  for (let i = 0; i < array.length; i += batchSize) {
    batches.push(array.slice(i, i + batchSize));
  }
  return batches;
}

function fetchMetaInsightsForMultipleCampaigns_(campaigns, token, since, until) {
  const insightsBatch = [];

  campaigns.forEach(camp => {
    try {
      const insights = fetchMetaInsightsByCampaignWithCache_(camp.id, token, since, until);
      insightsBatch.push(insights);
    } catch (error) {
      insightsBatch.push([]);
      console.error(`Gagal fetch insights untuk campaign ${camp.id}: ${error.message}`);
    }
  });

  return insightsBatch;
}

function fetchMetaInsightsByCampaignWithCache_(campaignId, token, since, until) {
  const cacheKey = `meta_insights_${campaignId}_${since}_${until}`;
  const cache = CacheService.getDocumentCache();

  const cachedData = cache.get(cacheKey);
  if (cachedData) {
    console.log(`Cache hit for campaign ${campaignId}`);
    return JSON.parse(cachedData);
  }

  const insights = fetchMetaInsightsByCampaign_(campaignId, token, since, until);
  cache.put(cacheKey, JSON.stringify(insights), 1800);

  return insights;
}


/********************************************************
 * HELPERS UMUM
 ********************************************************/
function normalizeHeader_(h) {
  return String(h || "")
    .toLowerCase()
    .trim()
    .replace(/[_]+/g, " ")
    .replace(/[()]/g, " ")
    .replace(/[^a-z0-9\s%]/g, " ")
    .replace(/\s+/g, " ");
}

function findColumn(headers, keywords) {
  for (let i = 0; i < headers.length; i++) {
    for (let key of keywords) {
      const k = String(key || "").toLowerCase();
      if (headers[i].includes(k)) return i;
    }
  }
  return -1;
}

function isNonEmpty_(v) {
  if (v === null || v === undefined) return false;
  return String(v).trim().length > 0;
}

function isNumberOrZero_(v) {
  if (v === null || v === undefined || v === "") return false;
  const n = Number(String(v).replace(/[^\d.-]/g, ""));
  return !isNaN(n);
}

function normalizeSite_(site) {
  return String(site)
    .trim()
    .replace(/^https?:\/\//i, "")
    .replace(/\/+$/g, "")
    .toLowerCase();
}

const APP_CONFIG = {
  APP_NAME: 'Revenue Performance 2026 - PT Tren Gen Horizon',
  API_VERSION: 'v25.0',
  SETTINGS_SHEET: 'SETTINGS_META',
  DATA_SHEET: 'RAW_META_DATA',
  LOG_SHEET: 'LOG_SYNC_META',
  EXPORT_SHEET: 'META_CAMPAIGN_METADATA',
  ROI_SHEET: 'ROI TODAY',
  ALERT_EMAIL_TO: 'triosptr@trendhorizone.id',
  META_BASE_URL: 'https://graph.facebook.com',
  DEFAULT_LOOKBACK_DAYS: 3,
  MUTABLE_LOOKBACK_DAYS: 2
};

const PROP_KEYS = {
  LAST_SYNC_UNTIL_PREFIX: 'LAST_SYNC_UNTIL_',
  TRACKER_STREAM: 'TRACKER_STREAM_V1'
};

/* =========================
 * UI
 * ========================= */
function doGet() {
  initializeApp_();
  cleanupAutoTriggers_();

  const tpl = HtmlService.createTemplateFromFile('index');
  tpl.APP_VERSION = APP_CONFIG.APP_VERSION;
  tpl.APP_NAME = APP_CONFIG.APP_NAME;

  return tpl.evaluate()
    .setTitle(APP_CONFIG.APP_NAME + ' - ' + APP_CONFIG.APP_VERSION)
    .setXFrameOptionsMode(HtmlService.XFrameOptionsMode.ALLOWALL);
}

function onOpen() {
  initializeApp_();
  cleanupAutoTriggers_();

  SpreadsheetApp.getUi()
    .createMenu('Refresh Data Here')
    .addItem('Initialize Sheets', 'initializeApp_')
    .addItem('Run Sync Now', 'runSyncFromSheetDefault_')
    .addItem('Run Update All Fast', 'runUpdateAllFast')
    .addItem('Run Check Connection Now', 'checkAllConnectionsHourly')
    .addToUi();
}

function cleanupAutoTriggers_() {
  const handlersToRemove = { checkAllConnectionsHourly: true, syncTodayHourly: true };
  ScriptApp.getProjectTriggers().forEach(t => {
    const fn = t.getHandlerFunction();
    if (handlersToRemove[fn]) {
      ScriptApp.deleteTrigger(t);
    }
  });
}

function runUpdateGoogleEarning() {
  initializeApp_();
  try {
    if (typeof updateAllGoogleAuto !== 'function') {
      throw new Error('Fungsi updateAllGoogleAuto tidak ditemukan.');
    }
    updateAllGoogleAuto();
    logSync_('INFO', 'UPDATE_GOOGLE_EARNING', '', '', 'Update Google Earning berhasil dijalankan', '');
    return { success: true, message: 'Update Google Earning berhasil dijalankan.' };
  } catch (e) {
    logSync_('ERROR', 'UPDATE_GOOGLE_EARNING', '', '', e.message || String(e), '');
    throw e;
  }
}

function runUpdateAllFast() {
  initializeApp_();
  try {
    if (typeof updateAllGoogleAuto !== 'function') {
      throw new Error('Fungsi updateAllGoogleAuto tidak ditemukan.');
    }
    updateAllGoogleAuto();
    const today = formatDate_(new Date());
    const syncRes = syncMetaData({
      startDate: today,
      endDate: today,
      incremental: true,
      skipSort: true,
      skipFilters: true
    });
    logSync_('INFO', 'UPDATE_ALL_FAST', '', '', `Update all cepat selesai untuk ${today}`, JSON.stringify(syncRes?.totals || {}));
    return {
      success: true,
      message: `Update all selesai (data Meta tanggal ${today})`,
      today,
      totals: syncRes?.totals || { fetched: 0, inserted: 0, updated: 0 }
    };
  } catch (e) {
    logSync_('ERROR', 'UPDATE_ALL_FAST', '', '', e.message || String(e), '');
    throw e;
  }
}

/* =========================
 * INIT
 * ========================= */
function initializeApp_() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();

  let settings = ss.getSheetByName(APP_CONFIG.SETTINGS_SHEET);
  if (!settings) {
    settings = ss.insertSheet(APP_CONFIG.SETTINGS_SHEET);
    settings.getRange(1, 1, 1, 9).setValues([[
      'ACTIVE',
      'ACCOUNT_NAME',
      'AD_ACCOUNT_ID',
      'ACCESS_TOKEN',
      'TOKEN_PREVIEW',
      'LAST_TEST_STATUS',
      'LAST_TEST_AT',
      'ACCOUNT_STATUS_TEXT',
      'NOTES'
    ]]);
    settings.setFrozenRows(1);
  } else {
    // Pastikan header ada tanpa menghapus data user.
    // Jika header sudah benar di baris 1, tidak melakukan apa-apa.
    // Jika header ada tetapi formatnya berbeda, hanya perbaiki baris header.
    const expected = [
      'ACTIVE',
      'ACCOUNT_NAME',
      'AD_ACCOUNT_ID',
      'ACCESS_TOKEN',
      'TOKEN_PREVIEW',
      'LAST_TEST_STATUS',
      'LAST_TEST_AT',
      'ACCOUNT_STATUS_TEXT',
      'NOTES'
    ];

    const h1 = settings.getRange(1, 1, 1, 9).getValues()[0].map(v => String(v || '').trim().toUpperCase());
    const h2 = settings.getRange(2, 1, 1, 9).getValues()[0].map(v => String(v || '').trim().toUpperCase());

    const looksLikeHeader = (h) => h[0] === 'ACTIVE' && h[1] === 'ACCOUNT_NAME' && h[2] === 'AD_ACCOUNT_ID' && h[3] === 'ACCESS_TOKEN';

    if (looksLikeHeader(h1)) {
      // Jika header baris 1 ada tapi sebagian nama kolom tidak persis, rapikan baris 1 saja.
      settings.getRange(1, 1, 1, 9).setValues([expected]);
      settings.setFrozenRows(1);
    } else if (looksLikeHeader(h2) && h1.every(x => !x)) {
      // Header ternyata di baris 2 dan baris 1 kosong: biarkan agar tidak menggeser data.
      settings.setFrozenRows(2);
    } else if (settings.getLastRow() === 0 || h1.every(x => !x)) {
      // Sheet kosong: tulis header ke baris 1.
      settings.getRange(1, 1, 1, 9).setValues([expected]);
      settings.setFrozenRows(1);
    }
  }

  ensureDataSheetReady_();
  ensureLogSheetReady_();

  let exportSheet = ss.getSheetByName(APP_CONFIG.EXPORT_SHEET);
  if (!exportSheet) {
    exportSheet = ss.insertSheet(APP_CONFIG.EXPORT_SHEET);
    exportSheet.getRange(1, 1, 1, 31).setValues([[
      'Exported At',
      'Exported By',
      'Account Name',
      'Ad Account ID',
      'Campaign ID',
      'Campaign Name',
      'Campaign Status',
      'Objective',
      'Countries',
      'Interests',
      'Devices',
      'Placements',
      'Adset ID',
      'Adset Name',
      'Adset Status',
      'Budget Type',
      'Budget Value',
      'Optimization Goal',
      'Billing Event',
      'Ad ID',
      'Ad Name',
      'Ad Status',
      'Creative ID',
      'Media Count',
      'Media URLs',
      'Title List',
      'Headline List',
      'Description List',
      'Link URL',
      'Parameter',
      'CTA'
    ]]);
    exportSheet.setFrozenRows(1);
  }

  formatSheets_();
}

function findSettingsHeaderRow_(sh) {
  const maxScan = Math.min(5, Math.max(sh.getLastRow(), 1));
  const expectedA = ['ACTIVE', 'ACCOUNT_NAME', 'AD_ACCOUNT_ID', 'ACCESS_TOKEN'];

  for (let r = 1; r <= maxScan; r++) {
    const row = sh.getRange(r, 1, 1, 4).getValues()[0].map(v => String(v || '').trim().toUpperCase());
    const match = row.every((v, i) => v === expectedA[i]);
    if (match) return r;
  }

  return 1;
}

function formatSheets_() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();

  const data = ss.getSheetByName(APP_CONFIG.DATA_SHEET);
  if (data && data.getLastRow() > 1) {
    data.getRange(2, 1, data.getLastRow() - 1, 1).setNumberFormat('yyyy-mm-dd');
    data.getRange(2, 6, Math.max(data.getLastRow() - 1, 1), 3).setNumberFormat('#,##0.00');
    data.getRange(2, 9, Math.max(data.getLastRow() - 1, 1), 1).setNumberFormat('#,##0');
  }

  const settings = ss.getSheetByName(APP_CONFIG.SETTINGS_SHEET);
  if (settings && settings.getLastRow() > 1) {
    const headerRow = findSettingsHeaderRow_(settings);
    const startRow = headerRow + 1;
    if (settings.getLastRow() >= startRow) {
      settings.getRange(startRow, 6, settings.getLastRow() - startRow + 1, 3).setHorizontalAlignment('center');
    }
  }
}

/* =========================
 * BOOTSTRAP
 * ========================= */
function getAppBootstrap() {
  initializeApp_();
  logSync_('INFO', 'OPEN_DASHBOARD', '', '', 'User membuka dashboard', '');

  return {
    success: true,
    appName: APP_CONFIG.APP_NAME,
    appVersion: APP_CONFIG.APP_VERSION,
    today: formatDate_(new Date()),
    currentUserEmail: getCurrentUserEmail_(),
    defaultStart: formatDate_(shiftDate_(new Date(), -7)),
    defaultEnd: formatDate_(new Date()),
    accounts: getAccounts_(),
    filters: getAvailableFilters_(),
    triggerInstalled: false
  };
}

function getActivityLogs(limit) {
  initializeApp_();
  const sh = ensureLogSheetReady_();
  const max = Math.min(Math.max(Number(limit || 60), 1), 300);
  if (!sh || sh.getLastRow() < 2) {
    return {
      success: true,
      logs: [],
      sourceSheet: APP_CONFIG.LOG_SHEET,
      totalRows: 0,
      shownRows: 0
    };
  }
  const col = Math.max(sh.getLastColumn(), 8);
  const all = sh.getRange(1, 1, sh.getLastRow(), col).getValues();
  let totalValidRows = 0;
  const logs = [];
  for (let i = all.length - 1; i >= 0; i--) {
    const r = all[i];
    if (!r || r.every(v => String(v || '').trim() === '')) continue;
    const up = r.map(v => String(v || '').trim().toUpperCase());
    if (isLikelyLogHeader_(up)) continue;

    const asNew = {
      timestamp: r[0] || '',
      userEmail: r[1] || '',
      level: r[2] || '',
      action: r[3] || '',
      accountName: r[4] || '',
      adAccountId: r[5] || '',
      message: r[6] || '',
      extra: r[7] || ''
    };
    const asLegacy = {
      timestamp: r[0] || '',
      userEmail: '',
      level: r[1] || '',
      action: r[2] || '',
      accountName: r[3] || '',
      adAccountId: r[4] || '',
      message: r[5] || '',
      extra: r[6] || ''
    };
    const lvlNew = String(asNew.level || '').toUpperCase();
    const lvlOld = String(asLegacy.level || '').toUpperCase();
    const isNew = ['INFO', 'WARN', 'ERROR'].indexOf(lvlNew) >= 0 || String(asNew.action || '').trim() !== '';
    const isOld = ['INFO', 'WARN', 'ERROR'].indexOf(lvlOld) >= 0 || String(asLegacy.action || '').trim() !== '';
    const picked = isNew ? asNew : (isOld ? asLegacy : asNew);
    if (!String(picked.timestamp || '').trim()) continue;
    totalValidRows++;
    logs.push(picked);
    if (logs.length >= max) break;
  }

  const lastDashboardLogin = logs.find(l => String(l.action || '').toUpperCase() === 'LOGIN_DASHBOARD') || null;
  return {
    success: true,
    logs,
    lastDashboardLogin,
    sourceSheet: APP_CONFIG.LOG_SHEET,
    totalRows: totalValidRows,
    shownRows: logs.length
  };
}

function getLatestSyncInfo() {
  initializeApp_();
  const res = getActivityLogs(300);
  const logs = res?.logs || [];
  const preferred = ['UPDATE_ALL_FAST', 'SYNC_FINISH', 'UPDATE_GOOGLE_EARNING', 'SYNC_REQUEST'];
  let picked = null;
  for (let i = 0; i < preferred.length; i++) {
    picked = logs.find(x => String(x?.action || '').toUpperCase() === preferred[i]);
    if (picked) break;
  }
  return {
    success: true,
    by: picked?.userEmail || '',
    at: picked?.timestamp || '',
    action: picked?.action || '',
    message: picked?.message || ''
  };
}

function getGoogleActivityLogs(limit) {
  initializeApp_();
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sh = ss.getSheetByName('Log_Google');
  const max = Math.min(Math.max(Number(limit || 60), 1), 300);
  if (!sh || sh.getLastRow() < 2) {
    return { success: true, logs: [] };
  }
  const rows = sh.getRange(2, 1, sh.getLastRow() - 1, 5).getValues();
  const out = [];
  for (let i = rows.length - 1; i >= 0; i--) {
    const r = rows[i];
    if (!r || r.every(v => String(v || '').trim() === '')) continue;
    out.push({
      timestamp: String(r[0] || ''),
      source: String(r[1] || ''),
      step: String(r[2] || ''),
      status: String(r[3] || ''),
      message: String(r[4] || '')
    });
    if (out.length >= max) break;
  }
  return { success: true, logs: out };
}

function getTrackerStream(payload) {
  initializeApp_();
  const sinceId = Math.max(Number(payload?.sinceId || 0), 0);
  const limit = Math.min(Math.max(Number(payload?.limit || 300), 1), 1000);
  const state = getTrackerState_();
  const events = (state.events || []).filter(e => Number(e.id || 0) > sinceId).slice(-limit);
  return {
    success: true,
    events,
    lastId: Number(state.seq || 0)
  };
}

function trackClientEvent(payload) {
  initializeApp_();
  const event = appendTrackerEvent_({
    timestamp: formatDateTime_(new Date()),
    level: String(payload?.level || 'INFO').toUpperCase(),
    action: String(payload?.action || 'CLIENT_EVENT').slice(0, 80),
    userEmail: getCurrentUserEmail_(),
    message: String(payload?.message || '').slice(0, 500),
    extra: String(payload?.extra || '').slice(0, 500),
    source: 'CLIENT'
  });
  return { success: true, event };
}

function trackDashboardLogin(payload) {
  initializeApp_();
  const mode = String(payload?.mode || 'PIN').trim();
  const ok = payload?.success !== false;
  const action = ok ? 'LOGIN_DASHBOARD' : 'LOGIN_DASHBOARD_FAILED';
  logSync_(
    ok ? 'INFO' : 'WARN',
    action,
    '',
    '',
    ok ? `User login ke dashboard via ${mode}` : `Percobaan login gagal via ${mode}`,
    JSON.stringify({ mode, userAgent: String(payload?.userAgent || '').slice(0, 300) })
  );
  return { success: true };
}

/* =========================
 * SETTINGS ACCOUNT
 * ========================= */
function saveAccounts(accounts) {
  initializeApp_();

  if (!Array.isArray(accounts)) {
    throw new Error('Format akun tidak valid.');
  }

  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sh = ss.getSheetByName(APP_CONFIG.SETTINGS_SHEET);

  const headerRow = findSettingsHeaderRow_(sh);
  const startRow = headerRow + 1;

  const expectedHeader = [[
    'ACTIVE',
    'ACCOUNT_NAME',
    'AD_ACCOUNT_ID',
    'ACCESS_TOKEN',
    'TOKEN_PREVIEW',
    'LAST_TEST_STATUS',
    'LAST_TEST_AT',
    'ACCOUNT_STATUS_TEXT',
    'NOTES'
  ]];

  // Baca data existing tanpa menghapus apa pun (hindari pengosongan data).
  const existingRowCount = sh.getLastRow() >= startRow ? (sh.getLastRow() - startRow + 1) : 0;
  const existingValues = existingRowCount > 0
    ? sh.getRange(startRow, 1, existingRowCount, 9).getValues()
    : [];

  const existingMap = {};
  existingValues.forEach((r, i) => {
    const id = normalizeAdAccountId_(r[2] || '');
    if (!id) return;
    existingMap[id] = {
      lastTestStatus: r[5] || '',
      lastTestAt: r[6] || '',
      accountStatusText: r[7] || '',
      notes: r[8] || ''
    };
  });

  const rows = accounts
    .filter(a => a.accountName || a.adAccountId || a.accessToken)
    .map(a => {
      const id = normalizeAdAccountId_(a.adAccountId || '');
      const old = existingMap[id] || {};
      const token = String(a.accessToken || '').trim();
      return [
        a.active ? 'TRUE' : 'FALSE',
        String(a.accountName || '').trim(),
        id,
        token,
        maskToken_(token),
        old.lastTestStatus || '',
        old.lastTestAt || '',
        old.accountStatusText || '',
        a.notes || old.notes || ''
      ];
    });

  // Pastikan header benar (tanpa menghapus data user).
  sh.getRange(headerRow, 1, 1, 9).setValues(expectedHeader);
  sh.setFrozenRows(headerRow);

  // Tulis data baru ke area data, lalu bersihkan sisa baris lama (jika ada).
  if (rows.length) {
    sh.getRange(startRow, 1, rows.length, 9).setValues(rows);
  }

  if (existingRowCount > rows.length) {
    sh.getRange(startRow + rows.length, 1, existingRowCount - rows.length, 9).clearContent();
  }

  formatSheets_();
  ensureHourlyConnectionTrigger_();

  return {
    success: true,
    message: `Berhasil menyimpan ${rows.length} akun.`
  };
}

function getAccounts_() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sh = ss.getSheetByName(APP_CONFIG.SETTINGS_SHEET);
  if (!sh || sh.getLastRow() < 2) return [];

  const headerRow = findSettingsHeaderRow_(sh);
  const startRow = headerRow + 1;
  if (sh.getLastRow() < startRow) return [];

  const width = Math.max(6, sh.getLastColumn());
  return sh.getRange(startRow, 1, sh.getLastRow() - startRow + 1, width).getValues()
    .map(r => {
      const status = String(r[5] || '').trim();
      const lastAt = String(r[6] || '').trim();
      return {
        active: String(r[0]).toUpperCase() === 'TRUE',
        accountName: r[1] || '',
        adAccountId: r[2] || '',
        accessToken: r[3] || '',
        tokenPreview: r[4] || '',
        lastTestStatus: status,
        lastTestAt: lastAt,
        accountStatusText: r[7] || '',
        notes: r[8] || ''
      };
    })
    .filter(r => r.accountName && String(r.accountName).trim() !== '');
}

function updateAccountTestStatus_(adAccountId, status, dt, statusText) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sh = ss.getSheetByName(APP_CONFIG.SETTINGS_SHEET);
  if (!sh || sh.getLastRow() < 2) return;

  const headerRow = findSettingsHeaderRow_(sh);
  const startRow = headerRow + 1;
  if (sh.getLastRow() < startRow) return;

  const values = sh.getRange(startRow, 1, sh.getLastRow() - startRow + 1, 9).getValues();
  for (let i = 0; i < values.length; i++) {
    if (normalizeAdAccountId_(values[i][2]) === normalizeAdAccountId_(adAccountId)) {
      sh.getRange(startRow + i, 6).setValue(status);
      sh.getRange(startRow + i, 7).setValue(formatDateTime_(dt));
      sh.getRange(startRow + i, 8).setValue(statusText || '');
      break;
    }
  }
}

function testAccountConnection(input) {
  initializeApp_();

  const accountName = input.accountName || '';
  const adAccountId = normalizeAdAccountId_(input.adAccountId || '');
  const accessToken = String(input.accessToken || '').trim();

  if (!adAccountId || !accessToken) {
    throw new Error('Ad Account ID dan Access Token wajib diisi.');
  }

  try {
    const url = `${APP_CONFIG.META_BASE_URL}/${APP_CONFIG.API_VERSION}/${encodeURIComponent(adAccountId)}?fields=id,name,account_status&access_token=${encodeURIComponent(accessToken)}`;
    const res = fetchJson_(url);

    updateAccountTestStatus_(
      adAccountId,
      'CONNECTED',
      new Date(),
      `Connected • account_status: ${res.account_status ?? '-'}`
    );

    logSync_('INFO', 'TEST_CONNECTION', accountName, adAccountId, 'Test koneksi berhasil', JSON.stringify(res));

    return {
      success: true,
      message: `Koneksi berhasil untuk ${accountName || adAccountId}`,
      data: res
    };
  } catch (err) {
    updateAccountTestStatus_(
      adAccountId,
      'FAILED',
      new Date(),
      err.message
    );

    logSync_('ERROR', 'TEST_CONNECTION', accountName, adAccountId, err.message, '');

    return {
      success: false,
      message: `Koneksi gagal: ${err.message}`
    };
  }
}

function testAllAccountsConnection() {
  initializeApp_();
  const result = runAllAccountConnectionChecks_({ sourceAction: 'MANUAL_TEST_ALL_CONNECTION', sendAlertEmail: false });
  return {
    success: true,
    message: `Test selesai • total ${result.total}, connected ${result.connected}, failed ${result.failed}`,
    total: result.total,
    connected: result.connected,
    failed: result.failed,
    details: result.details
  };
}

function getAccountNotifications() {
  initializeApp_();
  const accounts = getAccounts_().filter(a => a.active && a.adAccountId && a.accessToken);
  const alerts = [];

  const mapSeverity = (type) => (type === 'PAYMENT' ? 'HIGH' : 'MEDIUM');

  accounts.forEach(acc => {
    try {
      const accId = normalizeAdAccountId_(acc.adAccountId);
      const token = String(acc.accessToken || '').trim();
      const base = `${APP_CONFIG.META_BASE_URL}/${APP_CONFIG.API_VERSION}`;
      const info = fetchJson_(`${base}/${encodeURIComponent(accId)}?fields=id,name,account_status,disable_reason&access_token=${encodeURIComponent(token)}`);
      const accountStatus = Number(info?.account_status || 0);
      const disableReason = Number(info?.disable_reason || 0);

      if (accountStatus !== 1 || disableReason > 0) {
        alerts.push({
          accountName: acc.accountName || accId,
          adAccountId: accId,
          type: 'PAYMENT',
          severity: 'HIGH',
          entityName: info?.name || acc.accountName || accId,
          code: `account_status:${accountStatus},disable_reason:${disableReason}`,
          message: `Akun butuh pengecekan pembayaran/billing (account_status ${accountStatus}, disable_reason ${disableReason}).`
        });
      }

      let cUrl = `${base}/${encodeURIComponent(accId)}/campaigns?fields=id,name,effective_status,status,issues_info&limit=50&access_token=${encodeURIComponent(token)}`;
      let fetched = 0;
      while (cUrl && fetched < 100) {
        const cRes = fetchJson_(cUrl);
        const data = cRes?.data || [];
        data.forEach(c => {
          const eff = String(c?.effective_status || '').toUpperCase();
          const hasIssue = Array.isArray(c?.issues_info) && c.issues_info.length > 0;
          if (eff === 'DISAPPROVED' || eff === 'WITH_ISSUES' || hasIssue) {
            alerts.push({
              accountName: acc.accountName || accId,
              adAccountId: accId,
              type: 'ADSETUP',
              severity: mapSeverity('ADSETUP'),
              entityName: c?.name || c?.id || '-',
              code: eff || 'WITH_ISSUES',
              message: hasIssue ? `Campaign memiliki issues_info (${c.issues_info.length} issue).` : `Campaign status ${eff}.`
            });
          }
        });
        fetched += data.length;
        cUrl = cRes?.paging?.next || '';
      }
    } catch (e) {
      alerts.push({
        accountName: acc.accountName || acc.adAccountId || '-',
        adAccountId: normalizeAdAccountId_(acc.adAccountId || ''),
        type: 'CONNECTION',
        severity: 'HIGH',
        entityName: acc.accountName || acc.adAccountId || '-',
        code: 'FETCH_FAILED',
        message: `Gagal mengambil notifikasi akun: ${e.message || e}`
      });
    }
  });

  logSync_(
    'INFO',
    'FETCH_ACCOUNT_ALERTS',
    '',
    '',
    `Notifikasi akun dibaca: ${alerts.length} alert`,
    JSON.stringify({ accounts: accounts.length, alerts: alerts.length })
  );

  return {
    success: true,
    generatedAt: formatDateTime_(new Date()),
    alerts,
    summary: {
      totalAccounts: accounts.length,
      totalAlerts: alerts.length,
      high: alerts.filter(a => a.severity === 'HIGH').length,
      medium: alerts.filter(a => a.severity === 'MEDIUM').length
    }
  };
}

function sendAccountAlertsEmail(payload) {
  initializeApp_();
  const to = String(payload?.to || APP_CONFIG.ALERT_EMAIL_TO || '').trim();
  if (!to) throw new Error('Email tujuan notifikasi belum diatur.');
  const data = getAccountNotifications();
  const alerts = data.alerts || [];
  if (!alerts.length) {
    logSync_('INFO', 'SEND_ALERT_EMAIL_SKIPPED', '', '', `Tidak ada alert aktif untuk dikirim ke ${to}`, '');
    return { success: true, sent: false, message: 'Tidak ada alert aktif.', to };
  }

  const subject = `[MetaApp Alert] ${alerts.length} notifikasi akun`;
  const lines = [];
  lines.push(`Generated: ${data.generatedAt || formatDateTime_(new Date())}`);
  lines.push(`Total Accounts: ${data.summary?.totalAccounts || 0}`);
  lines.push(`Total Alerts: ${data.summary?.totalAlerts || 0}`);
  lines.push(`HIGH: ${data.summary?.high || 0} | MEDIUM: ${data.summary?.medium || 0}`);
  lines.push('');
  alerts.slice(0, 120).forEach((a, i) => {
    lines.push(`${i + 1}. [${a.severity}] [${a.type}] ${a.accountName} (${a.adAccountId})`);
    lines.push(`   Entity: ${a.entityName}`);
    lines.push(`   Code: ${a.code}`);
    lines.push(`   Message: ${a.message}`);
    lines.push('');
  });

  MailApp.sendEmail({
    to,
    subject,
    body: lines.join('\n')
  });

  logSync_('INFO', 'SEND_ALERT_EMAIL', '', '', `Kirim email alert ke ${to}`, JSON.stringify({ alerts: alerts.length }));
  return {
    success: true,
    sent: true,
    to,
    totalAlerts: alerts.length,
    message: `Email alert berhasil dikirim ke ${to}`
  };
}

function updateCampaignBudget(payload) {
  initializeApp_();

  const campaignId = String(payload?.campaignId || '').trim();
  if (!campaignId) throw new Error('Campaign ID wajib diisi.');

  const budgetTypeRaw = String(payload?.budgetType || '').trim().toUpperCase();
  const budgetType = budgetTypeRaw === 'LIFETIME' ? 'LIFETIME' : 'DAILY';
  const budgetValue = Number(payload?.budgetValue);
  if (!budgetValue || budgetValue <= 0) throw new Error('Budget value harus lebih dari 0.');

  const resolved = resolveAccountForCampaignAction_(payload || {});
  const endpoint = `${APP_CONFIG.META_BASE_URL}/${APP_CONFIG.API_VERSION}/${encodeURIComponent(campaignId)}`;
  const postPayload = { access_token: resolved.accessToken };
  if (budgetType === 'LIFETIME') postPayload.lifetime_budget = String(Math.round(budgetValue));
  else postPayload.daily_budget = String(Math.round(budgetValue));

  const updateRes = fetchJson_(endpoint, { method: 'post', payload: postPayload, muteHttpExceptions: true });
  const detail = fetchJson_(`${endpoint}?fields=id,name,objective,status,daily_budget,lifetime_budget&access_token=${encodeURIComponent(resolved.accessToken)}`);

  logSync_(
    'INFO',
    'UPDATE_CAMPAIGN_BUDGET',
    resolved.accountName || '',
    resolved.adAccountId || '',
    `Campaign ${campaignId} updated`,
    JSON.stringify({ budgetType, budgetValue, updateRes })
  );

  return {
    success: true,
    message: `Budget campaign ${detail.name || campaignId} berhasil diperbarui.`,
    campaign: detail
  };
}

function listCampaigns(payload) {
  initializeApp_();
  const resolved = resolveAccountForCampaignAction_(payload || {});
  const includeInactive = String(payload?.includeInactive || '').toUpperCase() === 'TRUE';
  const maxItems = Math.min(Math.max(Number(payload?.limit || 300), 1), 1000);
  try {
    const fields = 'id,name,status,objective,daily_budget,lifetime_budget,updated_time';
    let url = `${APP_CONFIG.META_BASE_URL}/${APP_CONFIG.API_VERSION}/${encodeURIComponent(resolved.adAccountId)}/campaigns?fields=${encodeURIComponent(fields)}&limit=100&access_token=${encodeURIComponent(resolved.accessToken)}`;
    const all = [];

    while (url && all.length < maxItems) {
      const page = fetchJson_(url);
      const data = page?.data || [];
      data.forEach(x => all.push(x));
      url = page?.paging?.next || '';
    }

    let campaigns = all.map(c => ({
      id: c.id || '',
      name: c.name || '',
      status: c.status || '',
      objective: c.objective || '',
      daily_budget: c.daily_budget || '',
      lifetime_budget: c.lifetime_budget || '',
      updated_time: c.updated_time || ''
    }));
    if (!includeInactive) {
      campaigns = campaigns.filter(c => String(c.status || '').toUpperCase() === 'ACTIVE');
    }
    campaigns.sort((a, b) => {
      const sa = String(a.status || '').toUpperCase();
      const sb = String(b.status || '').toUpperCase();
      if (sa === 'ACTIVE' && sb !== 'ACTIVE') return -1;
      if (sb === 'ACTIVE' && sa !== 'ACTIVE') return 1;
      return String(a.name || '').localeCompare(String(b.name || ''));
    });

    return {
      success: true,
      account: { adAccountId: resolved.adAccountId, accountName: resolved.accountName || '' },
      campaigns
    };
  } catch (e) {
    throw new Error(`Gagal memuat campaign (${resolved.adAccountId}): ${e.message || String(e)}`);
  }
}

function getCampaignTargetingDetail(payload) {
  initializeApp_();
  const campaignId = String(payload?.campaignId || '').trim();
  if (!campaignId) throw new Error('Campaign ID wajib diisi.');
  const resolved = resolveAccountForCampaignAction_(payload || {});

  const camp = fetchJson_(
    `${APP_CONFIG.META_BASE_URL}/${APP_CONFIG.API_VERSION}/${encodeURIComponent(campaignId)}?fields=id,name,status,objective,updated_time&access_token=${encodeURIComponent(resolved.accessToken)}`
  );

  let url = `${APP_CONFIG.META_BASE_URL}/${APP_CONFIG.API_VERSION}/${encodeURIComponent(campaignId)}/adsets?fields=id,name,status,effective_status,optimization_goal,billing_event,bid_amount,daily_budget,lifetime_budget,targeting&limit=100&access_token=${encodeURIComponent(resolved.accessToken)}`;
  const adsets = [];
  while (url && adsets.length < 500) {
    const page = fetchJson_(url);
    (page?.data || []).forEach(x => adsets.push(x));
    url = page?.paging?.next || '';
  }

  const countries = {};
  const interests = {};
  const devices = {};
  const placements = {};

  adsets.forEach(a => {
    const t = a?.targeting || {};
    (t?.geo_locations?.countries || []).forEach(v => { if (v) countries[String(v)] = true; });
    (t?.interests || []).forEach(v => { if (v?.name) interests[String(v.name)] = true; });
    (t?.flexible_spec || []).forEach(g => {
      (g?.interests || []).forEach(v => { if (v?.name) interests[String(v.name)] = true; });
    });
    (t?.device_platforms || []).forEach(v => { if (v) devices[String(v)] = true; });
    (t?.user_device || []).forEach(v => { if (v) devices[String(v)] = true; });
    (t?.publisher_platforms || []).forEach(v => { if (v) placements[`platform:${v}`] = true; });
    (t?.facebook_positions || []).forEach(v => { if (v) placements[`facebook:${v}`] = true; });
    (t?.instagram_positions || []).forEach(v => { if (v) placements[`instagram:${v}`] = true; });
    (t?.audience_network_positions || []).forEach(v => { if (v) placements[`audience_network:${v}`] = true; });
    (t?.messenger_positions || []).forEach(v => { if (v) placements[`messenger:${v}`] = true; });
  });

  const normalizedAdsets = adsets.map(a => ({
    id: a.id || '',
    name: a.name || '',
    status: a.status || a.effective_status || '',
    budgetType: a.daily_budget ? 'DAILY' : (a.lifetime_budget ? 'LIFETIME' : '-'),
    budgetValue: a.daily_budget || a.lifetime_budget || '',
    optimizationGoal: a.optimization_goal || '',
    billingEvent: a.billing_event || '',
    bidAmount: a.bid_amount || '',
    targeting: a.targeting || {}
  }));

  const ads = fetchAdsForCampaign_(campaignId, resolved.accessToken);

  const adsMetadata = ads.map(a => {
    const c = (a && a.creative) ? a.creative : {};
    const storySpec = c.object_story_spec || {};
    const linkData = storySpec.link_data || {};
    const videoData = storySpec.video_data || {};
    const childAttachments = linkData.child_attachments || [];
    const assetFeed = c.asset_feed_spec || {};
    const titles = []
      .concat(c?.title ? [c.title] : [])
      .concat(linkData?.name ? [linkData.name] : [])
      .concat((assetFeed?.titles || []).map(x => x?.text).filter(Boolean));
    const headlines = []
      .concat(linkData?.caption ? [linkData.caption] : [])
      .concat(videoData?.title ? [videoData.title] : [])
      .concat((assetFeed?.descriptions || []).map(x => x?.text).filter(Boolean));
    const descriptions = []
      .concat(c?.body ? [c.body] : [])
      .concat(linkData?.description ? [linkData.description] : [])
      .concat(videoData?.message ? [videoData.message] : [])
      .concat((assetFeed?.bodies || []).map(x => x?.text).filter(Boolean));
    const links = []
      .concat(c?.link_url ? [c.link_url] : [])
      .concat(linkData?.link ? [linkData.link] : [])
      .concat(videoData?.call_to_action?.value?.link ? [videoData.call_to_action.value.link] : [])
      .concat(childAttachments.map(x => x?.link).filter(Boolean))
      .concat((assetFeed?.link_urls || []).map(x => x?.website_url || x?.display_url).filter(Boolean));
    const ctas = []
      .concat(linkData?.call_to_action?.type ? [linkData.call_to_action.type] : [])
      .concat(videoData?.call_to_action?.type ? [videoData.call_to_action.type] : [])
      .concat((assetFeed?.call_to_action_types || []).filter(Boolean));
    const params = []
      .concat(c?.url_tags ? [c.url_tags] : [])
      .concat(linkData?.link ? [extractQueryParams_(linkData.link)] : [])
      .concat(c?.link_url ? [extractQueryParams_(c.link_url)] : [])
      .filter(Boolean);
    const mediaUrls = []
      .concat(c?.image_url ? [c.image_url] : [])
      .concat(c?.thumbnail_url ? [c.thumbnail_url] : [])
      .concat(linkData?.picture ? [linkData.picture] : [])
      .concat(videoData?.image_url ? [videoData.image_url] : [])
      .concat(childAttachments.map(x => x?.picture).filter(Boolean))
      .concat((assetFeed?.images || []).map(x => x?.url || x?.image_url || '').filter(Boolean))
      .concat((assetFeed?.videos || []).map(x => x?.thumbnail_url || '').filter(Boolean));
    const mediaHashes = []
      .concat(c?.image_hash ? [c.image_hash] : [])
      .concat(linkData?.image_hash ? [linkData.image_hash] : [])
      .concat(childAttachments.map(x => x?.image_hash).filter(Boolean))
      .concat((assetFeed?.images || []).map(x => x?.hash || '').filter(Boolean));
    const hashUrls = resolveImageHashUrls_(resolved.adAccountId, resolved.accessToken, mediaHashes);
    const mergedMedia = uniqueList_(mediaUrls.concat(Object.values(hashUrls || {})));

    return {
      adId: a?.id || '',
      adName: a?.name || '',
      adStatus: a?.status || '',
      adsetId: a?.adset_id || '',
      creativeId: c?.id || '',
      titleList: uniqueList_(titles),
      headlineList: uniqueList_(headlines),
      descriptionList: uniqueList_(descriptions),
      title: uniqueJoin_(titles),
      headline: uniqueJoin_(headlines),
      description: uniqueJoin_(descriptions),
      linkUrl: uniqueJoin_(links),
      parameter: uniqueJoin_(params),
      callToAction: uniqueJoin_(ctas),
      mediaList: mergedMedia,
      mediaPreview: mergedMedia[0] || ''
    };
  });

  if (!payload?.skipLog) {
    logSync_(
      'INFO',
      'VIEW_CAMPAIGN_DETAIL',
      resolved.accountName || '',
      resolved.adAccountId || '',
      `Open detail campaign ${campaignId}`,
      JSON.stringify({ adsets: normalizedAdsets.length, ads: adsMetadata.length })
    );
  }

  return {
    success: true,
    campaign: {
      id: camp.id || campaignId,
      name: camp.name || '',
      status: camp.status || '',
      objective: camp.objective || '',
      updated_time: camp.updated_time || ''
    },
    summary: {
      countries: Object.keys(countries).sort(),
      interests: Object.keys(interests).sort(),
      devices: Object.keys(devices).sort(),
      placements: Object.keys(placements).sort()
    },
    adsets: normalizedAdsets,
    ads: adsMetadata
  };
}

function exportCampaignMetadataToSheet(payload) {
  initializeApp_();
  const campaignId = String(payload?.campaignId || '').trim();
  if (!campaignId) throw new Error('Campaign ID wajib diisi.');
  const detail = getCampaignTargetingDetail(Object.assign({}, payload || {}, { campaignId, skipLog: true }));
  const resolved = resolveAccountForCampaignAction_(payload || {});

  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sh = ss.getSheetByName(APP_CONFIG.EXPORT_SHEET);
  if (!sh) throw new Error(`Sheet ${APP_CONFIG.EXPORT_SHEET} tidak ditemukan.`);

  const adsetMap = {};
  (detail.adsets || []).forEach(a => {
    if (a?.id) adsetMap[String(a.id)] = a;
  });

  const summary = detail.summary || {};
  const now = formatDateTime_(new Date());
  const by = getCurrentUserEmail_();
  const common = [
    now,
    by,
    resolved.accountName || '',
    resolved.adAccountId || '',
    detail.campaign?.id || campaignId,
    detail.campaign?.name || '',
    detail.campaign?.status || '',
    detail.campaign?.objective || '',
    (summary.countries || []).join(' | '),
    (summary.interests || []).join(' | '),
    (summary.devices || []).join(' | '),
    (summary.placements || []).join(' | ')
  ];

  const ads = detail.ads || [];
  const rows = (ads.length ? ads : [{ adId: '', adName: '', adStatus: '', creativeId: '', mediaList: [], titleList: [], headlineList: [], descriptionList: [], linkUrl: '', parameter: '', callToAction: '', adsetId: '' }])
    .map(ad => {
      const adset = adsetMap[String(ad.adsetId || '')] || {};
      return common.concat([
        adset.id || '',
        adset.name || '',
        adset.status || '',
        adset.budgetType || '',
        adset.budgetValue || '',
        adset.optimizationGoal || '',
        adset.billingEvent || '',
        ad.adId || '',
        ad.adName || '',
        ad.adStatus || '',
        ad.creativeId || '',
        (ad.mediaList || []).length,
        (ad.mediaList || []).join(' | '),
        (ad.titleList || []).join(' | '),
        (ad.headlineList || []).join(' | '),
        (ad.descriptionList || []).join(' | '),
        ad.linkUrl || '',
        ad.parameter || '',
        ad.callToAction || ''
      ]);
    });

  sh.getRange(sh.getLastRow() + 1, 1, rows.length, rows[0].length).setValues(rows);

  logSync_(
    'INFO',
    'EXPORT_CAMPAIGN_METADATA',
    resolved.accountName || '',
    resolved.adAccountId || '',
    `Export metadata campaign ${campaignId} ke sheet`,
    JSON.stringify({ rows: rows.length })
  );

  return {
    success: true,
    message: `Export selesai: ${rows.length} baris masuk ke sheet ${APP_CONFIG.EXPORT_SHEET}.`,
    rows: rows.length,
    sheetName: APP_CONFIG.EXPORT_SHEET
  };
}

function updateAdMetadata(payload) {
  initializeApp_();
  const adId = String(payload?.adId || '').trim();
  if (!adId) throw new Error('Ad ID wajib diisi.');
  const resolved = resolveAccountForCampaignAction_(payload || {});

  const adData = fetchJson_(
    `${APP_CONFIG.META_BASE_URL}/${APP_CONFIG.API_VERSION}/${encodeURIComponent(adId)}?fields=id,name,creative{id,name,title,body,link_url,url_tags,object_story_spec,asset_feed_spec}&access_token=${encodeURIComponent(resolved.accessToken)}`
  );
  const currentCreative = adData?.creative || {};
  const storySpec = JSON.parse(JSON.stringify(currentCreative?.object_story_spec || {}));

  if (!Object.keys(storySpec).length) {
    throw new Error('Creative tidak memiliki object_story_spec yang dapat diedit.');
  }

  const titleList = normalizeTextList_(payload?.titleList || payload?.title, 5);
  const headlineList = normalizeTextList_(payload?.headlineList || payload?.headline, 5);
  const descriptionList = normalizeTextList_(payload?.descriptionList || payload?.description, 5);
  const title = titleList[0] || '';
  const headline = headlineList[0] || '';
  const description = descriptionList[0] || '';
  const linkUrl = String(payload?.linkUrl || '').trim();
  const parameter = String(payload?.parameter || '').trim();
  const callToAction = String(payload?.callToAction || '').trim().toUpperCase();

  if (storySpec.link_data) {
    if (title) storySpec.link_data.name = title;
    if (headline) storySpec.link_data.caption = headline;
    if (description) storySpec.link_data.description = description;
    if (linkUrl) storySpec.link_data.link = linkUrl;
    if (callToAction) {
      storySpec.link_data.call_to_action = storySpec.link_data.call_to_action || { value: {} };
      storySpec.link_data.call_to_action.type = callToAction;
      if (linkUrl) {
        storySpec.link_data.call_to_action.value = storySpec.link_data.call_to_action.value || {};
        storySpec.link_data.call_to_action.value.link = linkUrl;
      }
    }
  }

  if (storySpec.video_data) {
    if (title) storySpec.video_data.title = title;
    if (description) storySpec.video_data.message = description;
    if (callToAction) {
      storySpec.video_data.call_to_action = storySpec.video_data.call_to_action || { value: {} };
      storySpec.video_data.call_to_action.type = callToAction;
      if (linkUrl) {
        storySpec.video_data.call_to_action.value = storySpec.video_data.call_to_action.value || {};
        storySpec.video_data.call_to_action.value.link = linkUrl;
      }
    }
  }

  const currentAssetFeed = JSON.parse(JSON.stringify(currentCreative?.asset_feed_spec || {}));
  if (titleList.length || headlineList.length || descriptionList.length) {
    currentAssetFeed.titles = (titleList.length ? titleList : [title]).filter(Boolean).map(text => ({ text }));
    currentAssetFeed.descriptions = (headlineList.length ? headlineList : [headline]).filter(Boolean).map(text => ({ text }));
    currentAssetFeed.bodies = (descriptionList.length ? descriptionList : [description]).filter(Boolean).map(text => ({ text }));
    if (callToAction) currentAssetFeed.call_to_action_types = [callToAction];
  }

  const createPayload = {
    access_token: resolved.accessToken,
    name: `Edited-${adData?.name || adId}-${Date.now()}`,
    object_story_spec: JSON.stringify(storySpec)
  };
  if (parameter) createPayload.url_tags = parameter;
  if (Object.keys(currentAssetFeed || {}).length) createPayload.asset_feed_spec = JSON.stringify(currentAssetFeed);

  const creativeCreateRes = fetchJson_(
    `${APP_CONFIG.META_BASE_URL}/${APP_CONFIG.API_VERSION}/${encodeURIComponent(resolved.adAccountId)}/adcreatives`,
    { method: 'post', payload: createPayload, muteHttpExceptions: true }
  );
  const newCreativeId = String(creativeCreateRes?.id || '').trim();
  if (!newCreativeId) throw new Error('Gagal membuat creative baru.');

  const adUpdateRes = fetchJson_(
    `${APP_CONFIG.META_BASE_URL}/${APP_CONFIG.API_VERSION}/${encodeURIComponent(adId)}`,
    { method: 'post', payload: { access_token: resolved.accessToken, creative: JSON.stringify({ creative_id: newCreativeId }) }, muteHttpExceptions: true }
  );

  logSync_(
    'INFO',
    'UPDATE_AD_METADATA',
    resolved.accountName || '',
    resolved.adAccountId || '',
    `Update ad ${adId} with creative ${newCreativeId}`,
    JSON.stringify({ titleList, headlineList, descriptionList, linkUrl, callToAction, adUpdateRes })
  );

  return {
    success: true,
    message: 'Metadata iklan berhasil diperbarui.',
    adId,
    creativeId: newCreativeId
  };
}

function fetchAdsForCampaign_(campaignId, accessToken) {
  const fieldsPrimary = 'id,name,status,adset_id,creative{id,name,title,body,link_url,url_tags,image_url,thumbnail_url,object_story_spec,asset_feed_spec}';
  const fieldsFallback = 'id,name,status,adset_id,creative{id,name,title,body,image_url,thumbnail_url,object_story_spec,asset_feed_spec}';
  const ads = [];

  const readPaged = (fields) => {
    let url = `${APP_CONFIG.META_BASE_URL}/${APP_CONFIG.API_VERSION}/${encodeURIComponent(campaignId)}/ads?fields=${encodeURIComponent(fields)}&limit=100&access_token=${encodeURIComponent(accessToken)}`;
    while (url && ads.length < 500) {
      const page = fetchJson_(url);
      (page && page.data ? page.data : []).forEach(x => ads.push(x));
      url = (page && page.paging && page.paging.next) ? page.paging.next : '';
    }
  };

  try {
    readPaged(fieldsPrimary);
    return ads;
  } catch (e1) {
    ads.length = 0;
    try {
      readPaged(fieldsFallback);
      return ads;
    } catch (e2) {
      return [];
    }
  }
}

function resolveImageHashUrls_(adAccountId, accessToken, hashes) {
  const out = {};
  const list = uniqueList_(hashes || []).slice(0, 50);
  if (!list.length) return out;
  try {
    const url = `${APP_CONFIG.META_BASE_URL}/${APP_CONFIG.API_VERSION}/${encodeURIComponent(adAccountId)}/adimages?fields=hash,url,url_128,permalink_url&hashes=${encodeURIComponent(JSON.stringify(list))}&access_token=${encodeURIComponent(accessToken)}`;
    const res = fetchJson_(url);
    const data = res && res.data ? res.data : {};
    Object.keys(data || {}).forEach(k => {
      const item = data[k] || {};
      const hash = String(item.hash || k || '').trim();
      const mediaUrl = String(item.url || item.url_128 || item.permalink_url || '').trim();
      if (hash && mediaUrl) out[hash] = mediaUrl;
    });
  } catch (e) {}
  return out;
}

function extractQueryParams_(url) {
  const s = String(url || '');
  const idx = s.indexOf('?');
  return idx >= 0 ? s.slice(idx + 1) : '';
}

function uniqueJoin_(arr) {
  return uniqueList_(arr).join(' | ');
}

function uniqueList_(arr) {
  const map = {};
  (arr || []).forEach(v => {
    const s = String(v || '').trim();
    if (!s) return;
    map[s] = true;
  });
  return Object.keys(map);
}

function normalizeTextList_(value, maxLen) {
  const max = Math.max(Number(maxLen || 5), 1);
  let source = [];
  if (Array.isArray(value)) source = value;
  else source = String(value || '').split('\n');
  return uniqueList_(source).slice(0, max);
}

function setCampaignStatus(payload) {
  initializeApp_();
  const campaignId = String(payload?.campaignId || '').trim();
  if (!campaignId) throw new Error('Campaign ID wajib diisi.');
  const status = String(payload?.status || '').trim().toUpperCase();
  if (status !== 'ACTIVE' && status !== 'PAUSED') {
    throw new Error('Status campaign hanya boleh ACTIVE atau PAUSED.');
  }

  const resolved = resolveAccountForCampaignAction_(payload || {});
  const endpoint = `${APP_CONFIG.META_BASE_URL}/${APP_CONFIG.API_VERSION}/${encodeURIComponent(campaignId)}`;
  const updateRes = fetchJson_(endpoint, {
    method: 'post',
    payload: { access_token: resolved.accessToken, status },
    muteHttpExceptions: true
  });
  const detail = fetchJson_(`${endpoint}?fields=id,name,status,objective,daily_budget,lifetime_budget,updated_time&access_token=${encodeURIComponent(resolved.accessToken)}`);

  logSync_(
    'INFO',
    'SET_CAMPAIGN_STATUS',
    resolved.accountName || '',
    resolved.adAccountId || '',
    `Campaign ${campaignId} status -> ${status}`,
    JSON.stringify({ updateRes })
  );

  return {
    success: true,
    message: `Status campaign ${detail.name || campaignId} menjadi ${detail.status || status}.`,
    campaign: detail
  };
}

function resolveAccountForCampaignAction_(payload) {
  const adAccountId = normalizeAdAccountId_(payload?.adAccountId || '');
  const accountName = String(payload?.accountName || '').trim();
  const tokenDirect = String(payload?.accessToken || '').trim();
  if (tokenDirect) {
    return { adAccountId, accountName, accessToken: tokenDirect };
  }

  const accounts = getAccounts_();
  let account = null;
  if (adAccountId) {
    account = accounts.find(a => normalizeAdAccountId_(a.adAccountId) === adAccountId);
  }
  if (!account && accountName) {
    account = accounts.find(a => String(a.accountName || '').trim() === accountName);
  }
  if (!account) {
    throw new Error('Akun untuk aksi campaign tidak ditemukan di SETTINGS_META.');
  }
  if (!account.accessToken) {
    throw new Error('Access token akun tidak tersedia di SETTINGS_META.');
  }

  return {
    adAccountId: normalizeAdAccountId_(account.adAccountId),
    accountName: account.accountName,
    accessToken: account.accessToken
  };
}

/* =========================
 * AUTO CHECK CONNECTION
 * ========================= */
function hasHourlyTrigger_() {
  return ScriptApp.getProjectTriggers().some(t => t.getHandlerFunction() === 'checkAllConnectionsHourly');
}

function ensureHourlyConnectionTrigger_() {
  if (!hasHourlyTrigger_()) {
    ScriptApp.newTrigger('checkAllConnectionsHourly')
      .timeBased()
      .everyHours(1)
      .create();
  }
}

function installHourlyConnectionTrigger() {
  ensureHourlyConnectionTrigger_();
  return {
    success: true,
    message: 'Auto check connection tiap 1 jam sudah aktif.'
  };
}

function hasHourlyTodaySyncTrigger_() {
  return ScriptApp.getProjectTriggers().some(t => t.getHandlerFunction() === 'syncTodayHourly');
}

function ensureHourlyTodaySyncTrigger_() {
  if (!hasHourlyTodaySyncTrigger_()) {
    ScriptApp.newTrigger('syncTodayHourly')
      .timeBased()
      .everyHours(1)
      .create();
  }
}

function installHourlyTodaySyncTrigger() {
  ensureHourlyTodaySyncTrigger_();
  return {
    success: true,
    message: 'Auto sync data (hari ini) tiap 1 jam sudah aktif.'
  };
}

function checkAllConnectionsHourly() {
  initializeApp_();
  runAllAccountConnectionChecks_({ sourceAction: 'AUTO_CHECK_CONNECTION', sendAlertEmail: true });
}

function runAllAccountConnectionChecks_(options) {
  const opts = options || {};
  const action = String(opts.sourceAction || 'AUTO_CHECK_CONNECTION');
  const sendAlertEmail = !!opts.sendAlertEmail;
  const accounts = getAccounts_().filter(a => a.active && a.adAccountId && a.accessToken);
  const details = [];
  let connected = 0;
  let failed = 0;

  accounts.forEach(acc => {
    try {
      const adAccountId = normalizeAdAccountId_(acc.adAccountId);
      const url = `${APP_CONFIG.META_BASE_URL}/${APP_CONFIG.API_VERSION}/${encodeURIComponent(adAccountId)}?fields=id,name,account_status&access_token=${encodeURIComponent(acc.accessToken)}`;
      const res = fetchJson_(url);
      const statusText = `Connected • account_status: ${res.account_status ?? '-'}`;
      updateAccountTestStatus_(acc.adAccountId, 'CONNECTED', new Date(), statusText);
      logSync_('INFO', action, acc.accountName, acc.adAccountId, 'Connection check success', '');
      connected++;
      details.push({ accountName: acc.accountName || adAccountId, adAccountId, success: true, accountStatus: res.account_status ?? '-' });
    } catch (err) {
      updateAccountTestStatus_(acc.adAccountId, 'FAILED', new Date(), err.message);
      logSync_('ERROR', action, acc.accountName, acc.adAccountId, err.message, '');
      failed++;
      details.push({ accountName: acc.accountName || acc.adAccountId, adAccountId: normalizeAdAccountId_(acc.adAccountId), success: false, error: err.message || String(err) });
    }
  });

  if (sendAlertEmail) {
    try {
      sendAccountAlertsEmail({ to: APP_CONFIG.ALERT_EMAIL_TO });
    } catch (e) {
      logSync_('ERROR', 'SEND_ALERT_EMAIL_AUTO', '', '', e.message || String(e), '');
    }
  }

  return {
    total: accounts.length,
    connected,
    failed,
    details
  };
}

/* =========================
 * SYNC DATA
 * ========================= */
function syncMetaData(payload) {
  initializeApp_();

  const startDate = payload && payload.startDate ? payload.startDate : formatDate_(shiftDate_(new Date(), -7));
  const endDate = payload && payload.endDate ? payload.endDate : formatDate_(new Date());
  const incremental = !!payload?.incremental;
  const skipSort = !!payload?.skipSort;
  const skipFilters = !!payload?.skipFilters;
  logSync_('INFO', 'SYNC_REQUEST', '', '', `Sync dimulai ${startDate} s/d ${endDate}`, JSON.stringify({ incremental }));

  const activeAccounts = getAccounts_().filter(x => x.active);
  if (!activeAccounts.length) throw new Error('Tidak ada akun aktif.');

  let fetched = 0, inserted = 0, updated = 0;
  const summary = [];

  activeAccounts.forEach(acc => {
    const result = syncSingleAccount_(acc, startDate, endDate, { incremental });
    fetched += result.fetched;
    inserted += result.inserted;
    updated += result.updated;
    summary.push(result);
  });

  if (!skipSort) sortRawDataNewest_();
  logSync_('INFO', 'SYNC_FINISH', '', '', `Sync selesai fetched ${fetched}, inserted ${inserted}, updated ${updated}`, JSON.stringify({ startDate, endDate, incremental }));

  return {
    success: true,
    message: 'Sync selesai',
    totals: { fetched, inserted, updated },
    summary,
    filters: skipFilters ? null : getAvailableFilters_()
  };
}

function syncTodayHourly() {
  const today = formatDate_(new Date());
  const start = formatDate_(shiftDate_(new Date(), -APP_CONFIG.MUTABLE_LOOKBACK_DAYS));
  try {
    syncMetaData({ startDate: start, endDate: today, incremental: true });
  } catch (e) {
    logSync_('ERROR', 'AUTO_SYNC_TODAY', '', '', e.message || String(e), '');
  }
}

function runSyncFromSheetDefault_() {
  const result = syncMetaData({
    startDate: formatDate_(shiftDate_(new Date(), -7)),
    endDate: formatDate_(new Date())
  });

  SpreadsheetApp.getUi().alert(
    `Sync selesai\nFetched: ${result.totals.fetched}\nInserted: ${result.totals.inserted}\nUpdated: ${result.totals.updated}`
  );
}

function syncSingleAccount_(acc, userStartDate, userEndDate, options) {
  const accountName = acc.accountName;
  const adAccountId = normalizeAdAccountId_(acc.adAccountId);
  const accessToken = acc.accessToken;

  const opts = options || {};
  const incremental = !!opts.incremental;
  const fetchStartDate = userStartDate;
  const fetchEndDate = userEndDate;
  const plannedRanges = buildFetchRangesForAccount_(adAccountId, fetchStartDate, fetchEndDate, {
    mutableLookbackDays: APP_CONFIG.MUTABLE_LOOKBACK_DAYS
  });

  if (!plannedRanges.length) {
    logSync_(
      'INFO',
      'SYNC_ACCOUNT_SKIPPED',
      accountName,
      adAccountId,
      'Semua tanggal sudah ada di sheet (non-mutable), skip fetch API',
      JSON.stringify({ fetchStartDate, fetchEndDate })
    );
    return {
      accountName,
      adAccountId,
      fetchStartDate,
      fetchEndDate,
      fetched: 0,
      inserted: 0,
      updated: 0,
      skipped: true
    };
  }

  let insights = [];
  plannedRanges.forEach(r => {
    const part = fetchInsightsByDate_(adAccountId, accessToken, r.since, r.until);
    insights = insights.concat(part || []);
  });

  const rows = insights.map(item => {
    const campaignName = String(item.campaign_name || '');
    const siteName = extractSiteName_(campaignName);
    const totalBudget = getCampaignBudget_(item.campaign_id, accessToken);
    const totalSpend = toNumber_(item.spend);
    const totalClick = toNumber_(item.inline_link_clicks || item.clicks || 0);
    const cpr = totalClick > 0 ? totalSpend / totalClick : 0;
    const rowDate = item.date_start || item.date_stop || plannedRanges[0].since;
    const uniqueKey = buildUniqueKey_(adAccountId, rowDate, campaignName);

    return {
      date: rowDate,
      accountName,
      adAccountId,
      campaignName,
      siteName,
      totalBudget,
      totalSpend,
      cpr,
      totalClick,
      updatedAt: new Date(),
      uniqueKey
    };
  });

  const result = upsertRawRows_(rows);

  logSync_(
    'INFO',
    'SYNC_ACCOUNT',
    accountName,
    adAccountId,
    `Fetched ${rows.length}, inserted ${result.inserted}, updated ${result.updated}`,
    JSON.stringify({ fetchStartDate, fetchEndDate, ranges: plannedRanges, incremental })
  );

  return {
    accountName,
    adAccountId,
    fetchStartDate,
    fetchEndDate,
    fetched: rows.length,
    inserted: result.inserted,
    updated: result.updated
  };
}

function buildFetchRangesForAccount_(adAccountId, startDate, endDate, options) {
  const opts = options || {};
  const mutableLookbackDays = Math.max(Number(opts.mutableLookbackDays || 2), 0);
  const mutableStart = formatDate_(shiftDate_(new Date(), -mutableLookbackDays));
  const covered = getStoredDateSetForAccountRange_(adAccountId, startDate, endDate);
  const dates = listDateRange_(startDate, endDate);
  const toFetch = dates.filter(d => d >= mutableStart || !covered[d]);
  return buildDateRanges_(toFetch);
}

function getStoredDateSetForAccountRange_(adAccountId, startDate, endDate) {
  const cache = CacheService.getDocumentCache();
  const key = `COVER_${normalizeAdAccountId_(adAccountId)}_${startDate}_${endDate}`;
  const cached = cache.get(key);
  if (cached) {
    try {
      const parsed = JSON.parse(cached);
      if (parsed && typeof parsed === 'object') return parsed;
    } catch (e) {}
  }

  const sh = ensureDataSheetReady_();
  const out = {};
  if (!sh || sh.getLastRow() < 2) return out;
  const rows = sh.getRange(2, 1, sh.getLastRow() - 1, 3).getValues();
  const idNorm = normalizeAdAccountId_(adAccountId);
  rows.forEach(r => {
    const d = normalizeDateForCompare_(r[0]);
    const id = normalizeAdAccountId_(r[2]);
    if (!d || id !== idNorm) return;
    if (d < startDate || d > endDate) return;
    out[d] = true;
  });
  try { cache.put(key, JSON.stringify(out), 300); } catch (e) {}
  return out;
}

function listDateRange_(startDate, endDate) {
  const out = [];
  let d = parseDateISO_(startDate);
  const end = parseDateISO_(endDate);
  while (formatDate_(d) <= formatDate_(end)) {
    out.push(formatDate_(d));
    d = shiftDate_(d, 1);
  }
  return out;
}

function buildDateRanges_(dates) {
  if (!dates || !dates.length) return [];
  const sorted = dates.slice().sort();
  const ranges = [];
  let start = sorted[0];
  let prev = sorted[0];
  for (let i = 1; i < sorted.length; i++) {
    const cur = sorted[i];
    const nextFromPrev = formatDate_(shiftDate_(parseDateISO_(prev), 1));
    if (cur !== nextFromPrev) {
      ranges.push({ since: start, until: prev });
      start = cur;
    }
    prev = cur;
  }
  ranges.push({ since: start, until: prev });
  return ranges;
}

function getMaxStoredDateForAccount_(adAccountId) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sh = ss.getSheetByName(APP_CONFIG.DATA_SHEET);
  if (!sh || sh.getLastRow() < 2) return '';

  const rows = sh.getRange(2, 1, sh.getLastRow() - 1, 3).getValues();
  let maxDate = '';
  const idNorm = normalizeAdAccountId_(adAccountId);

  rows.forEach(r => {
    const d = normalizeDateForCompare_(r[0]);
    const id = normalizeAdAccountId_(r[2]);
    if (id !== idNorm) return;
    if (!maxDate || d > maxDate) maxDate = d;
  });

  return maxDate;
}

function parseDateISO_(s) {
  const m = String(s || '').trim().match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!m) return new Date(s);
  return new Date(Number(m[1]), Number(m[2]) - 1, Number(m[3]));
}

function fetchInsightsByDate_(adAccountId, accessToken, since, until) {
  let allRows = [];
  let after = '';

  while (true) {
    let url =
      `${APP_CONFIG.META_BASE_URL}/${APP_CONFIG.API_VERSION}/${encodeURIComponent(adAccountId)}/insights` +
      `?level=campaign` +
      `&time_increment=1` +
      `&limit=500` +
      `&fields=campaign_id,campaign_name,spend,inline_link_clicks,clicks,date_start,date_stop` +
      `&time_range=${encodeURIComponent(JSON.stringify({ since, until }))}` +
      `&access_token=${encodeURIComponent(accessToken)}`;

    if (after) url += `&after=${encodeURIComponent(after)}`;

    const json = fetchJson_(url);
    allRows = allRows.concat(json.data || []);

    const nextAfter = json.paging && json.paging.cursors && json.paging.cursors.after
      ? json.paging.cursors.after
      : '';

    if (!nextAfter) break;
    after = nextAfter;
  }

  return allRows;
}

function getCampaignBudget_(campaignId, accessToken) {
  if (!campaignId) return 0;

  const cache = CacheService.getDocumentCache();
  const key = `BUDGET_${campaignId}`;
  const cached = cache.get(key);
  if (cached !== null) return Number(cached);

  try {
    const url =
      `${APP_CONFIG.META_BASE_URL}/${APP_CONFIG.API_VERSION}/${encodeURIComponent(campaignId)}` +
      `?fields=daily_budget,lifetime_budget&access_token=${encodeURIComponent(accessToken)}`;

    const json = fetchJson_(url);
    const val = toNumber_(json.daily_budget || json.lifetime_budget || 0);
    cache.put(key, String(val), 21600);
    return val;
  } catch (e) {
    return 0;
  }
}

/* =========================
 * DASHBOARD DATA
 * ========================= */
function getDashboardData(filters) {
  initializeApp_();

  const sh = ensureDataSheetReady_();
  const startDate = String(filters?.startDate || '').trim();
  const endDate = String(filters?.endDate || '').trim();
  const roiToday = getRoiTodayData_();

  if (!sh || sh.getLastRow() < 2) {
    return {
      success: true,
      rows: [],
      summary: { totalSpend: 0, totalBudget: 0, totalClicks: 0, avgCpr: 0, countRows: 0 },
      charts: { byDate: [], bySite: [] },
      roiToday
    };
  }

  const raw = sh.getRange(2, 1, sh.getLastRow() - 1, 11).getValues();
  const site = String(filters?.site || '').trim();
  const accountName = String(filters?.accountName || '').trim();

  const rows = raw.map(r => {
    const campaignName = String(r[3] || '');
    const computedSiteName = extractSiteName_(campaignName) || String(r[4] || '');
    return {
      date: normalizeDateForCompare_(r[0]),
      accountName: String(r[1] || ''),
      adAccountId: String(r[2] || ''),
      campaignName,
      siteName: computedSiteName,
      totalBudget: toNumber_(r[5]),
      totalSpend: toNumber_(r[6]),
      cpr: toNumber_(r[7]),
      totalClick: toNumber_(r[8]),
      updatedAt: String(r[9] || ''),
      uniqueKey: String(r[10] || '')
    };
  });

  const filtered = rows.filter(r => {
    if (startDate && r.date < startDate) return false;
    if (endDate && r.date > endDate) return false;
    if (site && r.siteName !== site) return false;
    if (accountName && r.accountName !== accountName) return false;
    return true;
  });

  filtered.sort((a, b) => {
    if (a.date < b.date) return 1;
    if (a.date > b.date) return -1;
    return 0;
  });

  const totalSpend = filtered.reduce((n, r) => n + r.totalSpend, 0);
  const totalBudget = filtered.reduce((n, r) => n + r.totalBudget, 0);
  const totalClicks = filtered.reduce((n, r) => n + r.totalClick, 0);
  const avgCpr = totalClicks > 0 ? totalSpend / totalClicks : 0;

  const byDateMap = {};
  const bySiteMap = {};

  filtered.forEach(r => {
    if (!byDateMap[r.date]) byDateMap[r.date] = { date: r.date, spend: 0, clicks: 0 };
    byDateMap[r.date].spend += r.totalSpend;
    byDateMap[r.date].clicks += r.totalClick;

    if (!bySiteMap[r.siteName]) bySiteMap[r.siteName] = { site: r.siteName, spend: 0, clicks: 0 };
    bySiteMap[r.siteName].spend += r.totalSpend;
    bySiteMap[r.siteName].clicks += r.totalClick;
  });

  const byDate = Object.values(byDateMap).sort((a, b) => a.date.localeCompare(b.date));
  const bySite = Object.values(bySiteMap).sort((a, b) => b.spend - a.spend).slice(0, 15);

  return {
    success: true,
    rows: filtered,
    summary: {
      totalSpend,
      totalBudget,
      totalClicks,
      avgCpr,
      countRows: filtered.length
    },
    charts: {
      byDate,
      bySite
    },
    roiToday
  };
}

function getRoiTodayData(payload) {
  initializeApp_();
  const startDate = String(payload?.startDate || '').trim();
  const endDate = String(payload?.endDate || '').trim();
  return {
    success: true,
    roiToday: getRoiTodayData_(startDate, endDate, true)
  };
}

function getRoiTodayData_(startDate, endDate, writeRange) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sh = ss.getSheetByName(APP_CONFIG.ROI_SHEET);
  if (!sh) {
    return {
      summary: { totalModal: 0, totalRevenue: 0, roiOverall: 0 },
      rows: [],
      range: { startDate: startDate || '', endDate: endDate || '' },
      sheetName: APP_CONFIG.ROI_SHEET
    };
  }

  if (writeRange && startDate) sh.getRange('B1').setValue(startDate);
  if (writeRange && endDate) sh.getRange('B2').setValue(endDate);

  const totalModal = toNumber_(sh.getRange('B4').getValue());
  const totalRevenue = toNumber_(sh.getRange('B5').getValue());
  const roiOverall = toNumber_(sh.getRange('B6').getValue());

  const lastRow = sh.getLastRow();
  const rows = [];
  if (lastRow >= 9) {
    const values = sh.getRange(9, 1, lastRow - 8, 5).getValues();
    values.forEach(r => {
      const site = String(r[0] || '').trim();
      const modalIklan = toNumber_(r[1]);
      const hasAny = site || String(r[1] || '').trim() || String(r[2] || '').trim() || String(r[3] || '').trim() || String(r[4] || '').trim();
      if (!hasAny) return;
      if (modalIklan <= 0) return;
      rows.push({
        site,
        modalIklan,
        revenue: toNumber_(r[2]),
        fixRevenue: toNumber_(r[3]),
        roi: toNumber_(r[4])
      });
    });
  }
  rows.sort((a, b) => (Number(b.roi || 0) - Number(a.roi || 0)));

  return {
    summary: { totalModal, totalRevenue, roiOverall },
    rows,
    range: {
      startDate: String(sh.getRange('B1').getDisplayValue() || ''),
      endDate: String(sh.getRange('B2').getDisplayValue() || '')
    },
    sheetName: APP_CONFIG.ROI_SHEET
  };
}

function getAvailableFilters_() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const data = ss.getSheetByName(APP_CONFIG.DATA_SHEET);
  const settings = ss.getSheetByName(APP_CONFIG.SETTINGS_SHEET);

  const sites = [];
  const accounts = [];

  if (data && data.getLastRow() >= 2) {
    const vals = data.getRange(2, 1, data.getLastRow() - 1, 5).getValues();
    const siteSet = {};
    vals.forEach(r => {
      const campaignName = String(r[3] || '').trim();
      const site = extractSiteName_(campaignName);
      if (site) siteSet[site] = true;
    });
    Object.keys(siteSet).sort().forEach(v => sites.push(v));
  }

  if (settings && settings.getLastRow() >= 2) {
    const headerRow = findSettingsHeaderRow_(settings);
    const startRow = headerRow + 1;
    if (settings.getLastRow() < startRow) return { sites, accounts };

    const vals = settings.getRange(startRow, 1, settings.getLastRow() - startRow + 1, 2).getValues();
    const accSet = {};
    vals.forEach(r => {
      const acc = String(r[1] || '').trim();
      if (acc) accSet[acc] = true;
    });
    Object.keys(accSet).sort().forEach(v => accounts.push(v));
  }

  return { sites, accounts };
}

/* =========================
 * UPSERT DATA
 * ========================= */
function upsertRawRows_(rows) {
  if (!rows.length) return { inserted: 0, updated: 0 };

  const sh = ensureDataSheetReady_();

  const existingMap = {};
  if (sh.getLastRow() >= 2) {
    const keys = sh.getRange(2, 11, sh.getLastRow() - 1, 1).getValues().flat();
    keys.forEach((k, i) => {
      if (k) existingMap[String(k)] = i + 2;
    });
  }

  let inserted = 0;
  let updated = 0;
  const appendRows = [];

  rows.forEach(r => {
    const values = [[
      r.date,
      r.accountName,
      r.adAccountId,
      r.campaignName,
      r.siteName,
      r.totalBudget,
      r.totalSpend,
      r.cpr,
      r.totalClick,
      formatDateTime_(r.updatedAt),
      r.uniqueKey
    ]];

    if (existingMap[r.uniqueKey]) {
      sh.getRange(existingMap[r.uniqueKey], 1, 1, 11).setValues(values);
      updated++;
    } else {
      appendRows.push(values[0]);
      inserted++;
    }
  });

  if (appendRows.length) {
    sh.getRange(sh.getLastRow() + 1, 1, appendRows.length, 11).setValues(appendRows);
  }

  formatSheets_();
  return { inserted, updated };
}

function sortRawDataNewest_() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sh = ss.getSheetByName(APP_CONFIG.DATA_SHEET);
  if (!sh || sh.getLastRow() < 3) return;

  sh.getRange(2, 1, sh.getLastRow() - 1, 11).sort([
    { column: 1, ascending: false },
    { column: 2, ascending: true },
    { column: 4, ascending: true }
  ]);
}

/* =========================
 * HELPERS
 * ========================= */
function normalizeAdAccountId_(val) {
  const s = String(val || '').trim();
  if (!s) return '';
  return s.startsWith('act_') ? s : `act_${s.replace(/^act_/, '')}`;
}

function extractSiteName_(campaignName) {
  const s = String(campaignName || '').trim();
  if (!s) return '';

  const base = s.split('_')[0] || s;

  // Nama site = kata sebelum titik pertama + kata sesudah titik pertama
  // Contoh: cinder.cipicipichips.ADX_5Negara -> cinder.cipicipichips
  const parts = base.split('.');
  if (parts.length >= 2) return parts[0] + '.' + parts[1];

  return base;
}

function buildUniqueKey_(adAccountId, date, campaignName) {
  return [normalizeAdAccountId_(adAccountId), date, campaignName].join('||');
}

function normalizeDateForCompare_(value) {
  if (Object.prototype.toString.call(value) === '[object Date]' && !isNaN(value)) {
    return Utilities.formatDate(value, Session.getScriptTimeZone(), 'yyyy-MM-dd');
  }

  const s = String(value || '').trim();
  if (/^\d{4}-\d{2}-\d{2}$/.test(s)) return s;

  const d = new Date(s);
  if (!isNaN(d.getTime())) {
    return Utilities.formatDate(d, Session.getScriptTimeZone(), 'yyyy-MM-dd');
  }

  return s;
}

function fetchJson_(url, options) {
  const res = UrlFetchApp.fetch(url, Object.assign({
    method: 'get',
    muteHttpExceptions: true
  }, options || {}));

  const code = res.getResponseCode();
  const text = res.getContentText();

  if (code < 200 || code >= 300) {
    throw new Error(`HTTP ${code}: ${text}`);
  }

  const json = JSON.parse(text);
  if (json.error) throw new Error(json.error.message || JSON.stringify(json.error));
  return json;
}

function formatDate_(d) {
  return Utilities.formatDate(new Date(d), Session.getScriptTimeZone(), 'yyyy-MM-dd');
}

function formatDateTime_(d) {
  return Utilities.formatDate(new Date(d), Session.getScriptTimeZone(), 'yyyy-MM-dd HH:mm:ss');
}

function shiftDate_(date, days) {
  const d = new Date(date);
  d.setDate(d.getDate() + days);
  return d;
}

function toNumber_(v) {
  if (v === null || v === undefined || v === '') return 0;
  const n = Number(v);
  return isNaN(n) ? 0 : n;
}

function maskToken_(token) {
  const s = String(token || '');
  if (s.length <= 10) return s;
  return s.slice(0, 6) + '...' + s.slice(-4);
}

function logSync_(level, action, accountName, adAccountId, message, extra) {
  const row = [
    formatDateTime_(new Date()),
    getCurrentUserEmail_(),
    level || '',
    action || '',
    accountName || '',
    adAccountId || '',
    message || '',
    extra || ''
  ];
  try {
    const sh = ensureLogSheetReady_();
    sh.appendRow(row);
  } catch (e1) {
    try {
      const sh = ensureLogSheetReady_();
      sh.appendRow(row);
    } catch (e2) {}
  }
  try {
    appendTrackerEvent_({
      timestamp: row[0],
      userEmail: row[1],
      level: row[2],
      action: row[3],
      message: row[6],
      extra: row[7],
      source: 'SERVER'
    });
  } catch (e3) {}
}

function getCurrentUserEmail_() {
  const email = String(Session.getActiveUser().getEmail() || '').trim();
  return email || 'unknown@anonymous';
}

function ensureDataSheetReady_() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  let sh = ss.getSheetByName(APP_CONFIG.DATA_SHEET);
  if (!sh) sh = ss.insertSheet(APP_CONFIG.DATA_SHEET);
  const expected = [[
    'Date',
    'Account Name',
    'Ad Account ID',
    'Campaign Name',
    'Site Name',
    'Total Budget',
    'Total Spend',
    'CPR',
    'Total Click',
    'Updated At',
    'Unique Key'
  ]];
  const h = sh.getRange(1, 1, 1, 11).getValues()[0].map(v => String(v || '').trim().toUpperCase());
  if (h.every(x => !x) || h[0] === 'DATE') sh.getRange(1, 1, 1, 11).setValues(expected);
  sh.setFrozenRows(1);
  return sh;
}

function ensureLogSheetReady_() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  let sh = ss.getSheetByName(APP_CONFIG.LOG_SHEET);
  if (!sh) sh = ss.insertSheet(APP_CONFIG.LOG_SHEET);
  const expected = [[
    'Timestamp',
    'User Email',
    'Level',
    'Action',
    'Account Name',
    'Ad Account ID',
    'Message',
    'Extra'
  ]];
  const h = sh.getRange(1, 1, 1, 8).getValues()[0].map(v => String(v || '').trim().toUpperCase());
  if (h.every(x => !x) || isLikelyLogHeader_(h)) sh.getRange(1, 1, 1, 8).setValues(expected);
  sh.setFrozenRows(1);
  return sh;
}

function isLikelyLogHeader_(h) {
  const row = (h || []).map(v => String(v || '').trim().toUpperCase());
  if (!row.length) return false;
  return row[0] === 'TIMESTAMP'
    || row[0] === 'TISTAMPY'
    || (row[0].indexOf('TIME') >= 0 && row[1].indexOf('USER') >= 0)
    || (row[2] === 'LEVEL' && row[3] === 'ACTION');
}

function getTrackerState_() {
  const props = PropertiesService.getDocumentProperties();
  const raw = String(props.getProperty(PROP_KEYS.TRACKER_STREAM) || '').trim();
  if (!raw) return { seq: 0, events: [] };
  try {
    const parsed = JSON.parse(raw);
    return {
      seq: Number(parsed?.seq || 0),
      events: Array.isArray(parsed?.events) ? parsed.events : []
    };
  } catch (e) {
    return { seq: 0, events: [] };
  }
}

function saveTrackerState_(state) {
  const props = PropertiesService.getDocumentProperties();
  props.setProperty(PROP_KEYS.TRACKER_STREAM, JSON.stringify(state || { seq: 0, events: [] }));
}

function appendTrackerEvent_(entry) {
  const state = getTrackerState_();
  const id = Number(state.seq || 0) + 1;
  const event = {
    id,
    timestamp: String(entry?.timestamp || formatDateTime_(new Date())),
    userEmail: String(entry?.userEmail || getCurrentUserEmail_()),
    level: String(entry?.level || 'INFO').toUpperCase(),
    action: String(entry?.action || 'EVENT'),
    message: String(entry?.message || ''),
    extra: String(entry?.extra || ''),
    source: String(entry?.source || 'SERVER')
  };
  state.seq = id;
  state.events = (state.events || []).concat([event]).slice(-2000);
  saveTrackerState_(state);
  return event;
}
