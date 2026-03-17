"use strict";

const mysql = require("mysql2/promise");
const axios = require("axios");

// ================= CONFIG =================
const API_KEY          = "4827f87b-0e70-45ac-b822-92e7b4d6a291";
const REFRESH_INTERVAL = 60_000; // 1 minute
const PARALLEL_LIMIT   = 10;
const MAX_RETRY        = 3;

// ================= MYSQL =================
const pool = mysql.createPool({
    host:               "82.25.121.2",
    user:               "u527886566_credifyy",
    password:           "VAKILr@6762",
    database:           "u527886566_credifyy",
    waitForConnections: true,
    connectionLimit:    20,
});

// ================= AXIOS =================
const axiosInstance = axios.create({
    timeout: 20_000,
    headers: {
        apikey:         API_KEY,
        "Content-Type": "application/json",
    },
});

// ================= DATE FORMAT =================
function formatDateTime(date) {
    const p = n => String(n).padStart(2, "0");
    return `${date.getFullYear()}-${p(date.getMonth()+1)}-${p(date.getDate())} ` +
           `${p(date.getHours())}:${p(date.getMinutes())}:${p(date.getSeconds())}`;
}

// ================= DOB FIX =================
function formatDOB(dob) {
    if (!dob) return null;

    // Already a valid YYYY-MM-DD string
    if (typeof dob === "string" && /^\d{4}-\d{2}-\d{2}$/.test(dob)) return dob;

    // Try parsing a DD/MM/YYYY or D/M/YYYY string (common in Indian data)
    if (typeof dob === "string") {
        const parts = dob.split(/[\/\-\.]/);
        if (parts.length === 3) {
            // Detect DD/MM/YYYY vs YYYY/MM/DD by first segment length
            let year, month, day;
            if (parts[0].length === 4) {
                [year, month, day] = parts;          // YYYY-MM-DD variant
            } else {
                [day, month, year] = parts;           // DD/MM/YYYY variant
            }
            const p = n => String(n).padStart(2, "0");
            const y = Number(year), m = Number(month), d = Number(day);
            if (y >= 1900 && y <= 2100 && m >= 1 && m <= 12 && d >= 1 && d <= 31) {
                return `${y}-${p(m)}-${p(d)}`;
            }
        }
    }

    // Date object
    if (dob instanceof Date && !isNaN(dob)) {
        const year = dob.getFullYear();
        if (year < 1900 || year > 2100) return null;
        const p = n => String(n).padStart(2, "0");
        return `${year}-${p(dob.getMonth()+1)}-${p(dob.getDate())}`;
    }

    return null;
}

// ================= DASHBOARD =================
// FIX: wrap in try/catch so a DB hiccup never crashes the dashboard
async function showConsoleDashboard(lastBatchProcessed) {
    try {
        const [[row]] = await pool.query(`
            SELECT
                COUNT(*)                    AS total,
                SUM(status='pending')       AS pending,
                SUM(status='completed')     AS completed,
                SUM(status='failed')        AS failed,
                SUM(status='failed_final')  AS failed_final,
                SUM(status='ineligible')    AS ineligible,
                SUM(status='created')       AS created
            FROM bulk_leads
        `);
        console.clear();
        console.log("=========================================");
        console.log("        🚀 LEAD ENGINE CONSOLE");
        console.log("=========================================");
        console.log("Total Leads     :", row.total        ?? 0);
        console.log("Pending         :", row.pending       ?? 0);
        console.log("Completed       :", row.completed     ?? 0);
        console.log("Created         :", row.created       ?? 0);
        console.log("Failed (retry)  :", row.failed        ?? 0);
        console.log("Failed (final)  :", row.failed_final  ?? 0);
        console.log("Ineligible      :", row.ineligible    ?? 0);
        console.log("-----------------------------------------");
        console.log("Processed Batch :", lastBatchProcessed);
        console.log("Last Update     :", new Date().toLocaleString());
        console.log("=========================================\n");
    } catch (err) {
        console.error("❌ Dashboard query failed:", err.message);
    }
}

// ================= API HELPERS =================

// FIX: treat any truthy success value ("true", true, 1) as eligible
async function checkDedupe(mobileNumber) {
    const res = await axiosInstance.post(
        "https://l.creditlinks.in:8000/api/partner/dedupe",
        { mobileNumber }
    );
    const s = res.data?.success;
    return s === true || s === "true" || s === 1;
}

// FIX: same truthy check for success; propagate structured error on failure
async function createLead(payload) {
    const res = await axiosInstance.post(
        "https://l.creditlinks.in:8000/api/v2/partner/create-lead",
        payload
    );
    const s = res.data?.success;
    if (s === true || s === "true" || s === 1) return res.data;
    throw new Error(JSON.stringify(res.data));
}

async function getSummary(leadId) {
    const res = await axiosInstance.get(
        `https://l.creditlinks.in:8000/api/partner/get-summary/${leadId}`
    );
    return res.data;
}

// ================= CORE PROCESS =================
async function processLead(lead) {
    // FIX: check retry_count BEFORE doing any API work
    if ((lead.retry_count ?? 0) >= MAX_RETRY) {
        await pool.query(
            "UPDATE bulk_leads SET status='failed_final', message='Max retries reached' WHERE id=?",
            [lead.id]
        );
        return;
    }

    try {
        // ── Validate mobile ──────────────────────────────────────────────────
        const cleanMobile = (lead.mobileNumber ?? "").replace(/\D/g, "");
        if (cleanMobile.length < 10) {
            await pool.query(
                "UPDATE bulk_leads SET status='failed', message='Invalid mobile number' WHERE id=?",
                [lead.id]
            );
            return;
        }

        // ── Validate DOB ─────────────────────────────────────────────────────
        const formattedDOB = formatDOB(lead.dob);
        if (!formattedDOB) {
            await pool.query(
                "UPDATE bulk_leads SET status='failed', message='Invalid or missing DOB' WHERE id=?",
                [lead.id]
            );
            return;
        }

        // ── Dedupe check ─────────────────────────────────────────────────────
        const eligible = await checkDedupe(cleanMobile);
        if (!eligible) {
            await pool.query(
                "UPDATE bulk_leads SET status='ineligible', message='Failed dedupe check' WHERE id=?",
                [lead.id]
            );
            return;
        }

        // ── Build payload ────────────────────────────────────────────────────
        // FIX: monthlyIncome defaults to 0 if NaN instead of sending NaN to API
        const monthlyIncome = Number(lead.monthlyIncome);
        const payload = {
            mobileNumber:        cleanMobile,
            firstName:           lead.firstName           ?? null,
            lastName:            lead.lastName            ?? null,
            pan:                 lead.pan?.toUpperCase()  ?? null,
            dob:                 formattedDOB,
            email:               lead.email               ?? null,
            pincode:             lead.pincode             ?? null,
            monthlyIncome:       isNaN(monthlyIncome) ? 0 : monthlyIncome,
            consumerConsentDate: formatDateTime(new Date()),
            consumerConsentIp:   lead.consumerConsentIp   ?? null,
            employmentStatus:    lead.employmentStatus    ?? null,
            employerName:        lead.employerName        ?? null,
            officePincode:       lead.officePincode       ?? null,
        };

        // ── Create lead ──────────────────────────────────────────────────────
        const apiResult = await createLead(payload);

        // FIX: store leadId immediately so it's not lost if getSummary fails
        await pool.query(
            "UPDATE bulk_leads SET leadId=?, status='created' WHERE id=?",
            [apiResult.leadId ?? null, lead.id]
        );

        // ── Fetch summary ────────────────────────────────────────────────────
        // FIX: getSummary failure should not roll back the 'created' status
        let offersTotal = 0, minMPR = null, maxMPR = null, message = null;
        try {
            const summary = await getSummary(apiResult.leadId);
            const s = summary?.success;
            if ((s === true || s === "true" || s === 1) && summary.summary) {
                offersTotal = summary.summary.offersTotal ?? 0;
                minMPR      = summary.summary.minMPR      ?? null;
                maxMPR      = summary.summary.maxMPR      ?? null;
                message     = summary.message             ?? null;
            }
        } catch (summaryErr) {
            console.warn(`⚠️  getSummary failed for lead ${lead.id}:`, summaryErr.message);
            // Continue — lead is already 'created', just mark completed without summary
        }

        await pool.query(
            `UPDATE bulk_leads
             SET offersTotal=?, minMPR=?, maxMPR=?, message=?, status='completed'
             WHERE id=?`,
            [offersTotal, minMPR, maxMPR, message, lead.id]
        );

    } catch (err) {
        // ── Structured error message ─────────────────────────────────────────
        // FIX: truncate to 500 chars so it fits in a VARCHAR column safely
        const raw = err.response?.data
            ? JSON.stringify(err.response.data)
            : err.message;
        const errorMessage = raw.slice(0, 500);

        await pool.query(
            `UPDATE bulk_leads
             SET status='failed',
                 retry_count = IFNULL(retry_count, 0) + 1,
                 message = ?
             WHERE id=?`,
            [errorMessage, lead.id]
        );
    }
}

// ================= ENGINE =================
// FIX: guard against overlapping runs with a lock flag
let engineRunning = false;

async function runEngine() {
    if (engineRunning) {
        console.log("⏭  Engine still running from last cycle — skipping this tick");
        return;
    }
    engineRunning = true;

    try {
        // FIX: only pick 'failed' leads that haven't hit MAX_RETRY yet
        const [leads] = await pool.query(
            `SELECT * FROM bulk_leads
             WHERE (status = 'pending')
                OR (status = 'failed' AND IFNULL(retry_count, 0) < ?)
             LIMIT ?`,
            [MAX_RETRY, PARALLEL_LIMIT]
        );

        if (leads.length === 0) {
            await showConsoleDashboard(0);
            return;
        }

        // FIX: allSettled so one lead failure never stops the rest
        const results = await Promise.allSettled(leads.map(processLead));

        // Log any unexpected errors
        results.forEach((r, i) => {
            if (r.status === "rejected") {
                console.error(`❌ processLead crashed for lead id=${leads[i].id}:`, r.reason?.message);
            }
        });

        await showConsoleDashboard(leads.length);
    } catch (err) {
        console.error("❌ Engine error:", err.message);
    } finally {
        engineRunning = false;
    }
}

// ================= GRACEFUL SHUTDOWN =================
async function shutdown(sig) {
    console.log(`\n⛔ ${sig} — shutting down…`);
    try { await pool.end(); } catch {}
    process.exit(0);
}
process.on("SIGINT",  () => shutdown("SIGINT"));
process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("unhandledRejection", err => console.error("⚠️  Unhandled:", err?.message ?? err));

// ================= START =================
runEngine();
setInterval(runEngine, REFRESH_INTERVAL);
