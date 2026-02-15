const mysql = require("mysql2/promise");
const axios = require("axios");

// ================= CONFIG =================
const API_KEY = "4827f87b-0e70-45ac-b822-92e7b4d6a291";
const REFRESH_INTERVAL = 60000; // 1 minute
const PARALLEL_LIMIT = 10;
const MAX_RETRY = 3;

// ================= MYSQL =================
const pool = mysql.createPool({
    host: "82.25.121.2",
    user: "u527886566_credifyy",
    password: "VAKILr@6762", // CHANGE THIS
    database: "u527886566_credifyy",
    waitForConnections: true,
    connectionLimit: 20
});

// ================= AXIOS =================
const axiosInstance = axios.create({
    timeout: 20000,
    headers: {
        apikey: API_KEY,
        "Content-Type": "application/json"
    }
});

// ================= DATE FORMAT =================
function formatDateTime(date) {
    const pad = (n) => n.toString().padStart(2, "0");
    return date.getFullYear() + "-" +
        pad(date.getMonth() + 1) + "-" +
        pad(date.getDate()) + " " +
        pad(date.getHours()) + ":" +
        pad(date.getMinutes()) + ":" +
        pad(date.getSeconds());
}

// ================= DOB FIX =================
function formatDOB(dob) {

    if (!dob) return null;

    if (typeof dob === "string" && /^\d{4}-\d{2}-\d{2}$/.test(dob)) {
        return dob;
    }

    if (dob instanceof Date) {

        const year = dob.getFullYear();
        if (year < 1900) return null;

        const pad = (n) => n.toString().padStart(2, "0");

        return year + "-" +
            pad(dob.getMonth() + 1) + "-" +
            pad(dob.getDate());
    }

    return null;
}

// ================= TERMINAL DASHBOARD =================
async function showConsoleDashboard(lastBatchProcessed) {

    const [rows] = await pool.query(`
        SELECT 
            COUNT(*) as total,
            SUM(status='pending') as pending,
            SUM(status='completed') as completed,
            SUM(status='failed') as failed,
            SUM(status='ineligible') as ineligible
        FROM bulk_leads
    `);

    console.clear();
    console.log("=========================================");
    console.log("        🚀 LEAD ENGINE CONSOLE");
    console.log("=========================================");
    console.log("Total Leads     :", rows[0].total || 0);
    console.log("Pending         :", rows[0].pending || 0);
    console.log("Completed       :", rows[0].completed || 0);
    console.log("Failed          :", rows[0].failed || 0);
    console.log("Ineligible      :", rows[0].ineligible || 0);
    console.log("-----------------------------------------");
    console.log("Processed Batch :", lastBatchProcessed);
    console.log("Last Update     :", new Date().toLocaleString());
    console.log("=========================================\n");
}

// ================= API HELPERS =================

async function checkDedupe(mobileNumber) {

    const response = await axiosInstance.post(
        "https://l.creditlinks.in:8000/api/partner/dedupe", {
            mobileNumber: mobileNumber
        }
    );

    if (response.data && response.data.success === "true") {
        return true;
    }

    return false;
}

async function createLead(payload) {

    const response = await axiosInstance.post(
        "https://l.creditlinks.in:8000/api/v2/partner/create-lead",
        payload
    );

    if (response.data && response.data.success === "true") {
        return response.data;
    }

    throw new Error(JSON.stringify(response.data));
}

async function getSummary(leadId) {

    const response = await axiosInstance.get(
        "https://l.creditlinks.in:8000/api/partner/get-summary/" + leadId
    );

    return response.data;
}

// ================= CORE PROCESS =================

async function processLead(lead) {

    try {

        if (lead.retry_count >= MAX_RETRY) {

            await pool.query(
                "UPDATE bulk_leads SET status='failed_final' WHERE id=?",
                [lead.id]
            );
            return;
        }

        const cleanMobile = lead.mobileNumber.replace(/\D/g, "");
        const formattedDOB = formatDOB(lead.dob);

        if (!formattedDOB) {
            await pool.query(
                "UPDATE bulk_leads SET status='failed', message='Invalid DOB' WHERE id=?",
                [lead.id]
            );
            return;
        }

        const eligible = await checkDedupe(cleanMobile);

        if (!eligible) {

            await pool.query(
                "UPDATE bulk_leads SET status='ineligible' WHERE id=?",
                [lead.id]
            );
            return;
        }

        const payload = {
            mobileNumber: cleanMobile,
            firstName: lead.firstName,
            lastName: lead.lastName,
            pan: lead.pan ? lead.pan.toUpperCase() : null,
            dob: formattedDOB,
            email: lead.email,
            pincode: lead.pincode,
            monthlyIncome: Number(lead.monthlyIncome),
            consumerConsentDate: formatDateTime(new Date()),
            consumerConsentIp: lead.consumerConsentIp,
            employmentStatus: lead.employmentStatus,
            employerName: lead.employerName,
            officePincode: lead.officePincode
        };

        const apiResult = await createLead(payload);

        await pool.query(
            "UPDATE bulk_leads SET leadId=?, status='created' WHERE id=?",
            [apiResult.leadId, lead.id]
        );

        const summary = await getSummary(apiResult.leadId);

        let offersTotal = 0;
        let minMPR = null;
        let maxMPR = null;
        let message = null;

        if (summary && summary.success === "true" && summary.summary) {

            offersTotal = summary.summary.offersTotal ? summary.summary.offersTotal : 0;
            minMPR = summary.summary.minMPR ? summary.summary.minMPR : null;
            maxMPR = summary.summary.maxMPR ? summary.summary.maxMPR : null;
            message = summary.message ? summary.message : null;
        }

        await pool.query(
            `UPDATE bulk_leads
             SET offersTotal=?, minMPR=?, maxMPR=?, message=?, status='completed'
             WHERE id=?`,
            [offersTotal, minMPR, maxMPR, message, lead.id]
        );

    } catch (err) {

        let errorMessage = err.message;

        if (err.response && err.response.data) {
            errorMessage = JSON.stringify(err.response.data);
        }

        await pool.query(
            `UPDATE bulk_leads
             SET status='failed',
                 retry_count = IFNULL(retry_count,0)+1,
                 message=?
             WHERE id=?`,
            [errorMessage, lead.id]
        );
    }
}

// ================= ENGINE =================

async function runEngine() {

    const [leads] = await pool.query(
        "SELECT * FROM bulk_leads WHERE status IN ('pending','failed') LIMIT ?",
        [PARALLEL_LIMIT]
    );

    if (leads.length === 0) {
        await showConsoleDashboard(0);
        return;
    }

    await Promise.all(leads.map(processLead));

    await showConsoleDashboard(leads.length);
}

// ================= AUTO LOOP =================

setInterval(runEngine, REFRESH_INTERVAL);

// First run immediately
runEngine();
