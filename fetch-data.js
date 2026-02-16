/**
 * fetch-data.js
 * Queries BigQuery (MemberPress + GA4) and writes data.json
 *
 * Generates data for 4 periods: 7d, 30d, 90d, YTD
 * The dashboard switches between them client-side.
 *
 * Usage:
 *   GOOGLE_APPLICATION_CREDENTIALS="./keys/lwj-data-storage-*.json" node fetch-data.js
 */

const { BigQuery } = require('@google-cloud/bigquery');
const fs = require('fs');
const path = require('path');

const CONFIG = {
  projectId: process.env.BQ_PROJECT_ID || 'lwj-data-storage',
  location: 'US',
  memberPress: { dataset: 'LWJ' },
  ga4: { dataset: 'analytics_301113294' },
  outputPath: path.join(__dirname, 'data.json'),
};

const bq = new BigQuery({ projectId: CONFIG.projectId, location: CONFIG.location });

// ─── DATE HELPERS ──────────────────────────────────────────────────────────────
function fmt(d) { return d.toISOString().split('T')[0]; }

function getDateRange(days) {
  const end = new Date();
  const start = new Date();
  start.setDate(start.getDate() - days);
  return { startDate: fmt(start), endDate: fmt(end) };
}

function getYTDRange() {
  const end = new Date();
  const start = new Date(end.getFullYear(), 0, 1);
  return { startDate: fmt(start), endDate: fmt(end) };
}

// ─── MEMBERPRESS QUERIES ───────────────────────────────────────────────────────
async function fetchSubscriptionMetrics(startDate, endDate) {
  const ds = CONFIG.memberPress.dataset;

  const [[newSubsRow], [churnRow], [dailyRows]] = await Promise.all([
    bq.query({ query: `
      SELECT
        COUNT(*) AS new_subs,
        COUNTIF(status = 'active') AS new_active,
        COUNTIF(status = 'cancelled') AS new_cancelled,
        COUNTIF(status = 'pending') AS new_pending,
        COUNTIF(status = 'suspended') AS new_suspended,
        SUM(CASE WHEN status = 'active' THEN total ELSE 0 END) AS new_mrr
      FROM \`${CONFIG.projectId}.${ds}.Mepr_Subscriptions\`
      WHERE created_at BETWEEN TIMESTAMP('${startDate}') AND TIMESTAMP('${endDate}')
    ` }),
    bq.query({ query: `
      SELECT COUNT(*) AS churned
      FROM \`${CONFIG.projectId}.${ds}.Mepr_Subscriptions\`
      WHERE status = 'cancelled'
        AND created_at BETWEEN TIMESTAMP('${startDate}') AND TIMESTAMP('${endDate}')
    ` }),
    bq.query({ query: `
      SELECT DATE(created_at) AS day, COUNT(*) AS new_subs, COUNTIF(status = 'active') AS still_active
      FROM \`${CONFIG.projectId}.${ds}.Mepr_Subscriptions\`
      WHERE created_at BETWEEN TIMESTAMP('${startDate}') AND TIMESTAMP('${endDate}')
      GROUP BY day ORDER BY day
    ` }),
  ]);

  const ns = newSubsRow[0] || {};
  const ch = churnRow[0] || {};
  return {
    newSubs: Number(ns.new_subs || 0),
    newActive: Number(ns.new_active || 0),
    newCancelled: Number(ns.new_cancelled || 0),
    newPending: Number(ns.new_pending || 0),
    newSuspended: Number(ns.new_suspended || 0),
    newMrr: Math.round(Number(ns.new_mrr || 0) * 100) / 100,
    churned: Number(ch.churned || 0),
    retentionRate: ns.new_subs > 0
      ? Math.round((Number(ns.new_active || 0) / Number(ns.new_subs)) * 1000) / 10 : 0,
    daily: dailyRows.map(d => ({
      day: d.day.value,
      newSubs: Number(d.new_subs),
      stillActive: Number(d.still_active),
    })),
  };
}

// Global subscription state (doesn't change with period)
async function fetchGlobalSubState() {
  const ds = CONFIG.memberPress.dataset;
  const [[activeRow], [plansRows], [statusRows]] = await Promise.all([
    bq.query({ query: `
      SELECT COUNT(*) AS total_active, SUM(total) AS total_mrr,
        COUNTIF(period_type = 'months') AS monthly_subs,
        COUNTIF(period_type = 'years') AS annual_subs
      FROM \`${CONFIG.projectId}.${ds}.Mepr_Subscriptions\` WHERE status = 'active'
    ` }),
    bq.query({ query: `
      SELECT total AS price, period_type, COUNT(*) AS active_count, SUM(total) AS plan_mrr
      FROM \`${CONFIG.projectId}.${ds}.Mepr_Subscriptions\`
      WHERE status = 'active' AND total > 0
      GROUP BY total, period_type ORDER BY active_count DESC LIMIT 8
    ` }),
    bq.query({ query: `
      SELECT status, COUNT(*) AS cnt
      FROM \`${CONFIG.projectId}.${ds}.Mepr_Subscriptions\`
      GROUP BY status ORDER BY cnt DESC
    ` }),
  ]);

  const ac = activeRow[0] || {};
  return {
    totalActive: Number(ac.total_active || 0),
    totalMrr: Math.round(Number(ac.total_mrr || 0) * 100) / 100,
    monthlySubs: Number(ac.monthly_subs || 0),
    annualSubs: Number(ac.annual_subs || 0),
    plans: plansRows.map(p => ({
      price: Number(p.price),
      periodType: p.period_type,
      activeCount: Number(p.active_count),
      mrr: Math.round(Number(p.plan_mrr) * 100) / 100,
    })),
    allStatuses: statusRows.map(s => ({ status: s.status, count: Number(s.cnt) })),
  };
}

// ─── GA4 QUERIES ───────────────────────────────────────────────────────────────
async function fetchGA4Sessions(startDate, endDate) {
  const s = startDate.replace(/-/g, '');
  const e = endDate.replace(/-/g, '');

  const [rows] = await bq.query({ query: `
    SELECT traffic_source.source AS source, traffic_source.medium AS medium,
      COUNT(DISTINCT CONCAT(user_pseudo_id, '-',
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id')
      )) AS sessions
    FROM \`${CONFIG.projectId}.${CONFIG.ga4.dataset}.events_*\`
    WHERE _TABLE_SUFFIX BETWEEN '${s}' AND '${e}' AND event_name = 'session_start'
    GROUP BY source, medium ORDER BY sessions DESC
  ` });

  let totalSessions = 0;
  const sourceMap = {};
  rows.forEach(r => {
    totalSessions += Number(r.sessions);
    const key = classifyTrafficSource(r.source, r.medium);
    sourceMap[key] = (sourceMap[key] || 0) + Number(r.sessions);
  });
  return { totalSessions, bySource: sourceMap };
}

async function fetchGA4DailySessions(startDate, endDate) {
  const s = startDate.replace(/-/g, '');
  const e = endDate.replace(/-/g, '');

  const [rows] = await bq.query({ query: `
    SELECT _TABLE_SUFFIX AS day_str,
      COUNT(DISTINCT CONCAT(user_pseudo_id, '-',
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id')
      )) AS sessions
    FROM \`${CONFIG.projectId}.${CONFIG.ga4.dataset}.events_*\`
    WHERE _TABLE_SUFFIX BETWEEN '${s}' AND '${e}' AND event_name = 'session_start'
    GROUP BY day_str ORDER BY day_str
  ` });

  return rows.map(r => ({
    day: r.day_str.replace(/(\d{4})(\d{2})(\d{2})/, '$1-$2-$3'),
    sessions: Number(r.sessions),
  }));
}

function classifyTrafficSource(source, medium) {
  if (medium === 'cpc' || medium === 'paid') return 'Paid Search';
  if (medium === 'organic' || medium === 'organic_search') return 'Organic Search';
  if (medium === 'email') return 'Email';
  if (medium === 'referral') return 'Referral';
  if (medium === 'social' || medium === 'paid_social') return 'Social';
  return 'Direct';
}

// ─── BUILD PERIOD DATA ─────────────────────────────────────────────────────────
function pctChange(current, previous) {
  if (!previous) return 0;
  return Math.round(((current - previous) / previous) * 1000) / 10;
}

async function buildPeriod(startDate, endDate, prevStart, prevEnd, globalState) {
  const [subs, ga4, prevSubs, prevGa4, dailySessions] = await Promise.all([
    fetchSubscriptionMetrics(startDate, endDate),
    fetchGA4Sessions(startDate, endDate),
    fetchSubscriptionMetrics(prevStart, prevEnd),
    fetchGA4Sessions(prevStart, prevEnd),
    fetchGA4DailySessions(startDate, endDate),
  ]);

  const sourceOrder = ['Organic Search', 'Direct', 'Referral', 'Paid Search', 'Email', 'Social'];
  const trafficSources = sourceOrder
    .filter(name => ga4.bySource[name])
    .map(name => ({ source: name, sessions: ga4.bySource[name] || 0 }));

  return {
    kpis: {
      totalActiveSubs: globalState.totalActive,
      activeSubsChange: pctChange(globalState.totalActive, globalState.totalActive - subs.newActive + prevSubs.newActive),
      totalMrr: globalState.totalMrr,
      newSignups: subs.newSubs,
      signupsChange: pctChange(subs.newSubs, prevSubs.newSubs),
      retentionRate: subs.retentionRate,
      retentionChange: pctChange(subs.retentionRate, prevSubs.retentionRate),
      websiteSessions: ga4.totalSessions,
      sessionsChange: pctChange(ga4.totalSessions, prevGa4.totalSessions),
      conversionRate: ga4.totalSessions > 0
        ? Math.round((subs.newSubs / ga4.totalSessions) * 10000) / 100 : 0,
      churned: subs.churned,
      churnChange: pctChange(subs.churned, prevSubs.churned),
    },
    funnel: [
      { stage: 'Sessions', value: ga4.totalSessions, source: 'ga4' },
      { stage: 'New Signups', value: subs.newSubs, source: 'memberpress' },
      { stage: 'Active Members', value: subs.newActive, source: 'memberpress' },
    ],
    trafficSources,
    dailySignups: subs.daily,
    dailySessions,
    subscriptionBreakdown: {
      monthly: globalState.monthlySubs,
      annual: globalState.annualSubs,
      newActive: subs.newActive,
      newCancelled: subs.newCancelled,
      newPending: subs.newPending,
      newSuspended: subs.newSuspended,
    },
  };
}

// ─── MAIN ──────────────────────────────────────────────────────────────────────
async function main() {
  console.log('Fetching data from BigQuery...\n');

  // Global state (doesn't change with period)
  const globalState = await fetchGlobalSubState();
  console.log(`  Active subs: ${globalState.totalActive} | MRR: $${globalState.totalMrr.toLocaleString()}`);

  // Define periods with their comparison ranges
  const periods = {
    '7d':  { ...getDateRange(7),  prev: getDateRange(14) },
    '30d': { ...getDateRange(30), prev: getDateRange(60) },
    '90d': { ...getDateRange(90), prev: getDateRange(180) },
    'ytd': { ...getYTDRange(),    prev: (() => {
      const r = getYTDRange();
      const days = Math.ceil((new Date(r.endDate) - new Date(r.startDate)) / 86400000);
      return getDateRange(days * 2);
    })() },
  };

  const output = {
    meta: {
      generatedAt: new Date().toISOString(),
      sources: ['memberpress', 'ga4'],
    },
    globalState: {
      totalActive: globalState.totalActive,
      totalMrr: globalState.totalMrr,
      monthlySubs: globalState.monthlySubs,
      annualSubs: globalState.annualSubs,
      plans: globalState.plans,
      allStatuses: globalState.allStatuses,
    },
    periods: {},
  };

  for (const [key, range] of Object.entries(periods)) {
    console.log(`  Fetching ${key}: ${range.startDate} → ${range.endDate}`);
    output.periods[key] = await buildPeriod(
      range.startDate, range.endDate,
      range.prev.startDate, range.prev.endDate,
      globalState
    );
  }

  fs.writeFileSync(CONFIG.outputPath, JSON.stringify(output, null, 2));
  console.log(`\n✓ data.json written (${(fs.statSync(CONFIG.outputPath).size / 1024).toFixed(1)} KB)`);

  const d30 = output.periods['30d'].kpis;
  console.log(`\n  30-day summary:`);
  console.log(`    Sessions: ${d30.websiteSessions} | Signups: ${d30.newSignups} | Retention: ${d30.retentionRate}%`);
  console.log(`    Conv Rate: ${d30.conversionRate}% | Churned: ${d30.churned}`);
}

main().catch(err => {
  console.error('Error:', err.message);
  process.exit(1);
});
