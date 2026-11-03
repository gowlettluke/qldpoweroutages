const assert = require("node:assert/strict");
const fs = require("node:fs");
const vm = require("node:vm");
const html = fs.readFileSync("index.html", "utf8");
const script = html.match(/<script>\s*([\s\S]*?)<\/script>/)[1];
// Evaluate the real dashboard functions, excluding the DOM startup handlers.
const source = script.slice(0, script.lastIndexOf('    document.querySelectorAll(".tab-btn")'));
const context = vm.createContext({ Intl, console, URLSearchParams });
vm.runInContext(source, context);

function row(overrides = {}) {
  return context.normaliseSnapshotRow({
    snapshot_utc: "2026-11-03T09:31:09Z", provider_code: "au.qld.ergon", provider_name: "Ergon Energy",
    outage_type: "unplanned", outage_id: "26WB10530", affected_customers: "5539", affected_customers_known: "true",
    lga_name: "South Burnett Regional", lga_assignment_source: "provider",
    lga_allocations_json: JSON.stringify([
      { lga_name: "South Burnett Regional", affected_customers: 4150 },
      { lga_name: "Gympie Regional", affected_customers: 1007 },
      { lga_name: "Cherbourg Aboriginal Shire", affected_customers: 380 },
      { lga_name: "Somerset Regional", affected_customers: 2 }
    ]), ...overrides
  });
}

const current = [row()];
const data = context.prepareDashboardData({ generated_utc: "2026-11-03T09:31:09Z", current_outage_records: 1 }, current, []);
assert.equal(data.currentSummary.latestCustomerSum, 5539);
assert.equal(data.currentSummary.incidents, 1);
assert.equal(data.currentSummary.lgaCount, 4);
assert.equal(data.topLgas.reduce((sum, r) => sum + r.currentCustomers, 0), 5539);
assert.equal(data.topLgas.find((r) => r.name === "Gympie Regional").currentCustomers, 1007);
assert.equal(data.topProviders.length, 1);
assert.equal(data.topProviders[0].currentIncidents, 1);
assert.equal(data.lgaMapPoints.reduce((sum, r) => sum + r.customers, 0), 5539);

const legacy = [row({
  outage_id: "915719", raw_json: '{"EVENT_ID":"26FN10815"}', affected_customers: "131",
  lga_allocations_json: "", snapshot_utc: "2026-11-03T09:01:25Z"
}), row({
  outage_id: "915729", raw_json: '{"EVENT_ID":"26FN10815"}', affected_customers: "2",
  lga_allocations_json: "", snapshot_utc: "2026-11-03T09:15:58Z"
})];
const history = context.collapseWindowIncidents(legacy);
assert.equal(history.length, 1);
assert.equal(history[0].peakCustomers, 131);
assert.equal(history[0].latestCustomers, 2);

const cleared = context.prepareDashboardData({ generated_utc: "2026-11-03T09:46:00Z", current_outage_records: 0 }, current, []);
assert.equal(cleared.currentSummary.latestCustomerSum, 0);
assert.equal(cleared.currentSummary.incidents, 0);
assert.equal(cleared.trendSince.at(-1).customers, 0);
const empty = context.prepareDashboardData({ generated_utc: "2026-11-03T09:46:00Z", current_outage_records: 0 }, [], []);
assert.equal(empty.currentSummary.incidents, 0);
assert.throws(() => context.prepareDashboardData({ generated_utc: "2026-11-03T09:46:00Z", current_outage_records: 1 }, current, []), /still updating/);
assert.throws(() => row({ affected_customers: "999" }), /do not match/);
console.log("Dashboard checks passed: council allocations, unique events, history migration, cleared/empty feeds, inconsistent snapshots.");
