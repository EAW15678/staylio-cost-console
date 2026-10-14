import { useState, useEffect } from "react";

// Every number on this page comes from the Cost Console API, which reads the
// pipeline's cost ledger (public.cost_events) read-only. Nothing here is sample data.
const PROXY_BASE = 'https://dashboard-proxy.staylio.ai';

async function apiFetch(path) {
  const res = await fetch(`${PROXY_BASE}${path}`);
  if (!res.ok) throw new Error(`Cost data unavailable (${res.status}) for ${path.split('?')[0]}`);
  return res.json();
}

const periods = [["24h","24h"],["7d","7d"],["30d","30d"],["MTD","mtd"],["90d","90d"],["All","all"],["Custom","custom"]];
const STICKY_BG = "bg-[#182029]";

function money(v) { return v == null ? '—' : `$${Number(v).toFixed(2)}`; }
function et(iso) { return iso ? new Date(iso).toLocaleString('en-US', { timeZone: 'America/New_York', month: 'short', day: 'numeric', hour: 'numeric', minute: '2-digit' }) + ' ET' : '—'; }

function MetricCard({ label, value, sub, accent="white" }) {
  const c = accent==="teal"?"text-[#40CCC1]":accent==="green"?"text-[#46C37B]":"text-white";
  return (
    <div className="relative overflow-hidden rounded-2xl border border-[#303A44] bg-[#242C36] p-6">
      <div className={`absolute left-0 top-0 h-[3px] w-full ${accent==="white"?"bg-transparent":accent==="green"?"bg-gradient-to-r from-[#46C37B] to-[#46C37B]":"bg-gradient-to-r from-[#40CCC1] to-[#40CCC1]"}`} />
      <div className="mb-4 text-[0.68rem] font-medium uppercase tracking-[0.15em] text-[#ABB5C0]">{label}</div>
      <div className={`font-serif text-[2.3rem] leading-none ${c}`}>{value}</div>
      <div className="mt-3 text-[0.78rem] text-[#ABB5C0]">{sub}</div>
    </div>
  );
}

function SectionHeader({ eyebrow, title, body }) {
  return (
    <div className="mb-6">
      <div className="mb-2 text-[0.72rem] font-medium uppercase tracking-[0.16em] text-[#40CCC1]">{eyebrow}</div>
      <h2 className="font-serif text-[22px] font-medium leading-tight text-white">{title}</h2>
      {body && <p className="mt-2 max-w-[760px] text-sm leading-7 text-[#ABB5C0]">{body}</p>}
    </div>
  );
}


function Panel({ eyebrow, title, body, children, flush }) {
  return (
    <div className={`rounded-2xl border border-[#303A44] bg-[#242C36] ${flush ? 'overflow-hidden' : 'p-6 lg:p-8'}`}>
      <div className={flush ? 'px-6 pt-6 pb-4 lg:px-8 lg:pt-8' : ''}><SectionHeader eyebrow={eyebrow} title={title} body={body} /></div>
      {children}
    </div>
  );
}

function Table({ cols, rows, render, onRow, selectedKey, rowKey }) {
  return (
    <div className="overflow-x-auto">
      <table className="w-full min-w-[720px] border-collapse">
        <thead><tr className="border-y border-[#303A44] bg-[#182029]">
          {cols.map((h, i) => <th key={h} className={`px-6 py-4 text-left text-[0.68rem] font-medium uppercase tracking-[0.14em] text-[#ABB5C0] ${i === 0 ? `sticky left-0 z-10 ${STICKY_BG}` : ''}`}>{h}</th>)}
        </tr></thead>
        <tbody>
          {rows.length === 0 && <tr><td colSpan={cols.length} className="px-6 py-6 text-sm text-[#ABB5C0]">Nothing recorded in this period.</td></tr>}
          {rows.map(r => (
            <tr key={rowKey(r)} onClick={onRow ? () => onRow(r) : undefined}
                className={`border-b border-[#303A44] transition hover:bg-[#182029] ${onRow ? 'cursor-pointer' : ''} ${selectedKey === rowKey(r) ? 'bg-[#40CCC1]/10' : ''}`}>
              {render(r).map((cell, i) => <td key={i} className={`px-6 py-4 text-sm text-[#F4F7FB] ${i === 0 ? `sticky left-0 z-10 ${STICKY_BG} font-medium text-white` : ''}`}>{cell}</td>)}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function Bars({ rows, label, value, sub }) {
  const max = Math.max(...rows.map(value), 0.001);
  return (
    <div className="space-y-5">
      {rows.length === 0 && <div className="text-sm text-[#ABB5C0]">Nothing recorded in this period.</div>}
      {rows.map(r => (
        <div key={label(r)}>
          <div className="mb-2 flex items-end justify-between gap-4">
            <div><div className="text-[0.98rem] font-medium text-white">{label(r)}</div><div className="mt-1 text-xs text-[#ABB5C0]">{sub(r)}</div></div>
            <div className="font-serif text-2xl text-white">{money(value(r))}</div>
          </div>
          <div className="h-2 overflow-hidden rounded-full bg-white/8"><div className="h-full rounded-full bg-gradient-to-r from-[#40CCC1] to-[#40CCC1]" style={{ width: `${Math.max((value(r) / max) * 100, 2)}%` }} /></div>
        </div>
      ))}
    </div>
  );
}

function SpendChart({ series }) {
  const max = Math.max(...series.map(d => d.cost_usd), 0.001);
  return (
    <div className="h-[260px] rounded-xl border border-[#303A44] bg-[#182029] p-5">
      {series.length === 0 ? <div className="text-sm text-[#ABB5C0]">Nothing recorded in this period.</div> : (
        <div className="flex h-full items-end gap-2">
          {series.map(d => (
            <div key={d.date} className="flex flex-1 flex-col items-center justify-end gap-2" title={`${d.date}: ${money(d.cost_usd)} · ${d.event_count} charges`}>
              <div className="w-full rounded-t-md bg-gradient-to-t from-[#40CCC1] to-[#40CCC1]" style={{ height: `${Math.max((d.cost_usd / max) * 190, 3)}px` }} />
              <div className="text-[0.62rem] text-[#ABB5C0]">{d.date.slice(5)}</div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

function VendorList({ vendors }) {
  return (
    <div className="space-y-2">
      {(vendors ?? []).map(v => (
        <div key={v.vendor_id} className="flex justify-between gap-4 border-b border-[#303A44] pb-2 text-sm last:border-0">
          <span className="text-[#F4F7FB]">{v.vendor_name}{v.units != null && v.unit_name ? <span className="text-[#ABB5C0]"> · {Number(v.units).toLocaleString()} {v.unit_name}</span> : null}</span>
          <span className="font-serif text-white">{money(v.cost_usd ?? v.total_cost)}</span>
        </div>
      ))}
    </div>
  );
}

function ComponentList({ components }) {
  return (
    <div className="space-y-2">
      {(components ?? []).length === 0 && <div className="text-sm text-[#ABB5C0]">No component costs recorded.</div>}
      {(components ?? []).map(c => (
        <div key={c.component} className="flex justify-between gap-4 border-b border-[#303A44] pb-2 text-sm last:border-0">
          <span className="text-[#F4F7FB]">{c.component === '' ? '(Empty workflow name)' : c.component}</span>
          <span className="font-serif text-white">{money(c.cost_usd)}</span>
        </div>
      ))}
    </div>
  );
}

function BuildCosts({ b }) {
  return (
    <details className="rounded-xl border border-[#303A44] p-4">
      <summary className="cursor-pointer text-sm">{et(b.started_at)} · {b.status} · {money(b.total_cost)} · <LinkBadge b={b} /></summary>
      <div className="mt-2 break-all text-xs text-[#ABB5C0]">Run {b.run_id}</div>
      <div className="mt-4 grid gap-6 md:grid-cols-2">
        <div><h3 className="mb-3 font-medium">Components</h3><ComponentList components={b.components} /></div>
        <div><h3 className="mb-3 font-medium">Vendors</h3><VendorList vendors={b.vendors} /></div>
      </div>
    </details>
  );
}

function ComponentAverages({ averages }) {
  return (
    <div className="mt-6 border-t border-[#303A44] pt-5">
      <h3 className="mb-2 font-medium">Typical build components</h3>
      <p className="mb-3 text-xs leading-6 text-[#ABB5C0]">{averages?.component_build_count ?? 0} completed builds, last 90 days. Average and median include zero recorded cost for a component absent from a build.</p>
      <Table cols={['Component', 'Average', 'Median', 'Builds']} rows={averages?.components ?? []} rowKey={c => c.component}
             render={c => [c.component === '' ? '(Empty workflow name)' : c.component, money(c.average_cost), money(c.median_cost), c.build_count]} />
    </div>
  );
}

function LinkBadge({ b }) {
  return b.linked
    ? <span className="inline-flex rounded-full border border-[#40CCC1]/40 bg-[#40CCC1]/20 px-2.5 py-1 text-[0.7rem] font-semibold text-[#40CCC1]">Recorded total</span>
    : <span className="inline-flex rounded-full border border-amber-400/30 bg-amber-400/10 px-2.5 py-1 text-[0.7rem] font-semibold text-amber-300" title="Ran before the pipeline linked build steps (Oct 11, 2026): only the build's own charges are attached.">Partial — before build linking</span>;
}

function LoadingState() {
  return (
    <div className="rounded-2xl border border-[#303A44] bg-[#242C36] px-6 py-10">
      <div className="animate-pulse space-y-4">
        <div className="h-3 w-36 rounded bg-white/10" />
        <div className="h-10 w-72 rounded bg-white/10" />
        <div className="h-3 w-full rounded bg-white/10" />
        <div className="h-3 w-5/6 rounded bg-white/10" />
      </div>
    </div>
  );
}

function ErrorState({ message }) {
  return (
    <div className="rounded-2xl border border-red-500/20 bg-red-500/5 px-6 py-12 text-center">
      <div className="mb-3 text-[0.72rem] font-medium uppercase tracking-[0.16em] text-red-400">API Error</div>
      <div className="font-serif text-3xl text-white">Failed to load data.</div>
      <p className="mx-auto mt-3 max-w-[560px] text-sm leading-7 text-[#ABB5C0]">{message}</p>
    </div>
  );
}

export default function StaylioCostConsoleDashboard() {
  const [period, setPeriod] = useState('30d');
  const [customStart, setCustomStart] = useState('');
  const [customEnd, setCustomEnd] = useState('');
  const [applied, setApplied] = useState(null);
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [build, setBuild] = useState(null);
  const [property, setProperty] = useState(null);
  const [propertyDetail, setPropertyDetail] = useState(null);
  const [account, setAccount] = useState(null);
  const [accountDetail, setAccountDetail] = useState(null);
  const [costViews, setCostViews] = useState(null);
  const [costViewsError, setCostViewsError] = useState(null);

  const params = applied ? `start_date=${applied.start}&end_date=${applied.end}` : `period=${period}`;

  useEffect(() => {
    if (period === 'custom' && !applied) return;
    setLoading(true); setError(null); setBuild(null); setProperty(null); setPropertyDetail(null);
    Promise.all([
      apiFetch('/metrics/summary'),
      apiFetch(`/metrics/vendors?${params}`),
      apiFetch(`/metrics/workflows?${params}`),
      apiFetch(`/metrics/timeseries?${params}`),
      apiFetch(`/metrics/builds?${params}`),
      apiFetch('/metrics/builds/averages?period=90d'),
      apiFetch(`/metrics/properties?${params}`),
    ]).then(([summary, vendors, purposes, series, builds, averages, properties]) => {
      setData({ summary, vendors, purposes, series, builds, averages, properties }); setLoading(false);
    }).catch(e => { setError(e.message); setLoading(false); });
  }, [period, applied]);

  useEffect(() => {
    if (!property) return;
    setPropertyDetail(null);
    apiFetch(`/metrics/property/${property}?${params}`).then(setPropertyDetail).catch(e => setPropertyDetail({ error: e.message }));
  }, [property]);

  useEffect(() => {
    if (period === 'custom' && !applied) return;
    let active = true;
    setCostViews(null); setCostViewsError(null); setAccount(null); setAccountDetail(null);
    Promise.all([apiFetch(`/metrics/accounts?${params}`), apiFetch(`/metrics/property-costs?${params}`)])
      .then(([accounts, properties]) => { if (active) setCostViews({ accounts, properties }); })
      .catch(e => { if (active) setCostViewsError(e.message); });
    return () => { active = false; };
  }, [params, period, applied]);

  useEffect(() => {
    if (!account) return;
    let active = true;
    setAccountDetail(null);
    apiFetch(`/metrics/account/${account}?${params}`)
      .then(value => { if (active) setAccountDetail(value); })
      .catch(e => { if (active) setAccountDetail({ error: e.message }); });
    return () => { active = false; };
  }, [account, params]);

  const d = data;
  const builds = d?.builds?.builds ?? [];
  const avg = d?.averages;
  const kpis = d ? [
    { label: 'Spend today', value: money(d.summary.today_spend), sub: 'Eastern time, all vendors', accent: 'teal' },
    { label: 'Month to date', value: money(d.summary.mtd_spend), sub: 'from the pipeline cost ledger', accent: 'white' },
    { label: 'Spend this period', value: money(d.vendors.total), sub: d.vendors.period, accent: 'white' },
    { label: 'Builds this period', value: String(builds.length), sub: `${builds.filter(b => b.status === 'complete').length} completed · ${builds.filter(b => b.linked).length} with complete totals`, accent: 'green' },
    { label: 'Cost per completed build', value: avg?.completed_linked_builds ? money(avg.cost_per_build) : 'Waiting', sub: avg?.completed_linked_builds ? `average of ${avg.completed_linked_builds} linked builds, last 90 days` : 'no completed build with linked steps yet', accent: 'teal' },
    { label: 'Properties with spend', value: String((d.properties.properties ?? []).length), sub: d.properties.period, accent: 'white' },
  ] : [];

  return (
    <div className="relative min-h-screen overflow-hidden bg-[#182029] text-white">
      <nav className="sticky top-0 z-20 flex h-[76px] items-center justify-between border-b border-[#303A44] bg-[#182029]/95 px-6 backdrop-blur lg:px-10">
        <div className="brand"><svg viewBox="0 0 40 40" aria-hidden="true"><rect width="40" height="40" rx="10" fill="#40ccc1"/><path d="M11 17.5L20 10.5l9 7" fill="none" stroke="#002623" strokeWidth="2.6" strokeLinecap="round" strokeLinejoin="round"/><path d="M15 19.5v5a5 5 0 0010 0v-5" fill="none" stroke="#002623" strokeWidth="2.6" strokeLinecap="round"/></svg><span>UpliftStays</span></div>
        <span className="text-[0.72rem] font-semibold uppercase tracking-[0.14em] text-[#ABB5C0]">Cost Console</span>
      </nav>
      <main className="relative z-10 mx-auto max-w-[1600px] px-6 py-10 lg:px-10 lg:py-12">
        <div className="mb-10 grid gap-8 lg:grid-cols-[1.15fr_.85fr] lg:items-end">
          <div>
            <div className="mb-3 text-[0.72rem] font-medium uppercase tracking-[0.16em] text-[#40CCC1]">Staylio Cost Console</div>
            <h1 className="font-serif text-3xl font-normal leading-tight tracking-tight lg:text-4xl">What every build and property costs.</h1>
            <p className="mt-4 max-w-[780px] text-[1.02rem] font-light leading-8 text-[#ABB5C0]">Every figure comes from the pipeline's cost ledger, one row per vendor charge. Times are Eastern.</p>
          </div>
          <div className="rounded-2xl border border-[#303A44] bg-[#242C36] p-5 lg:p-6">
            <div className="mb-4 text-[0.7rem] font-semibold uppercase tracking-[0.14em] text-[#ABB5C0]">Period</div>
            <div className="flex flex-wrap gap-2">
              {periods.map(([label, key]) => (
                <button key={key} onClick={() => { setPeriod(key); if (key !== 'custom') setApplied(null); }}
                        className={`rounded-md border px-3 py-1.5 text-[0.8rem] font-medium transition ${period === key ? 'border-[#40CCC1]/50 bg-[#40CCC1]/20 text-[#40CCC1]' : 'border-[#303A44] bg-[#242C36] text-[#ABB5C0] hover:text-[#F4F7FB]'}`}>{label}</button>
              ))}
            </div>
            {period === 'custom' && (
              <div className="mt-4 flex flex-wrap items-end gap-2 border-t border-[#303A44] pt-4">
                <input type="date" value={customStart} onChange={e => setCustomStart(e.target.value)} className="rounded-md border border-[#303A44] bg-[#242C36] px-3 py-1.5 text-[0.8rem] [color-scheme:dark]" />
                <input type="date" value={customEnd} onChange={e => setCustomEnd(e.target.value)} className="rounded-md border border-[#303A44] bg-[#242C36] px-3 py-1.5 text-[0.8rem] [color-scheme:dark]" />
                <button disabled={!customStart || !customEnd} onClick={() => setApplied({ start: customStart, end: customEnd })} className="rounded-md border border-[#40CCC1]/50 bg-[#40CCC1]/20 px-3 py-1.5 text-[0.8rem] text-[#40CCC1] disabled:opacity-40">Apply</button>
              </div>
            )}
          </div>
        </div>

        {loading ? <LoadingState /> : error ? <ErrorState message={error} /> : (
          <>
            <section className="mb-10 grid gap-4 md:grid-cols-2 xl:grid-cols-3">{kpis.map(k => <MetricCard key={k.label} {...k} />)}</section>

            <section className="mb-10 grid gap-6 xl:grid-cols-[1.05fr_.95fr]">
              <Panel eyebrow="Spend over time" title="Daily spend (Eastern)"><SpendChart series={d.series.series ?? []} /></Panel>
              <Panel eyebrow="Why money was spent" title="Spend by purpose">
                <Bars rows={d.purposes.workflows ?? []} label={r => r.workflow_name} value={r => r.cost_usd} sub={r => `${r.event_count} charges · ${r.property_count} properties`} />
              </Panel>
            </section>

            <section className="mb-10 grid gap-6 xl:grid-cols-[1.2fr_.8fr]">
              <Panel flush eyebrow="Builds" title="Cost of each property build" body="A build's cost is its own charges plus every linked step's. Builds that ran before Oct 11, 2026 show only their own charges and are marked partial.">
                <Table cols={['Started', 'Property', 'Status', 'Total', 'Totals']} rows={builds} rowKey={b => b.run_id} selectedKey={build?.run_id} onRow={setBuild}
                       render={b => [et(b.started_at), b.property_name ?? '—', b.status, <span className="font-serif text-lg text-[#40CCC1]">{money(b.total_cost)}</span>, <LinkBadge b={b} />]} />
              </Panel>
              <Panel eyebrow={build ? 'Selected build' : 'Average per completed build'} title={build ? `${build.property_name ?? 'Build'} · ${money(build.total_cost)}` : (avg?.completed_linked_builds ? `${money(avg.cost_per_build)} over ${avg.completed_linked_builds} builds` : 'Waiting for a linked build')}
                     body={build ? `${et(build.started_at)} · ${build.status} · ${build.linked_steps} linked steps · run ${build.run_id}` : 'Click a build to see its vendor breakdown.'}>
                <VendorList vendors={build ? build.vendors : (avg?.vendors ?? []).map(v => ({ vendor_id: v.vendor_id, vendor_name: `${v.vendor_name} (in ${v.builds_using} of ${avg.completed_linked_builds})`, cost_usd: v.cost_per_build }))} />
                {build ? <div className="mt-6 border-t border-[#303A44] pt-5"><h3 className="mb-3 font-medium">Components</h3><ComponentList components={build.components} /></div> : <ComponentAverages averages={avg} />}
              </Panel>
            </section>

            <section className="mb-10 grid gap-6 xl:grid-cols-[1.2fr_.8fr]">
              <Panel flush eyebrow="Properties" title="Cost by property">
                <Table cols={['Property', 'Spend this period', 'Charges', 'Builds (all time)', 'Last activity', 'Account', 'Components']} rows={d.properties.properties ?? []} rowKey={p => p.property_id ?? 'none'} selectedKey={property}
                       onRow={p => p.property_id && setProperty(p.property_id)}
                       render={p => [p.property_name, <span className="font-serif text-lg text-[#40CCC1]">{money(p.total_cost)}</span>, p.event_count, p.builds_all_time, et(p.last_activity), p.account_name, <ComponentList components={p.components} />]} />
              </Panel>
              <Panel eyebrow="Selected property" title={propertyDetail?.property_name ?? (property ? 'Loading…' : 'Click a property')} body={propertyDetail && !propertyDetail.error ? `${money(propertyDetail.total_cost)} · ${propertyDetail.builds.length} builds · ${propertyDetail.period}` : propertyDetail?.error}>
                {propertyDetail && !propertyDetail.error && <VendorList vendors={propertyDetail.by_vendor} />}
                {propertyDetail && !propertyDetail.error && <div className="mt-6 border-t border-[#303A44] pt-5"><div className="mb-3 text-sm text-[#ABB5C0]">Account: {propertyDetail.account_name}</div><h3 className="mb-3 font-medium">Components</h3><ComponentList components={propertyDetail.components} /></div>}
              </Panel>
            </section>

            <section className="mb-10 space-y-6">
              <Panel flush eyebrow="Accounts" title="Account costs" body="Production recorded costs for every account. Spend and build counts follow the selected period; property counts include all owned properties.">
                {costViewsError ? <ErrorState message={costViewsError} /> : !costViews ? <LoadingState /> : (
                  <Table cols={['Account', 'Recorded cost', 'Properties', 'Builds', 'Components']} rows={costViews.accounts.accounts ?? []}
                         rowKey={a => a.account_id ?? 'unassigned'} selectedKey={account} onRow={a => a.account_id && setAccount(a.account_id)}
                         render={a => [a.account_name, money(a.total_cost), a.property_count, a.build_count, <ComponentList components={a.components} />]} />
                )}
              </Panel>
              {account && <Panel eyebrow="Selected account" title={accountDetail?.account_name ?? 'Loading account…'}>
                {accountDetail?.error ? <ErrorState message={accountDetail.error} /> : !accountDetail ? <LoadingState /> : (
                  <div className="space-y-6">
                    <div className="text-sm text-[#ABB5C0]">{money(accountDetail.total_cost)} recorded · {accountDetail.property_count} properties · {accountDetail.build_count} builds · {accountDetail.period}</div>
                    <ComponentList components={accountDetail.components} />
                    {accountDetail.properties.map(p => <div key={p.property_id ?? 'unassigned'} className="rounded-xl border border-[#303A44] p-5">
                      <button className="mb-3 text-left font-serif text-xl text-[#40CCC1]" onClick={() => p.property_id && setProperty(p.property_id)}>{p.property_name} · {money(p.total_cost)}</button>
                      <div className="mb-4 text-sm text-[#ABB5C0]">{p.build_count} builds in this period</div>
                      <ComponentList components={p.components} />
                      <div className="mt-5 space-y-3">{p.builds.map(b => <BuildCosts key={b.run_id} b={b} />)}</div>
                    </div>)}
                  </div>
                )}
              </Panel>}
            </section>

            <section className="mb-10 space-y-6">
              <Panel flush eyebrow="Property components" title="Property costs" body="Every Production property, including properties with no recorded spend. Click a property to see its builds, components and vendors.">
                {costViewsError ? <ErrorState message={costViewsError} /> : !costViews ? <LoadingState /> : (
                  <Table cols={['Property', 'Account', 'Recorded cost', 'Builds', 'Components']} rows={costViews.properties.properties ?? []}
                         rowKey={p => p.property_id ?? 'unassigned'} selectedKey={property} onRow={p => p.property_id && setProperty(p.property_id)}
                         render={p => [p.property_name, p.account_name, money(p.total_cost), p.build_count, <ComponentList components={p.components} />]} />
                )}
              </Panel>
              {property && <Panel eyebrow="Property builds" title={propertyDetail?.property_name ?? 'Loading property…'}>
                {propertyDetail?.error ? <ErrorState message={propertyDetail.error} /> : !propertyDetail ? <LoadingState /> : (
                  <div className="space-y-5">
                    <div className="text-sm text-[#ABB5C0]">Account: {propertyDetail.account_name} · {money(propertyDetail.total_cost)} recorded · {propertyDetail.build_count} builds in this period</div>
                    <ComponentList components={propertyDetail.components} />
                    <div className="space-y-3">{propertyDetail.builds.map(b => <BuildCosts key={b.run_id} b={b} />)}</div>
                  </div>
                )}
              </Panel>}
            </section>

            <section className="mb-10">
              <Panel flush eyebrow="Vendors" title="Spend by vendor this period">
                <Table cols={['Vendor', 'Category', 'Spend', 'Charges']} rows={d.vendors.vendors ?? []} rowKey={v => v.vendor_id}
                       render={v => [v.vendor_name, v.category, <span className="font-serif text-lg">{money(v.cost_usd)}</span>, v.event_count]} />
              </Panel>
            </section>
          </>
        )}
        <footer className="mt-14 border-t border-[#303A44] pt-6 text-[0.78rem] text-[#ABB5C0]">Source: pipeline cost ledger (public.cost_events), read-only · Eastern time</footer>
      </main>
    </div>
  );
}
