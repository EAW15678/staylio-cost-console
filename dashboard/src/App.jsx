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
const STICKY_BG = "bg-[#111e35]";

function money(v) { return v == null ? '—' : `$${Number(v).toFixed(2)}`; }
function et(iso) { return iso ? new Date(iso).toLocaleString('en-US', { timeZone: 'America/New_York', month: 'short', day: 'numeric', hour: 'numeric', minute: '2-digit' }) + ' ET' : '—'; }

function MetricCard({ label, value, sub, accent="white" }) {
  const c = accent==="teal"?"text-[#6dd5dc]":accent==="green"?"text-[#4caf7d]":"text-white";
  return (
    <div className="relative overflow-hidden rounded-2xl border border-white/10 bg-white/5 p-6">
      <div className={`absolute left-0 top-0 h-[3px] w-full ${accent==="white"?"bg-transparent":accent==="green"?"bg-gradient-to-r from-[#1A6B3A] to-[#4caf7d]":"bg-gradient-to-r from-[#0E6B72] to-[#6dd5dc]"}`} />
      <div className="mb-4 text-[0.68rem] font-bold uppercase tracking-[0.15em] text-white/35">{label}</div>
      <div className={`font-serif text-[2.3rem] leading-none ${c}`}>{value}</div>
      <div className="mt-3 text-[0.78rem] text-white/35">{sub}</div>
    </div>
  );
}

function SectionHeader({ eyebrow, title, body }) {
  return (
    <div className="mb-6">
      <div className="mb-2 text-[0.72rem] font-bold uppercase tracking-[0.16em] text-[#6dd5dc]">{eyebrow}</div>
      <h2 className="font-serif text-3xl leading-tight text-white">{title}</h2>
      {body && <p className="mt-2 max-w-[760px] text-sm leading-7 text-white/50">{body}</p>}
    </div>
  );
}


function Panel({ eyebrow, title, body, children, flush }) {
  return (
    <div className={`rounded-2xl border border-white/10 bg-white/5 ${flush ? 'overflow-hidden' : 'p-6 lg:p-8'}`}>
      <div className={flush ? 'px-6 pt-6 pb-4 lg:px-8 lg:pt-8' : ''}><SectionHeader eyebrow={eyebrow} title={title} body={body} /></div>
      {children}
    </div>
  );
}

function Table({ cols, rows, render, onRow, selectedKey, rowKey }) {
  return (
    <div className="overflow-x-auto">
      <table className="w-full min-w-[720px] border-collapse">
        <thead><tr className="border-y border-white/8 bg-white/[0.02]">
          {cols.map((h, i) => <th key={h} className={`px-6 py-4 text-left text-[0.68rem] font-bold uppercase tracking-[0.14em] text-white/30 ${i === 0 ? `sticky left-0 z-10 ${STICKY_BG}` : ''}`}>{h}</th>)}
        </tr></thead>
        <tbody>
          {rows.length === 0 && <tr><td colSpan={cols.length} className="px-6 py-6 text-sm text-white/40">Nothing recorded in this period.</td></tr>}
          {rows.map(r => (
            <tr key={rowKey(r)} onClick={onRow ? () => onRow(r) : undefined}
                className={`border-b border-white/6 transition hover:bg-white/[0.02] ${onRow ? 'cursor-pointer' : ''} ${selectedKey === rowKey(r) ? 'bg-white/[0.04]' : ''}`}>
              {render(r).map((cell, i) => <td key={i} className={`px-6 py-4 text-sm text-white/75 ${i === 0 ? `sticky left-0 z-10 ${STICKY_BG} font-medium text-white` : ''}`}>{cell}</td>)}
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
      {rows.length === 0 && <div className="text-sm text-white/40">Nothing recorded in this period.</div>}
      {rows.map(r => (
        <div key={label(r)}>
          <div className="mb-2 flex items-end justify-between gap-4">
            <div><div className="text-[0.98rem] font-medium text-white">{label(r)}</div><div className="mt-1 text-xs text-white/35">{sub(r)}</div></div>
            <div className="font-serif text-2xl text-white">{money(value(r))}</div>
          </div>
          <div className="h-2 overflow-hidden rounded-full bg-white/8"><div className="h-full rounded-full bg-gradient-to-r from-[#0E6B72] to-[#6dd5dc]" style={{ width: `${Math.max((value(r) / max) * 100, 2)}%` }} /></div>
        </div>
      ))}
    </div>
  );
}

function SpendChart({ series }) {
  const max = Math.max(...series.map(d => d.cost_usd), 0.001);
  return (
    <div className="h-[260px] rounded-xl border border-white/8 bg-white/[0.03] p-5">
      {series.length === 0 ? <div className="text-sm text-white/40">Nothing recorded in this period.</div> : (
        <div className="flex h-full items-end gap-2">
          {series.map(d => (
            <div key={d.date} className="flex flex-1 flex-col items-center justify-end gap-2" title={`${d.date}: ${money(d.cost_usd)} · ${d.event_count} charges`}>
              <div className="w-full rounded-t-md bg-gradient-to-t from-[#0E6B72] to-[#6dd5dc]" style={{ height: `${Math.max((d.cost_usd / max) * 190, 3)}px` }} />
              <div className="text-[0.62rem] text-white/30">{d.date.slice(5)}</div>
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
        <div key={v.vendor_id} className="flex justify-between gap-4 border-b border-white/6 pb-2 text-sm last:border-0">
          <span className="text-white/80">{v.vendor_name}{v.units != null && v.unit_name ? <span className="text-white/35"> · {Number(v.units).toLocaleString()} {v.unit_name}</span> : null}</span>
          <span className="font-serif text-white">{money(v.cost_usd ?? v.total_cost)}</span>
        </div>
      ))}
    </div>
  );
}

function LinkBadge({ b }) {
  return b.linked
    ? <span className="inline-flex rounded-full border border-[#0E6B72]/40 bg-[#0E6B72]/20 px-2.5 py-1 text-[0.7rem] font-semibold text-[#6dd5dc]">Complete total</span>
    : <span className="inline-flex rounded-full border border-amber-400/30 bg-amber-400/10 px-2.5 py-1 text-[0.7rem] font-semibold text-amber-300" title="Ran before the pipeline linked build steps (Oct 11, 2026): only the build's own charges are attached.">Partial — before build linking</span>;
}

function LoadingState() {
  return (
    <div className="rounded-2xl border border-white/10 bg-white/5 px-6 py-10">
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
      <div className="mb-3 text-[0.72rem] font-bold uppercase tracking-[0.16em] text-red-400">API Error</div>
      <div className="font-serif text-3xl text-white">Failed to load data.</div>
      <p className="mx-auto mt-3 max-w-[560px] text-sm leading-7 text-white/45">{message}</p>
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
    <div className="relative min-h-screen overflow-hidden bg-[#111e35] text-white">
      <nav className="sticky top-0 z-20 flex h-16 items-center justify-between border-b border-white/10 bg-[#1B2A4A]/95 px-6 backdrop-blur lg:px-10">
        <div className="font-serif text-[1.4rem] font-bold tracking-tight">Staylio<span className="text-[#6dd5dc]">.ai</span></div>
        <span className="text-[0.72rem] font-semibold uppercase tracking-[0.14em] text-white/35">Cost Console</span>
      </nav>
      <main className="relative z-10 mx-auto max-w-[1280px] px-6 py-10 lg:px-10 lg:py-12">
        <div className="mb-10 grid gap-8 lg:grid-cols-[1.15fr_.85fr] lg:items-end">
          <div>
            <div className="mb-3 text-[0.72rem] font-bold uppercase tracking-[0.16em] text-[#6dd5dc]">Staylio Cost Console</div>
            <h1 className="font-serif text-4xl leading-[1.04] tracking-tight lg:text-5xl">What every build and property costs.</h1>
            <p className="mt-4 max-w-[780px] text-[1.02rem] font-light leading-8 text-white/55">Every figure comes from the pipeline's cost ledger, one row per vendor charge. Times are Eastern.</p>
          </div>
          <div className="rounded-2xl border border-white/10 bg-white/5 p-5 lg:p-6">
            <div className="mb-4 text-[0.7rem] font-semibold uppercase tracking-[0.14em] text-white/35">Period</div>
            <div className="flex flex-wrap gap-2">
              {periods.map(([label, key]) => (
                <button key={key} onClick={() => { setPeriod(key); if (key !== 'custom') setApplied(null); }}
                        className={`rounded-md border px-3 py-1.5 text-[0.8rem] font-medium transition ${period === key ? 'border-[#0E6B72]/50 bg-[#0E6B72]/20 text-[#6dd5dc]' : 'border-white/10 bg-white/5 text-white/55 hover:text-white/85'}`}>{label}</button>
              ))}
            </div>
            {period === 'custom' && (
              <div className="mt-4 flex flex-wrap items-end gap-2 border-t border-white/8 pt-4">
                <input type="date" value={customStart} onChange={e => setCustomStart(e.target.value)} className="rounded-md border border-white/10 bg-white/5 px-3 py-1.5 text-[0.8rem] [color-scheme:dark]" />
                <input type="date" value={customEnd} onChange={e => setCustomEnd(e.target.value)} className="rounded-md border border-white/10 bg-white/5 px-3 py-1.5 text-[0.8rem] [color-scheme:dark]" />
                <button disabled={!customStart || !customEnd} onClick={() => setApplied({ start: customStart, end: customEnd })} className="rounded-md border border-[#0E6B72]/50 bg-[#0E6B72]/20 px-3 py-1.5 text-[0.8rem] text-[#6dd5dc] disabled:opacity-40">Apply</button>
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
                       render={b => [et(b.started_at), b.property_name ?? '—', b.status, <span className="font-serif text-lg text-[#6dd5dc]">{money(b.total_cost)}</span>, <LinkBadge b={b} />]} />
              </Panel>
              <Panel eyebrow={build ? 'Selected build' : 'Average per completed build'} title={build ? `${build.property_name ?? 'Build'} · ${money(build.total_cost)}` : (avg?.completed_linked_builds ? `${money(avg.cost_per_build)} over ${avg.completed_linked_builds} builds` : 'Waiting for a linked build')}
                     body={build ? `${et(build.started_at)} · ${build.status} · ${build.linked_steps} linked steps · run ${build.run_id}` : 'Click a build to see its vendor breakdown.'}>
                <VendorList vendors={build ? build.vendors : (avg?.vendors ?? []).map(v => ({ vendor_id: v.vendor_id, vendor_name: `${v.vendor_name} (in ${v.builds_using} of ${avg.completed_linked_builds})`, cost_usd: v.cost_per_build }))} />
              </Panel>
            </section>

            <section className="mb-10 grid gap-6 xl:grid-cols-[1.2fr_.8fr]">
              <Panel flush eyebrow="Properties" title="Cost by property">
                <Table cols={['Property', 'Spend this period', 'Charges', 'Builds (all time)', 'Last activity']} rows={d.properties.properties ?? []} rowKey={p => p.property_id ?? 'none'} selectedKey={property}
                       onRow={p => p.property_id && setProperty(p.property_id)}
                       render={p => [p.property_name, <span className="font-serif text-lg text-[#6dd5dc]">{money(p.total_cost)}</span>, p.event_count, p.builds_all_time, et(p.last_activity)]} />
              </Panel>
              <Panel eyebrow="Selected property" title={propertyDetail?.property_name ?? (property ? 'Loading…' : 'Click a property')} body={propertyDetail && !propertyDetail.error ? `${money(propertyDetail.total_cost)} · ${propertyDetail.builds.length} builds · ${propertyDetail.period}` : propertyDetail?.error}>
                {propertyDetail && !propertyDetail.error && <VendorList vendors={propertyDetail.by_vendor} />}
              </Panel>
            </section>

            <section className="mb-10">
              <Panel flush eyebrow="Vendors" title="Spend by vendor this period">
                <Table cols={['Vendor', 'Category', 'Spend', 'Charges']} rows={d.vendors.vendors ?? []} rowKey={v => v.vendor_id}
                       render={v => [v.vendor_name, v.category, <span className="font-serif text-lg">{money(v.cost_usd)}</span>, v.event_count]} />
              </Panel>
            </section>
          </>
        )}
        <footer className="mt-14 border-t border-white/8 pt-6 text-[0.78rem] text-white/30">Source: pipeline cost ledger (public.cost_events), read-only · Eastern time</footer>
      </main>
    </div>
  );
}
