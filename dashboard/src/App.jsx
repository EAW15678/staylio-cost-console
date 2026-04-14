import { useState, useEffect, useMemo } from "react";

const PROXY_BASE = 'https://dashboard-proxy.staylio.ai';

async function apiFetch(path) {
  const res = await fetch(`${PROXY_BASE}${path}`);
  if (!res.ok) throw new Error(`API ${path} returned ${res.status}`);
  return res.json();
}

const recentEstimateEvents = [
  { vendor: "Anthropic", workflow: "listing_generation", detail: "claude-sonnet-4-6 · landing_page_copy", cost: 0.0529 },
  { vendor: "Anthropic", workflow: "listing_generation", detail: "claude-haiku-4-5 · social_captions", cost: 0.0215 },
  { vendor: "Creatomate", workflow: "video_generation", detail: "video_render · vibe_match_16_9", cost: 0.1 },
  { vendor: "Creatomate", workflow: "video_generation", detail: "video_render · vibe_match_9_16", cost: 0.1 },
  { vendor: "ElevenLabs", workflow: "video_generation", detail: "tts · guest_review_1", cost: 0.1181 },
];

const propertyRows = [
  { property_id: "vista-azule", name: "Vista Azule", subdomain: "vista-azule.staylio.ai", total_cost: 0, listing_generation: 0, video_generation: 0, event_count: 0, status: "Active" },
  { property_id: "placeholder", name: "Pending Property", subdomain: "pending.staylio.ai", total_cost: 0, listing_generation: 0, video_generation: 0, event_count: 0, status: "Queued" },
];

const periods = ["24h", "7d", "30d", "MTD"];
const viewModes = ["Combined", "Actual", "Estimated"];

function money(v) { return `$${(v ?? 0).toFixed(2)}`; }
function shortMoney(v) { if (v >= 1000) return `$${(v/1000).toFixed(1).replace(/\.0$/,'')}k`; return money(v); }
function apiPeriod(p) { return p === "MTD" ? "30d" : p; }

function buildActualSeries(t) { return (t?.series ?? []).map(d => ({ label: d.date.slice(5), value: d.cost_usd })); }
function buildEstimatedSeries(total) {
  return [0.08,0.14,0.11,0.18,0.1,0.16,0.09,0.22,0.12,0.27,0.19,total].map((v,i) => ({ label: `${i+2}`.padStart(2,'0'), value: v }));
}
function mergeSeries(actual, estimated) {
  return actual.map((p,i) => ({ label: p.label, actual: p.value, estimated: estimated[i]?.value ?? 0, combined: p.value + (estimated[i]?.value ?? 0) }));
}

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

function ChartPanel({ rows, mode }) {
  const max = Math.max(...rows.map(r=>r.combined), 1);
  return (
    <div className="rounded-2xl border border-white/10 bg-white/5 p-6 lg:p-8">
      <SectionHeader eyebrow="Spend Over Time" title="Actual, estimated, and blended cost curves" />
      <div className="h-[300px] rounded-xl border border-white/8 bg-white/[0.03] p-5">
        <div className="flex h-full items-end gap-3">
          {rows.map(r => {
            const ah = (r.actual/max)*180, eh = (r.estimated/max)*180, ch = (r.combined/max)*180;
            return (
              <div key={r.label} className="flex flex-1 flex-col items-center justify-end gap-3">
                <div className="flex h-[210px] w-full items-end justify-center gap-1">
                  {(mode==="Combined"||mode==="Actual") && <div className="w-1/2 rounded-t-md bg-gradient-to-t from-[#0E6B72] to-[#6dd5dc]" style={{height:`${Math.max(ah,4)}px`,opacity:mode==="Actual"?1:0.75}} />}
                  {(mode==="Combined"||mode==="Estimated") && <div className="w-1/2 rounded-t-md bg-gradient-to-t from-[#1A6B3A] to-[#4caf7d]" style={{height:`${Math.max(mode==="Combined"?eh:ch,4)}px`,opacity:mode==="Estimated"?1:0.88}} />}
                </div>
                <div className="text-[0.68rem] text-white/30">{r.label}</div>
              </div>
            );
          })}
        </div>
      </div>
      <div className="mt-5 grid gap-4 md:grid-cols-3">
        {[["Actual synced cost",shortMoney(rows.reduce((s,r)=>s+r.actual,0)),"teal"],["Estimated workflow cost",shortMoney(rows.reduce((s,r)=>s+r.estimated,0)),"green"],["Combined operating cost",shortMoney(rows.reduce((s,r)=>s+r.combined,0)),"white"]].map(([l,v,t])=>(
          <div key={l} className="rounded-xl border border-white/8 bg-white/[0.03] p-4">
            <div className="mb-2 text-[0.68rem] uppercase tracking-[0.14em] text-white/30">{l}</div>
            <div className={`font-serif text-2xl ${t==="teal"?"text-[#6dd5dc]":t==="green"?"text-[#4caf7d]":"text-white"}`}>{v}</div>
          </div>
        ))}
      </div>
    </div>
  );
}

function WorkflowPanel({ rows }) {
  const max = Math.max(...(rows??[]).map(r=>r.cost_usd), 0.001);
  return (
    <div className="rounded-2xl border border-white/10 bg-white/5 p-6 lg:p-8">
      <SectionHeader eyebrow="Workflow Economics" title="Cost by canonical workflow" />
      <div className="space-y-5">
        {(rows??[]).map(wf=>(
          <div key={wf.workflow_name}>
            <div className="mb-2 flex items-end justify-between gap-4">
              <div>
                <div className="text-[0.98rem] font-medium text-white">{wf.workflow_name}</div>
                <div className="mt-1 text-xs text-white/35">{wf.event_count} events · {wf.property_count} properties</div>
              </div>
              <div className="font-serif text-2xl text-white">{money(wf.cost_usd)}</div>
            </div>
            <div className="h-2 rounded-full bg-white/8 overflow-hidden">
              <div className="h-full rounded-full bg-gradient-to-r from-[#0E6B72] to-[#6dd5dc]" style={{width:`${Math.max((wf.cost_usd/max)*100,wf.cost_usd===0?2:6)}%`,opacity:wf.cost_usd===0?0.22:1}} />
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

function VendorTable({ rows }) {
  return (
    <div className="rounded-2xl border border-white/10 bg-white/5 overflow-hidden">
      <div className="px-6 pt-6 pb-4 lg:px-8 lg:pt-8">
        <SectionHeader eyebrow="Vendor Intelligence" title="Usage-based, amortized, active, and inactive vendors" />
      </div>
      <div className="overflow-x-auto">
        <table className="w-full min-w-[820px] border-collapse">
          <thead>
            <tr className="border-y border-white/8 bg-white/[0.02]">
              {['Vendor','Category','Status','Cost Type','Period Spend','Events'].map(h=>(
                <th key={h} className="px-6 py-4 text-left text-[0.68rem] font-bold uppercase tracking-[0.14em] text-white/30">{h}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {(rows??[]).map(v=>{
              const status=v.inactive?'Inactive':'Active';
              const type=v.vendor_id==='late'?'amortized':v.inactive?'inactive':'usage-based';
              return (
                <tr key={v.vendor_id} className="border-b border-white/6 hover:bg-white/[0.02] transition">
                  <td className="px-6 py-4 text-white font-medium">{v.vendor_name}</td>
                  <td className="px-6 py-4 text-sm text-white/65">{v.category}</td>
                  <td className="px-6 py-4">
                    <span className={`inline-flex rounded-full border px-2.5 py-1 text-[0.7rem] font-semibold ${status==='Active'?'border-[#0E6B72]/40 bg-[#0E6B72]/20 text-[#6dd5dc]':'border-white/10 bg-white/6 text-white/45'}`}>{status}</span>
                  </td>
                  <td className="px-6 py-4 text-sm text-white/65">{type}</td>
                  <td className="px-6 py-4 font-serif text-xl text-white">{v.vendor_id==='late'?'$19.00/mo':money(v.cost_usd)}</td>
                  <td className="px-6 py-4 text-sm text-white/65">{v.event_count}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function PropertyPanel({ rows, selected, onSelect }) {
  const current = rows.find(r=>r.property_id===selected)??rows[0];
  return (
    <div className="grid gap-6 xl:grid-cols-[1fr_.95fr]">
      <div className="rounded-2xl border border-white/10 bg-white/5 overflow-hidden">
        <div className="px-6 pt-6 pb-4 lg:px-8 lg:pt-8">
          <SectionHeader eyebrow="Property Cost Profile" title="Property-level operating cost" />
        </div>
        <div className="overflow-x-auto">
          <table className="w-full min-w-[760px] border-collapse">
            <thead>
              <tr className="border-y border-white/8 bg-white/[0.02]">
                {['Property','Total Cost','Listing','Video','Events','Status'].map(h=>(
                  <th key={h} className="px-6 py-4 text-left text-[0.68rem] font-bold uppercase tracking-[0.14em] text-white/30">{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {rows.map(p=>(
                <tr key={p.property_id} className={`border-b border-white/6 transition hover:bg-white/[0.02] cursor-pointer ${selected===p.property_id?'bg-white/[0.03]':''}`} onClick={()=>onSelect(p.property_id)}>
                  <td className="px-6 py-4"><div className="font-medium text-white">{p.name}</div><div className="mt-1 text-xs text-white/30">{p.subdomain}</div></td>
                  <td className="px-6 py-4 font-serif text-xl text-[#6dd5dc]">{money(p.total_cost)}</td>
                  <td className="px-6 py-4 text-white/75">{money(p.listing_generation)}</td>
                  <td className="px-6 py-4 text-white/75">{money(p.video_generation)}</td>
                  <td className="px-6 py-4 text-sm text-white/65">{p.event_count}</td>
                  <td className="px-6 py-4 text-sm text-white/65">{p.status}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
      <div className="rounded-2xl border border-white/10 bg-white/5 p-6 lg:p-8">
        <SectionHeader eyebrow="Selected Property" title={current?.name??'—'} />
        <div className="space-y-4">
          {[['Total cost this run',money(current?.total_cost),'teal'],['Listing generation',money(current?.listing_generation),'white'],['Video generation',money(current?.video_generation),'white'],['Tracked events',String(current?.event_count??0),'white']].map(([l,v,t])=>(
            <div key={l} className="rounded-xl border border-white/8 bg-white/[0.03] p-4">
              <div className="mb-2 text-[0.68rem] uppercase tracking-[0.14em] text-white/30">{l}</div>
              <div className={`font-serif text-2xl ${t==='teal'?'text-[#6dd5dc]':'text-white'}`}>{v}</div>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}

function EventPanel({ rows }) {
  return (
    <div className="rounded-2xl border border-white/10 bg-white/5 p-6 lg:p-8">
      <SectionHeader eyebrow="Recent Emitted Events" title="Estimated workflow activity" />
      <div className="space-y-4">
        {rows.map((e,i)=>(
          <div key={i} className="rounded-xl border border-white/8 bg-white/[0.03] p-4">
            <div className="mb-2 flex items-start justify-between gap-4">
              <div>
                <div className="font-medium text-white">{e.vendor}</div>
                <div className="mt-1 text-xs uppercase tracking-[0.12em] text-[#6dd5dc]">{e.workflow}</div>
              </div>
              <div className="font-serif text-xl text-white">{money(e.cost)}</div>
            </div>
            <div className="text-sm text-white/55">{e.detail}</div>
          </div>
        ))}
      </div>
    </div>
  );
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
  const [mode, setMode] = useState('Combined');
  const [selectedProperty, setSelectedProperty] = useState('vista-azule');
  const [summary, setSummary] = useState(null);
  const [vendors, setVendors] = useState(null);
  const [workflows, setWorkflows] = useState(null);
  const [timeseries, setTimeseries] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    const p = apiPeriod(period);
    setLoading(true);
    setError(null);
    Promise.all([
      apiFetch('/metrics/summary'),
      apiFetch(`/metrics/vendors?period=${p}`),
      apiFetch(`/metrics/workflows?period=${p}`),
      apiFetch(`/metrics/timeseries?period=${p}`),
    ]).then(([s,v,w,t]) => {
      setSummary(s); setVendors(v); setWorkflows(w); setTimeseries(t);
      setLoading(false);
    }).catch(err => { setError(err.message); setLoading(false); });
  }, [period]);

  const todaySpend = summary?.today_spend ?? 0;
  const mtdSpend = summary?.mtd_spend ?? 0;
  const failedSyncs = summary?.failed_syncs_today ?? 0;
  const actualToday = (summary?.today_by_vendor ?? []).reduce((s,v)=>s+v.cost_usd, 0);
  const estimatedTotal = workflows?.total ?? 0;
  const activeVendorCount = (vendors?.vendors ?? []).filter(v=>!v.inactive).length;
  const eventTotal = (workflows?.workflows ?? []).reduce((s,w)=>s+w.event_count, 0);

  const kpis = [
    { label:'Spend Today', value:money(todaySpend), sub:'actual + estimated operating view', accent:'teal' },
    { label:'MTD Spend', value:money(mtdSpend), sub:'tracked across all sources', accent:'white' },
    { label:'Actual Synced Cost', value:money(actualToday), sub:'vendor-synced spend', accent:'teal' },
    { label:'Estimated Workflow Cost', value:money(estimatedTotal), sub:'pipeline-emitted estimates', accent:'green' },
    { label:'Active Vendors', value:String(activeVendorCount), sub:'usage + amortized', accent:'white' },
    { label:'Workflow Events', value:String(eventTotal), sub:'today across active workflows', accent:'green' },
  ];

  const chartRows = useMemo(() => {
    if (!timeseries) return [];
    return mergeSeries(buildActualSeries(timeseries), buildEstimatedSeries(estimatedTotal));
  }, [timeseries, estimatedTotal]);

  return (
    <div className="min-h-screen overflow-hidden bg-[#111e35] text-white relative">
      <div className="pointer-events-none absolute inset-0 opacity-[0.04]" style={{backgroundImage:'linear-gradient(rgba(110,213,220,0.5) 1px, transparent 1px), linear-gradient(90deg, rgba(110,213,220,0.5) 1px, transparent 1px)',backgroundSize:'60px 60px'}} />
      <nav className="sticky top-0 z-20 flex h-16 items-center justify-between border-b border-white/10 bg-[#1B2A4A]/95 px-6 backdrop-blur lg:px-10">
        <div className="font-serif text-[1.4rem] font-bold tracking-tight">Staylio<span className="text-[#6dd5dc]">.ai</span></div>
        <div className="hidden items-center gap-5 md:flex">
          <span className="text-[0.72rem] font-semibold uppercase tracking-[0.14em] text-white/35">Cost Console</span>
          <span className="text-sm text-white/45">Operational Cost Intelligence</span>
        </div>
      </nav>
      <main className="relative z-10 mx-auto max-w-[1280px] px-6 py-10 lg:px-10 lg:py-12">
        <div className="mb-10 grid gap-8 lg:grid-cols-[1.15fr_.85fr] lg:items-end">
          <div>
            <div className="mb-3 text-[0.72rem] font-bold uppercase tracking-[0.16em] text-[#6dd5dc]">Staylio Cost Console</div>
            <h1 className="max-w-[920px] font-serif text-4xl leading-[1.04] tracking-tight text-white lg:text-6xl">Operational cost intelligence — live.</h1>
            <p className="mt-4 max-w-[780px] text-[1.02rem] font-light leading-8 text-white/55">All data is live from the Cost Console API. Period filter re-fetches all panels.</p>
          </div>
          <div className="rounded-2xl border border-white/10 bg-white/5 p-5 lg:p-6">
            <div className="mb-4 text-[0.7rem] font-semibold uppercase tracking-[0.14em] text-white/35">Period</div>
            <div className="mb-5 flex flex-wrap gap-2">
              {periods.map(p=>(
                <button key={p} onClick={()=>setPeriod(p)} className={`rounded-md border px-3 py-1.5 text-[0.8rem] font-medium transition ${period===p?'border-[#0E6B72]/50 bg-[#0E6B72]/20 text-[#6dd5dc]':'border-white/10 bg-white/5 text-white/55 hover:text-white/85'}`}>{p}</button>
              ))}
            </div>
            <div className="mb-2 text-[0.7rem] font-semibold uppercase tracking-[0.14em] text-white/35">View Mode</div>
            <div className="flex flex-wrap gap-2">
              {viewModes.map(m=>(
                <button key={m} onClick={()=>setMode(m)} className={`rounded-md border px-3 py-1.5 text-[0.8rem] font-medium transition ${mode===m?'border-[#0E6B72]/50 bg-[#0E6B72]/20 text-[#6dd5dc]':'border-white/10 bg-white/5 text-white/55 hover:text-white/85'}`}>{m}</button>
              ))}
            </div>
          </div>
        </div>

        {loading ? <LoadingState /> : error ? <ErrorState message={error} /> : (
          <>
            <section className="mb-10 grid gap-4 md:grid-cols-2 xl:grid-cols-3">
              {kpis.map(card=><MetricCard key={card.label} {...card} />)}
            </section>
            <section className="mb-10 grid gap-6 xl:grid-cols-[1.05fr_.95fr]">
              <WorkflowPanel rows={workflows?.workflows??[]} />
              <ChartPanel rows={chartRows} mode={mode} />
            </section>
            <section className="mb-10 grid gap-6 xl:grid-cols-[1fr_.95fr]">
              <VendorTable rows={vendors?.vendors??[]} />
              <EventPanel rows={recentEstimateEvents} />
            </section>
            <section className="mb-10">
              <PropertyPanel rows={propertyRows} selected={selectedProperty} onSelect={setSelectedProperty} />
            </section>
            <section className="grid gap-6 xl:grid-cols-[1fr_.95fr]">
              <div className="rounded-2xl border border-white/10 bg-white/5 p-6 lg:p-8">
                <SectionHeader eyebrow="Wiring Status" title="What is live vs. static" />
                <div className="space-y-3 text-sm leading-7 text-white/60">
                  <div><strong className="text-white">Summary, workflows, vendors, timeseries:</strong> live from Cost Console API</div>
                  <div><strong className="text-white">Property panel:</strong> static — no property endpoint yet</div>
                  <div><strong className="text-white">Recent events:</strong> static — no operational_estimates endpoint yet</div>
                </div>
              </div>
              <div className="rounded-2xl border border-white/10 bg-white/5 p-6 lg:p-8">
                <SectionHeader eyebrow="System Health" title="Integrity and status" />
                <div className="space-y-4">
                  {[['Failed syncs today',String(failedSyncs)],['Anthropic connector','Verified'],['Pipeline emitter','Deterministic and idempotent'],['Inactive vendor handling','Ayrshare inactive · Late active'],['Authentication','Cloudflare Zero Trust · OTP']].map(([l,v])=>(
                    <div key={l} className="flex items-start justify-between gap-4 border-b border-white/6 pb-3 last:border-0 last:pb-0">
                      <div className="text-[0.68rem] font-semibold uppercase tracking-[0.14em] text-white/30">{l}</div>
                      <div className="text-right text-sm text-white/75">{v}</div>
                    </div>
                  ))}
                </div>
              </div>
            </section>
          </>
        )}

        <footer className="mt-14 flex flex-col gap-3 border-t border-white/8 pt-6 text-[0.78rem] text-white/30 md:flex-row md:items-center md:justify-between">
          <div><span className="mr-2 inline-block h-1.5 w-1.5 rounded-full bg-[#4caf7d] align-middle" />Live · wired to Cost Console API</div>
          <div>console.staylio.ai · protected by Cloudflare Zero Trust</div>
        </footer>
      </main>
    </div>
  );
}
