#!/usr/bin/env python3
"""Build one self-contained interactive report from benchmark JSON results."""

from __future__ import annotations

import argparse
import json
import webbrowser
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SUITES = {
    "baseline": ("Baseline", "comparison.json", "query"),
    "extended": ("Extended", "extended/comparison.json", "query"),
    "sql": ("SQL capabilities", "sql-v1/comparison.json", "query"),
    "optimizer": ("Query optimizer", "optimizer/optimizer.json", "optimizer"),
    "concurrency": (
        "Concurrent workload",
        "concurrency/concurrency.json",
        "concurrency",
    ),
    "storage": ("Storage formats", "storage/summary.json", "storage"),
    "parquet": ("Direct Parquet", "parquet/parquet.json", "parquet"),
    "memory": ("Memory bounded", "memory/comparison.json", "query"),
}


def read_environment(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    environment = {}
    for line in path.read_text().splitlines():
        if ": " in line:
            key, value = line.split(": ", 1)
            environment[key] = value
    return environment


def load_report_data(results: Path, included: list[str]) -> dict:
    suites = []
    for key in included:
        title, relative, kind = SUITES[key]
        path = results / relative
        if not path.exists():
            raise FileNotFoundError(f"missing {title} results: {path}")
        suites.append(
            {
                "key": key,
                "title": title,
                "kind": kind,
                "source": str(path.resolve()),
                "data": json.loads(path.read_text()),
            }
        )
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "suites": suites,
        "environment": read_environment(results / "environment.txt"),
    }


HTML = r'''<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="color-scheme" content="dark light">
<title>Dremel benchmark report</title>
<style>
:root{--bg:#07111f;--panel:#0d1b2c;--panel2:#11243a;--line:#203650;--text:#eaf2ff;--muted:#8fa7c2;--rust:#ff7a45;--cpp:#49a8ff;--good:#36d399;--warn:#f5bd4f;--bad:#fb7185;--shadow:0 18px 48px #0005;--radius:16px}
:root.light{--bg:#f4f7fb;--panel:#fff;--panel2:#edf3fa;--line:#d6e0eb;--text:#142136;--muted:#5d7087;--shadow:0 12px 35px #20365018}
*{box-sizing:border-box}html{scroll-behavior:smooth}body{margin:0;background:radial-gradient(circle at 90% 0,#12345b55 0,transparent 34%),var(--bg);color:var(--text);font:14px/1.5 Inter,ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;font-variant-numeric:tabular-nums}
button,input,select{font:inherit}.shell{max-width:1500px;margin:auto;padding:26px}.hero{display:flex;justify-content:space-between;align-items:flex-start;gap:20px;padding:30px;background:linear-gradient(135deg,#142b48,#0d1b2c 60%,#17345a);border:1px solid #294a70;border-radius:22px;box-shadow:var(--shadow)}
.eyebrow{color:#79bcff;text-transform:uppercase;letter-spacing:.16em;font-size:11px;font-weight:800}.hero h1{font-size:clamp(28px,4vw,48px);line-height:1.05;margin:8px 0 10px;letter-spacing:-.04em}.hero p{color:#b6c8dc;margin:0;max-width:740px}.hero-actions{display:flex;gap:9px;flex-wrap:wrap;justify-content:flex-end}.button{border:1px solid #355679;background:#172c46;color:#dcecff;border-radius:10px;padding:9px 12px;cursor:pointer}.button:hover{background:#203b5c}.status{display:inline-flex;align-items:center;gap:7px}.dot{width:8px;height:8px;border-radius:50%;background:var(--good);box-shadow:0 0 12px var(--good)}
.nav{position:sticky;top:0;z-index:10;display:flex;gap:8px;overflow:auto;margin:18px 0;padding:10px;background:#07111fdd;backdrop-filter:blur(14px);border:1px solid var(--line);border-radius:13px}.nav button{white-space:nowrap;border:0;background:transparent;color:var(--muted);padding:8px 11px;border-radius:8px;cursor:pointer}.nav button.active,.nav button:hover{background:var(--panel2);color:var(--text)}
.section{scroll-margin-top:82px;margin:18px 0;padding:24px;background:var(--panel);border:1px solid var(--line);border-radius:var(--radius);box-shadow:var(--shadow)}.section-head{display:flex;justify-content:space-between;gap:16px;align-items:end;margin-bottom:18px}.section h2{font-size:23px;margin:0;letter-spacing:-.02em}.section-sub{color:var(--muted);margin-top:4px}.grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:12px}.metric{min-height:105px;padding:17px;border:1px solid var(--line);background:linear-gradient(145deg,var(--panel2),var(--panel));border-radius:13px}.metric .label{font-size:11px;font-weight:800;letter-spacing:.1em;text-transform:uppercase;color:var(--muted)}.metric .value{font-size:25px;font-weight:800;margin-top:9px;line-height:1.1}.metric .note{color:var(--muted);font-size:12px;margin-top:7px}.rust{color:var(--rust)}.cpp{color:var(--cpp)}.good{color:var(--good)}.warn{color:var(--warn)}
.suite-grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:12px;margin-top:16px}.suite-card{padding:17px;border:1px solid var(--line);border-radius:13px;background:var(--panel2);cursor:pointer;transition:.18s transform,.18s border-color}.suite-card:hover{transform:translateY(-2px);border-color:#4f7eaa}.suite-card h3{margin:0 0 6px;font-size:16px}.suite-card p{color:var(--muted);margin:0}.split{display:flex;height:8px;overflow:hidden;background:#25364b;border-radius:99px;margin:14px 0 8px}.split span{display:block}.legend{display:flex;gap:14px;color:var(--muted);font-size:12px}.legend i{display:inline-block;width:8px;height:8px;border-radius:50%;margin-right:5px}
.controls{display:flex;gap:9px;flex-wrap:wrap;margin:18px 0 11px}.controls input,.controls select{color:var(--text);background:var(--panel2);border:1px solid var(--line);border-radius:9px;padding:9px 11px;outline:none}.controls input{min-width:250px}.controls input:focus,.controls select:focus{border-color:#4b8bc7}.table-wrap{overflow:auto;border:1px solid var(--line);border-radius:12px}table{width:100%;border-collapse:collapse;white-space:nowrap}th,td{padding:10px 12px;border-bottom:1px solid var(--line);text-align:right}th{position:sticky;top:0;background:var(--panel2);color:var(--muted);font-size:11px;text-transform:uppercase;letter-spacing:.07em;cursor:pointer;z-index:1}th:first-child,td:first-child,th:nth-child(2),td:nth-child(2){text-align:left}tbody tr:hover{background:#4c79a70c}tbody tr:last-child td{border-bottom:0}.pill{display:inline-flex;padding:3px 8px;border-radius:99px;font-size:11px;font-weight:800}.pill.Rust{background:#ff7a4520;color:var(--rust)}.pill.C\+\+{background:#49a8ff20;color:var(--cpp)}.pill.TIE,.pill.PASS{background:#36d39920;color:var(--good)}
.pair{display:grid;grid-template-columns:70px 1fr 72px;gap:8px;align-items:center;margin:8px 0}.track{height:9px;background:#26394f;border-radius:99px;overflow:hidden}.fill{height:100%;border-radius:99px;min-width:2px}.muted{color:var(--muted)}.callout{margin:15px 0;padding:14px 16px;border-left:3px solid #4da7f5;background:#49a8ff0b;border-radius:0 10px 10px 0}.environment{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:0 20px}.env-row{display:flex;justify-content:space-between;gap:14px;padding:9px 0;border-bottom:1px solid var(--line)}.env-row span:first-child{color:var(--muted)}.footer{text-align:center;color:var(--muted);padding:22px}.empty{padding:28px;color:var(--muted);text-align:center}
@media(max-width:900px){.grid{grid-template-columns:repeat(2,1fr)}.suite-grid,.environment{grid-template-columns:1fr}.hero{flex-direction:column}.hero-actions{justify-content:flex-start}.shell{padding:14px}}@media(max-width:520px){.grid{grid-template-columns:1fr}.section{padding:17px}.controls input{min-width:100%}}
@media print{.nav,.hero-actions,.controls{display:none}.shell{max-width:none;padding:0}.section,.hero{box-shadow:none;break-inside:avoid}body{background:#fff}}
</style>
</head>
<body>
<main class="shell">
  <header class="hero">
    <div><div class="eyebrow">Apples-to-apples engine evaluation</div><h1>Dremel benchmark report</h1><p id="hero-copy"></p></div>
    <div class="hero-actions"><button class="button" id="theme">Toggle theme</button><button class="button" onclick="window.print()">Print or save PDF</button></div>
  </header>
  <nav class="nav" id="nav"></nav>
  <div id="report"></div>
  <footer class="footer" id="footer"></footer>
</main>
<script type="application/json" id="report-data">__REPORT_DATA__</script>
<script>
const REPORT=JSON.parse(document.getElementById('report-data').textContent);
const $=(s,e=document)=>e.querySelector(s), $$=(s,e=document)=>[...e.querySelectorAll(s)];
const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const num=(v,d=2)=>Number.isFinite(Number(v))?Number(v).toLocaleString(undefined,{maximumFractionDigits:d}):'N/A';
const ms=v=>Number.isFinite(Number(v))?`${num(v,3)} ms`:'N/A';
const fromNs=v=>Number.isFinite(Number(v))?ms(Number(v)/1e6):'N/A';
const bytes=v=>{if(!Number.isFinite(Number(v)))return 'N/A';let n=Number(v),u=['B','KiB','MiB','GiB'],i=0;while(n>=1024&&i<u.length-1){n/=1024;i++}return `${num(n,i?2:0)} ${u[i]}`};
const percent=v=>`${num(v,1)}%`;
const pill=v=>`<span class="pill ${esc(v)}">${esc(v)}</span>`;
const metric=(label,value,note='',cls='')=>`<article class="metric"><div class="label">${esc(label)}</div><div class="value ${cls}">${value}</div><div class="note">${note}</div></article>`;
const section=(id,title,sub,body)=>`<section class="section" id="${id}"><div class="section-head"><div><h2>${esc(title)}</h2><div class="section-sub">${esc(sub)}</div></div></div>${body}</section>`;
const geo=values=>{const valid=values.filter(v=>Number.isFinite(v)&&v>0);return valid.length?Math.exp(valid.reduce((a,v)=>a+Math.log(v),0)/valid.length):NaN};
const suiteByKey=k=>REPORT.suites.find(s=>s.key===k);
function querySummary(s){const d=s.data,q=d.queries||[],ok=q.filter(x=>x.correct).length,r=d.rust_cpp_geometric_mean;return {count:q.length,ok,winner:r<.99?'Rust':r>1.01?'C++':'TIE',note:Number.isFinite(r)?`Rust/C++ geometric mean ${num(r,4)}x`:'No timing data'} }
function optimizerSummary(s){const q=s.data.queries||[],rs=q.map(x=>x.rust_speedup).filter(Number.isFinite),cs=q.map(x=>x.cpp_speedup).filter(Number.isFinite);return {count:q.length,ok:q.filter(x=>x.correct).length,winner:'PASS',note:`Optimizer speedup: Rust ${num(geo(rs),2)}x, C++ ${num(geo(cs),2)}x`}}
function concurrencySummary(s){const r=s.data.rust.summary,c=s.data.cpp.summary,w=c.throughput_completed_qps>r.throughput_completed_qps?'C++':'Rust';return {count:s.data.workload.request_count,ok:Number(r.all_completed_results_correct&&c.all_completed_results_correct)*s.data.workload.request_count,winner:w,note:`Throughput: Rust ${num(r.throughput_completed_qps)} QPS, C++ ${num(c.throughput_completed_qps)} QPS`}}
function storageSummary(s){const d=s.data;return {count:d.formats.length*d.queries_per_format,ok:d.cross_format_correctness?d.formats.length*d.queries_per_format:0,winner:d.cross_format_correctness?'PASS':'FAIL',note:`${d.formats.length} formats, ${d.scope}`}}
function parquetSummary(s){const q=s.data.queries||[],ratios=q.map(x=>{const t=x.timing||{};return t.rust_direct&&t.cpp_direct?t.rust_direct.median_ns/t.cpp_direct.median_ns:NaN}),g=geo(ratios);return {count:q.length,ok:q.filter(x=>x.correct).length,winner:g<.99?'Rust':g>1.01?'C++':'TIE',note:Number.isFinite(g)?`Direct Rust/C++ geometric mean ${num(g,4)}x`:'Correctness-only run'}}
function summary(s){return s.kind==='query'?querySummary(s):s.kind==='optimizer'?optimizerSummary(s):s.kind==='concurrency'?concurrencySummary(s):s.kind==='storage'?storageSummary(s):parquetSummary(s)}
function renderOverview(){const summaries=REPORT.suites.map(s=>[s,summary(s)]),total=summaries.reduce((a,[,x])=>a+x.count,0),correct=summaries.reduce((a,[,x])=>a+x.ok,0);const cards=summaries.map(([s,x])=>`<article class="suite-card" onclick="document.getElementById('${s.key}').scrollIntoView()"><h3>${esc(s.title)} ${pill(x.winner)}</h3><p>${x.ok}/${x.count} validated. ${esc(x.note)}</p></article>`).join('');return section('overview','Run overview','One report for every enabled benchmark suite',`<div class="grid">${metric('Suites',REPORT.suites.length,'Enabled in this run')}${metric('Validated cases',num(correct,0),`${num(total,0)} total checks`,correct===total?'good':'warn')}${metric('Correctness',total?percent(correct/total*100):'N/A','Cross-engine and suite assertions',correct===total?'good':'warn')}${metric('Generated',new Date(REPORT.generated_at).toLocaleTimeString(),new Date(REPORT.generated_at).toLocaleDateString())}</div><div class="suite-grid">${cards}</div><div class="callout"><span class="status"><span class="dot"></span><strong>How to read this report</strong></span><div class="muted">Lower latency and memory are better. A Rust/C++ ratio below 1 favors Rust; above 1 favors C++. Direct Parquet latency includes metadata and decoding, while its eager control excludes the one-time load.</div></div>`)}
function winnerSplit(d){const total=Math.max(1,d.rust_wins+d.cpp_wins+d.ties),r=d.rust_wins/total*100,c=d.cpp_wins/total*100,t=d.ties/total*100;return `<div class="split"><span style="width:${r}%;background:var(--rust)"></span><span style="width:${c}%;background:var(--cpp)"></span><span style="width:${t}%;background:var(--good)"></span></div><div class="legend"><span><i style="background:var(--rust)"></i>Rust ${d.rust_wins}</span><span><i style="background:var(--cpp)"></i>C++ ${d.cpp_wins}</span><span><i style="background:var(--good)"></i>Ties ${d.ties}</span></div>`}
function querySection(s){const d=s.data,q=d.queries||[],sum=querySummary(s),g=d.rust_cpp_geometric_mean,delta=Number.isFinite(g)?Math.abs(g-1)*100:0,interpret=g<1?`Rust is ${num(delta)}% faster by geometric mean`:g>1?`C++ is ${num(delta)}% faster by geometric mean`:'Engines are tied';const rows=q.map(x=>`<tr data-winner="${esc(x.winner)}" data-search="${esc(`${x.query_id} ${x.category} ${x.winner}`.toLowerCase())}"><td>${esc(x.query_id)}</td><td>${esc(x.category)}</td><td data-value="${x.rust_execution_median_ms}">${ms(x.rust_execution_median_ms)}</td><td data-value="${x.cpp_execution_median_ms}">${ms(x.cpp_execution_median_ms)}</td><td data-value="${x.rust_execution_p95_ms}">${ms(x.rust_execution_p95_ms)}</td><td data-value="${x.cpp_execution_p95_ms}">${ms(x.cpp_execution_p95_ms)}</td><td data-value="${x.rust_cpp_ratio}">${num(x.rust_cpp_ratio,4)}x</td><td>${pill(x.winner)}</td><td>${percent(x.difference_pct)}</td></tr>`).join('');const body=`<div class="grid">${metric('Correctness',`${sum.ok}/${sum.count}`,'Typed results matched','good')}${metric('Overall',pill(sum.winner),esc(interpret))}${metric('Peak RSS, Rust',bytes((d.rust_peak_rss_kib||0)*1024),'Observed process peak','rust')}${metric('Peak RSS, C++',bytes((d.cpp_peak_rss_kib||0)*1024),'Observed process peak','cpp')}</div>${winnerSplit(d)}<div class="controls"><input data-table-search="${s.key}-table" placeholder="Search query or category"><select data-table-winner="${s.key}-table"><option value="">All winners</option><option>Rust</option><option>C++</option><option>TIE</option></select></div><div class="table-wrap"><table id="${s.key}-table"><thead><tr><th>Query</th><th>Category</th><th>Rust median</th><th>C++ median</th><th>Rust p95</th><th>C++ p95</th><th>Ratio</th><th>Winner</th><th>Difference</th></tr></thead><tbody>${rows}</tbody></table></div>`;return section(s.key,s.title,`${d.input?.format||'Analytical'} workload with ${d.configuration?.threads||'N/A'} threads and ${d.configuration?.batch_size||'N/A'} row batches`,body)}
function optimizerSection(s){const d=s.data,q=d.queries||[],rs=q.map(x=>x.rust_speedup).filter(Number.isFinite),cs=q.map(x=>x.cpp_speedup).filter(Number.isFinite),rows=q.map(x=>{const t=x.timing||{};return `<tr data-search="${esc(`${x.query_id} ${x.category}`.toLowerCase())}"><td>${esc(x.query_id)}</td><td>${esc(x.category)}</td><td>${ms((t.rust_optimized||{}).median_ns/1e6)}</td><td>${ms((t.cpp_optimized||{}).median_ns/1e6)}</td><td>${Number.isFinite(x.rust_speedup)?num(x.rust_speedup,2)+'x':'plan only'}</td><td>${Number.isFinite(x.cpp_speedup)?num(x.cpp_speedup,2)+'x':'plan only'}</td><td>${pill(x.correct?'PASS':'FAIL')}</td></tr>`}).join('');return section(s.key,s.title,'Optimized plans compared with optimizer-disabled execution',`<div class="grid">${metric('Correctness',`${q.filter(x=>x.correct).length}/${q.length}`,'Plans and results','good')}${metric('Rust optimizer',`${num(geo(rs),2)}x`,'Geometric mean speedup','rust')}${metric('C++ optimizer',`${num(geo(cs),2)}x`,'Geometric mean speedup','cpp')}${metric('Iterations',d.iterations,d.validate_only?'Validation only':'Per configuration')}</div><div class="controls"><input data-table-search="optimizer-table" placeholder="Search optimizer case"></div><div class="table-wrap"><table id="optimizer-table"><thead><tr><th>Query</th><th>Rule area</th><th>Rust optimized</th><th>C++ optimized</th><th>Rust speedup</th><th>C++ speedup</th><th>Status</th></tr></thead><tbody>${rows}</tbody></table></div>`)}
function comparisonBars(items){const max=Math.max(...items.map(x=>x.value),1);return items.map(x=>`<div class="pair"><span>${esc(x.label)}</span><div class="track"><div class="fill" style="width:${x.value/max*100}%;background:${x.color}"></div></div><strong>${esc(x.text)}</strong></div>`).join('')}
function concurrencySection(s){const d=s.data,r=d.rust.summary,c=d.cpp.summary;const rows=['throughput_completed_qps','completed','failed','cancelled','deadline_misses','admission_rejected','jain_fairness_index'].map(k=>`<tr><td>${esc(k.replaceAll('_',' '))}</td><td>${num(r[k],k.includes('qps')?2:6)}</td><td>${num(c[k],k.includes('qps')?2:6)}</td></tr>`).join('');const bars=comparisonBars([{label:'Rust QPS',value:r.throughput_completed_qps,text:num(r.throughput_completed_qps,2),color:'var(--rust)'},{label:'C++ QPS',value:c.throughput_completed_qps,text:num(c.throughput_completed_qps,2),color:'var(--cpp)'},{label:'Rust p95',value:r.execution_latency.p95_ns,text:fromNs(r.execution_latency.p95_ns),color:'var(--rust)'},{label:'C++ p95',value:c.execution_latency.p95_ns,text:fromNs(c.execution_latency.p95_ns),color:'var(--cpp)'}]);return section(s.key,s.title,`${d.workload.request_count} mixed requests with bounded admission and scheduling`, `<div class="grid">${metric('Rust throughput',`${num(r.throughput_completed_qps,2)} QPS`,`${r.completed} completed`,'rust')}${metric('C++ throughput',`${num(c.throughput_completed_qps,2)} QPS`,`${c.completed} completed`,'cpp')}${metric('Rust exec p95',fromNs(r.execution_latency.p95_ns),`Queue p95 ${fromNs(r.queue_delay.p95_ns)}`)}${metric('C++ exec p95',fromNs(c.execution_latency.p95_ns),`Queue p95 ${fromNs(c.queue_delay.p95_ns)}`)}</div><div class="callout">${bars}</div><div class="table-wrap"><table><thead><tr><th>Metric</th><th>Rust</th><th>C++</th></tr></thead><tbody>${rows}</tbody></table></div>`)}
function storageSection(s){const d=s.data,rows=d.formats.map(x=>`<tr><td>${esc(x.format)}</td><td>${esc(x.file)}</td><td data-value="${x.bytes}">${bytes(x.bytes)}</td><td>${ms(x.rust_load_time_ms)}</td><td>${ms(x.cpp_load_time_ms)}</td><td>${num(x.rust_cpp_query_geomean,4)}x</td><td>${x.rust_cpp_query_geomean<1?pill('Rust'):pill('C++')}</td><td>${x.queries_correct}/${d.queries_per_format}</td></tr>`).join('');const smallest=[...d.formats].sort((a,b)=>a.bytes-b.bytes)[0];return section(s.key,s.title,d.scope,`<div class="grid">${metric('Cross-format results',d.cross_format_correctness?'PASS':'FAIL',`${d.formats.length} official and native formats`,d.cross_format_correctness?'good':'warn')}${metric('Queries',d.queries_per_format,'Per storage format')}${metric('Smallest file',esc(smallest.format),bytes(smallest.bytes))}${metric('Baseline',esc(d.baseline),'Comparison reference')}</div><div class="table-wrap" style="margin-top:18px"><table><thead><tr><th>Format</th><th>File</th><th>Size</th><th>Rust load</th><th>C++ load</th><th>Query ratio</th><th>Winner</th><th>Correct</th></tr></thead><tbody>${rows}</tbody></table></div>`)}
function parquetSection(s){const d=s.data,q=d.queries||[],ratios=[],wins={Rust:0,'C++':0,TIE:0};const rows=q.map(x=>{const t=x.timing||{},r=(t.rust_direct||{}).median_ns,c=(t.cpp_direct||{}).median_ns,re=(t.rust_eager||{}).median_ns,ce=(t.cpp_eager||{}).median_ns,ratio=r&&c?r/c:NaN,w=!Number.isFinite(ratio)?'TIE':Math.abs(ratio-1)<.01?'TIE':ratio<1?'Rust':'C++';if(Number.isFinite(ratio))ratios.push(ratio);wins[w]++;const scan=x.scan||{};return `<tr data-winner="${w}" data-search="${esc(`${x.query_id} ${x.category} ${w}`.toLowerCase())}"><td>${esc(x.query_id)}</td><td>${esc(x.category)}</td><td>${fromNs(r)}</td><td>${fromNs(c)}</td><td>${fromNs(re)}</td><td>${fromNs(ce)}</td><td>${scan.columns_read}/${scan.total_columns}</td><td>${scan.row_groups_read}/${scan.total_row_groups}</td><td>${bytes(scan.compressed_bytes_read)}</td><td>${pill(w)}</td></tr>`}).join(''),g=geo(ratios),sl=d.startup_load_ns||{},rss=d.peak_rss_kib_observed||{};return section(s.key,s.title,'Query-time projection and row-group pruning compared with eager in-memory execution',`<div class="grid">${metric('Correctness',`${q.filter(x=>x.correct).length}/${q.length}`,'Direct and eager results','good')}${metric('Direct overall',pill(g<.99?'Rust':g>1.01?'C++':'TIE'),Number.isFinite(g)?`Rust/C++ ${num(g,4)}x`:'Validation only')}${metric('Rust startup',fromNs(sl.rust_direct),`Eager ${fromNs(sl.rust_eager)}`,'rust')}${metric('C++ startup',fromNs(sl.cpp_direct),`Eager ${fromNs(sl.cpp_eager)}`,'cpp')}</div>${winnerSplit({rust_wins:wins.Rust,cpp_wins:wins['C++'],ties:wins.TIE})}<div class="grid" style="margin-top:14px">${metric('Rust direct peak RSS',bytes((rss.rust_direct||0)*1024),`Eager ${bytes((rss.rust_eager||0)*1024)}`)}${metric('C++ direct peak RSS',bytes((rss.cpp_direct||0)*1024),`Eager ${bytes((rss.cpp_eager||0)*1024)}`)}</div><div class="controls"><input data-table-search="parquet-table" placeholder="Search Parquet case"><select data-table-winner="parquet-table"><option value="">All winners</option><option>Rust</option><option>C++</option><option>TIE</option></select></div><div class="table-wrap"><table id="parquet-table"><thead><tr><th>Query</th><th>Category</th><th>Rust direct</th><th>C++ direct</th><th>Rust eager</th><th>C++ eager</th><th>Columns</th><th>Row groups</th><th>Selected bytes</th><th>Direct winner</th></tr></thead><tbody>${rows}</tbody></table></div>`)}
function environmentSection(){const entries=Object.entries(REPORT.environment||{});if(!entries.length)return '';return section('environment','Environment','Toolchains and machine configuration captured by the benchmark',`<div class="environment">${entries.map(([k,v])=>`<div class="env-row"><span>${esc(k.replaceAll('_',' '))}</span><strong>${esc(v)}</strong></div>`).join('')}</div>`)}
function installInteractions(){$$('[data-table-search]').forEach(input=>input.addEventListener('input',()=>filterTable(input.dataset.tableSearch)));$$('[data-table-winner]').forEach(select=>select.addEventListener('change',()=>filterTable(select.dataset.tableWinner)));$$('th').forEach(th=>th.addEventListener('click',()=>sortTable(th)));const observer=new IntersectionObserver(entries=>entries.forEach(e=>{if(e.isIntersecting){$$('.nav button').forEach(b=>b.classList.toggle('active',b.dataset.target===e.target.id))}}),{rootMargin:'-20% 0px -70%'});$$('.section').forEach(s=>observer.observe(s))}
function filterTable(id){const table=document.getElementById(id);if(!table)return;const search=$(`[data-table-search="${id}"]`)?.value.toLowerCase()||'',winner=$(`[data-table-winner="${id}"]`)?.value||'';$$('tbody tr',table).forEach(row=>row.hidden=!(row.dataset.search||row.textContent.toLowerCase()).includes(search)||(winner&&row.dataset.winner!==winner))}
function sortTable(th){const table=th.closest('table'),index=[...th.parentNode.children].indexOf(th),asc=th.dataset.order!=='asc';$$('th',table).forEach(x=>delete x.dataset.order);th.dataset.order=asc?'asc':'desc';const rows=$$('tbody tr',table).sort((a,b)=>{const av=a.children[index].dataset.value??a.children[index].textContent,bv=b.children[index].dataset.value??b.children[index].textContent,an=Number(av),bn=Number(bv),cmp=Number.isFinite(an)&&Number.isFinite(bn)?an-bn:String(av).localeCompare(String(bv));return asc?cmp:-cmp});rows.forEach(r=>$('tbody',table).appendChild(r))}
function init(){const created=new Date(REPORT.generated_at);$('#hero-copy').textContent=`Generated ${created.toLocaleString()} from ${REPORT.suites.length} enabled suites. All charts and data are embedded in this file for offline use.`;let html=renderOverview();for(const s of REPORT.suites)html+=s.kind==='query'?querySection(s):s.kind==='optimizer'?optimizerSection(s):s.kind==='concurrency'?concurrencySection(s):s.kind==='storage'?storageSection(s):parquetSection(s);html+=environmentSection();$('#report').innerHTML=html;const targets=[...REPORT.suites.map(s=>[s.key,s.title]),...(Object.keys(REPORT.environment||{}).length?[['environment','Environment']]:[])];$('#nav').innerHTML=`<button data-target="overview" onclick="document.getElementById('overview').scrollIntoView()">Overview</button>`+targets.map(([id,title])=>`<button data-target="${id}" onclick="document.getElementById('${id}').scrollIntoView()">${esc(title)}</button>`).join('');$('#footer').textContent='Dremel Rust and C++ benchmark report. Lower latency and memory are better.';installInteractions();$('#theme').addEventListener('click',()=>document.documentElement.classList.toggle('light'))}
init();
</script>
</body>
</html>
'''


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--results-dir", type=Path, default=ROOT / "results")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--include", action="append", choices=SUITES)
    parser.add_argument("--open", action="store_true", dest="open_report")
    args = parser.parse_args()

    results = args.results_dir.resolve()
    included = args.include or [
        key for key, (_, relative, _) in SUITES.items() if (results / relative).exists()
    ]
    if not included:
        parser.error(f"no benchmark JSON results found under {results}")
    output = (args.output or results / "benchmark-report.html").resolve()
    payload = json.dumps(load_report_data(results, included), separators=(",", ":"))
    payload = payload.replace("</", "<\\/")
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(HTML.replace("__REPORT_DATA__", payload))
    uri = output.as_uri()
    if args.open_report:
        webbrowser.open(uri)
    print(uri)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
