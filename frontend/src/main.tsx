import React, { useState } from 'react';
import { createRoot } from 'react-dom/client';
import './style.css';

type Device = {id:string; vehicle_id:string; firmware_version:string; connectivity_state:string; storage_state:string; capture_state:string; last_seen_at:string|null};
type Incident = {id:string; device_id:string; type:string; status:string; summary:string; opened_at:string};
type IncidentDetail = Incident & {timeline:{occurred_at:string;kind:string;annotation:string}[]};
type Evidence = {id:string; event_id:string; status:string; origin:string; mime_type:string};
type Overview = {devices:Device[]; outbox_pending:number; unprocessed:number};
type EventRow = {event_id:string; device_id:string; event_kind:string; occurred_at:string; received_at:string; payload:string};
type Timeline = {event_id:string; event_kind:string; occurred_at:string; received_at:string; processed_at:string|null; payload:string; traceparent:string|null};

function App() {
  const [key,setKey] = useState('');
  const [overview,setOverview] = useState<Overview|null>(null);
  const [incidents,setIncidents] = useState<Incident[]>([]);
  const [evidence,setEvidence] = useState<Evidence[]>([]);
  const [events,setEvents] = useState<EventRow[]>([]);
  const [timeline,setTimeline] = useState<Timeline[]>([]);
  const [selected,setSelected] = useState('');
  const [incidentDetail,setIncidentDetail] = useState<IncidentDetail|null>(null);
  const [filter,setFilter] = useState('ALL');
  const [error,setError] = useState('');
  const [busy,setBusy] = useState(false);
  async function api<T>(path:string):Promise<T> {
    const response = await fetch('/api/v1'+path,{headers:{'X-API-Key':key}});
    if (!response.ok) throw new Error(`Falha na API: ${response.status}`);
    return response.json() as Promise<T>;
  }
  async function refresh() {
    setBusy(true); setError('');
    try { const [o,i,e,v] = await Promise.all([api<Overview>('/ops/overview'),api<Incident[]>('/incidents'),api<Evidence[]>('/evidence'),api<EventRow[]>('/events')]); setOverview(o); setIncidents(i); setEvidence(e); setEvents(v); }
    catch(e) { setError(e instanceof Error ? e.message : 'Falha de conexão'); }
    finally { setBusy(false); }
  }
  async function showDevice(id:string) {
    setSelected(id); setError('');
    try { setTimeline(await api<Timeline[]>('/devices/'+encodeURIComponent(id)+'/timeline')); }
    catch(e) { setTimeline([]); setError(e instanceof Error ? e.message : 'Falha de conexão'); }
  }
  async function showIncident(id:string) {
    setError('');
    try { setIncidentDetail(await api<IncidentDetail>('/incidents/'+encodeURIComponent(id))); }
    catch(e) { setError(e instanceof Error ? e.message : 'Falha ao consultar incidente'); }
  }
  async function openEvidence(id:string) {
    try { const r=await api<{url:string}>('/evidence/'+encodeURIComponent(id)+'/access'); window.open(r.url,'_blank','noopener,noreferrer'); }
    catch(e) { setError(e instanceof Error ? e.message : 'Evidência indisponível'); }
  }
  const devices=overview?.devices ?? [];
  const counts = ['ONLINE','RECOVERING','SUSPECT','OFFLINE','UNKNOWN','MAINTENANCE','DECOMMISSIONED'].map(s=>({label:s,count:devices.filter(d=>d.connectivity_state===s).length}));
  const shown=filter==='ALL'?devices:devices.filter(d=>d.connectivity_state===filter);
  return <><header><div><div className="eyebrow">LABORATÓRIO · TELEMETRIA SINTÉTICA</div><h1>FleetCam <span>Operações</span></h1></div><div className="login"><label htmlFor="key">Chave local</label><input id="key" type="password" value={key} onChange={e=>setKey(e.target.value)} autoComplete="off" placeholder="Chave da API"/><button disabled={!key||busy} onClick={refresh}>{busy?'Atualizando…':'Conectar / atualizar'}</button></div></header>
    <main>{error&&<div className="error" role="alert">{error}</div>}
      {!overview?<section className="empty"><h2>Visibilidade para sua frota de laboratório</h2><p>Informe a chave local e conecte para consultar dispositivos, incidentes e evidências.</p><p>Os sinais demonstrados são sintéticos e não constituem diagnóstico de segurança viária.</p></section>:<>
      <section className="metrics" aria-label="Resumo da frota">{counts.map(c=><article key={c.label}><small>{c.label}</small><strong>{c.count}</strong></article>)}<article><small>OUTBOX PENDENTE</small><strong>{overview.outbox_pending}</strong></article><article><small>NÃO PROCESSADOS</small><strong>{overview.unprocessed}</strong></article></section>
      <section><div className="section-head"><div><h2>Dispositivos</h2><p>Conectividade baseada no último heartbeat aceito; SD e captura são sinais independentes.</p></div><label>Filtrar <select value={filter} onChange={e=>setFilter(e.target.value)}><option value="ALL">Todos</option>{counts.map(c=><option key={c.label}>{c.label}</option>)}</select></label></div>
      <div className="table-wrap"><table><thead><tr><th>Dispositivo</th><th>Veículo</th><th>Conexão</th><th>Armazenamento</th><th>Captura</th><th>Firmware</th><th>Último sinal</th></tr></thead><tbody>{shown.map(d=><tr key={d.id}><td><button className="link" onClick={()=>showDevice(d.id)}>{d.id}</button></td><td>{d.vehicle_id}</td><td><span className="pill">{d.connectivity_state}</span></td><td>{d.storage_state}</td><td>{d.capture_state}</td><td>{d.firmware_version}</td><td>{d.last_seen_at?new Date(d.last_seen_at).toLocaleString():'—'}</td></tr>)}</tbody></table></div>{shown.length===0&&<p>Nenhum dispositivo neste filtro.</p>}</section>
      {selected&&<section><div className="section-head"><div><h2>Timeline · {selected}</h2><p>Horários de origem e recebimento são preservados separadamente.</p></div><button className="secondary" onClick={()=>setSelected('')}>Fechar</button></div><div className="table-wrap"><table><thead><tr><th>Evento</th><th>Tipo</th><th>Origem</th><th>Recebido</th><th>Processado</th><th>Trace</th></tr></thead><tbody>{timeline.map(t=>{const trace=t.traceparent?.split("-")[1]; return <tr key={t.event_id}><td>{t.event_id}</td><td>{t.event_kind}</td><td>{t.occurred_at}</td><td>{t.received_at}</td><td>{t.processed_at??'Pendente'}</td><td>{trace && /^[0-9a-f]{32}$/.test(trace) ? <a href={"http://localhost:16686/trace/"+trace} target="_blank" rel="noreferrer">{trace}</a> : "—"}</td></tr>})}</tbody></table></div></section>}
      <section><h2>Eventos</h2><p>Origem, tipo e marcação de simulação conforme informados no envelope.</p>{events.length ? <div className="table-wrap"><table><thead><tr><th>Dispositivo</th><th>Tipo</th><th>Origem</th><th>Natureza</th><th>Ocorrido</th><th>Recebido</th></tr></thead><tbody>{events.map(e=>{ let payload: {event_type?:string;source?:string;simulated?:boolean} = {}; try { payload=JSON.parse(e.payload); } catch { /* Keep malformed historic data visible. */ } return <tr key={e.event_id}><td>{e.device_id}</td><td>{payload.event_type ?? e.event_kind}</td><td>{payload.source ?? 'UNKNOWN'}</td><td><span className="pill">{payload.simulated === true ? 'SIMULADO' : payload.simulated === false ? 'ANÁLISE INFORMADA' : 'DESCONHECIDA'}</span></td><td>{e.occurred_at}</td><td>{e.received_at}</td></tr>; })}</tbody></table></div> : <p>Nenhum evento registrado.</p>}</section>
      {incidentDetail&&<section><div className="section-head"><div><h2>Investigação · {incidentDetail.device_id}</h2><p>{incidentDetail.summary}</p></div><button className="secondary" onClick={()=>setIncidentDetail(null)}>Fechar</button></div><p>Tipo: {incidentDetail.type} · Estado: {incidentDetail.status}. Este sinal não confirma causa raiz.</p><div className="table-wrap"><table><thead><tr><th>Horário</th><th>Registro</th><th>Detalhe</th></tr></thead><tbody>{incidentDetail.timeline.map((entry,index)=><tr key={index}><td>{entry.occurred_at}</td><td>{entry.kind}</td><td>{entry.annotation}</td></tr>)}</tbody></table></div></section>}
      <div className="columns"><section><h2>Incidentes</h2><p>Gerados a partir dos sinais informados; não são causa raiz confirmada.</p>{incidents.length?incidents.map(i=><div className="entry" key={i.id}><span className="pill">{i.status}</span><strong>{i.type} · {i.device_id}</strong><p>{i.summary}</p><small>{i.opened_at}</small> <button className="secondary" onClick={()=>showIncident(i.id)}>Investigar</button></div>):<p>Nenhum incidente registrado.</p>}</section>
      <section><h2>Evidências</h2><p>Arquivos só são acessíveis após confirmação de integridade.</p>{evidence.length?evidence.map(e=><div className="entry" key={e.id}><span className="pill">{e.status}</span><strong>{e.origin}</strong><p>Evento {e.event_id}</p><button className="secondary" disabled={e.status!=='AVAILABLE'} onClick={()=>openEvidence(e.id)}>Abrir imagem</button></div>):<p>Nenhuma evidência registrada.</p>}</section></div>
      </>}</main><footer>FleetCam · ambiente local demonstrativo · sem dados reais de frota</footer></>;
}
createRoot(document.getElementById('root')!).render(<React.StrictMode><App/></React.StrictMode>);
