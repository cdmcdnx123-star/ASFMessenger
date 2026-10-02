// ASF Messenger server: numbers register karta hai aur messages users ke beech pahunchata hai.
const http=require("http"),fs=require("fs"),path=require("path"),{WebSocketServer}=require("ws");
const DB=path.join(__dirname,"data.json");
let db={users:{},queue:{}};try{db=JSON.parse(fs.readFileSync(DB,"utf8"))}catch(e){}
const save=()=>fs.writeFile(DB,JSON.stringify(db),()=>{});
const dg=s=>String(s||"").replace(/\D/g,""),fmt=d=>`ASF ${d.slice(0,4)} ${d.slice(4,7)} ${d.slice(7,11)}`;
const byNum=n=>Object.values(db.users).find(u=>dg(u.num)==dg(n));
const gen=()=>{let n;do{n=String(1+Math.floor(Math.random()*9))+Array.from({length:10},()=>Math.floor(Math.random()*10)).join("")}while(byNum(n));return fmt(n)};
const socks={}; // number(digits) -> Set of sockets

const srv=http.createServer((q,r)=>{
  fs.readFile(path.join(__dirname,"index.html"),(e,d)=>{
    if(e){r.writeHead(500);return r.end("index.html nahi mila")}
    r.writeHead(200,{"content-type":"text/html; charset=utf-8","cache-control":"no-store"});r.end(d)})});

const wss=new WebSocketServer({server:srv});
const out=(w,o)=>w.readyState==1&&w.send(JSON.stringify(o));
function deliver(u,p){
  const set=socks[dg(u.num)],live=set&&[...set].filter(w=>w.readyState==1);
  if(live&&live.length)return live.forEach(w=>out(w,p));
  const k=dg(u.num);(db.queue[k]=db.queue[k]||[]).push(p);db.queue[k]=db.queue[k].slice(-1000);save()}

wss.on("connection",w=>{
  w.on("close",()=>{if(w.me)socks[dg(w.me.num)]?.delete(w)});
  w.on("message",raw=>{
    let m;try{m=JSON.parse(raw)}catch(e){return}
    if(m.type=="hello"){
      const id=String(m.id||"").trim().toLowerCase();if(!id)return;
      let u=db.users[id];
      if(!u){let n=dg(m.num);if(n.length!=11||byNum(n))n=dg(gen());u=db.users[id]={name:String(m.name||id).slice(0,30),num:fmt(n)}}
      else if(m.name)u.name=String(m.name).slice(0,30);
      save();w.me=u;(socks[dg(u.num)]=socks[dg(u.num)]||new Set()).add(w);
      out(w,{type:"me",num:u.num,name:u.name});
      const q=db.queue[dg(u.num)]||[];delete db.queue[dg(u.num)];q.forEach(p=>out(w,p));save();return}
    if(!w.me)return;
    if(m.type=="find"){const u=byNum(m.num);return out(w,{type:"found",rid:m.rid,ok:!!u&&u!==w.me,name:u&&u.name})}
    if(m.type=="msg"){const to=byNum(m.to);if(!to||to===w.me||!m.t)return;
      deliver(to,{type:"msg",from:w.me.num,name:w.me.name,t:String(m.t).slice(0,4000),ts:Number(m.ts)||Date.now()})}})});

const PORT=process.env.PORT||3000;
srv.listen(PORT,"0.0.0.0",()=>console.log("ASF Messenger chal raha hai: http://localhost:"+PORT));
