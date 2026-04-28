"""
HC Report Automation — Playwright
===================================
After every run this script writes data.json which the dashboard reads.

SETUP (run once):
    pip install playwright pandas schedule
    playwright install chromium

HOW TO USE:
    Terminal 1:  python main.py          ← automation
    Terminal 2:  python -m http.server 8080  ← dashboard server
    Phone/PC:    http://YOUR_IP:8080/dashboard.html
"""

from playwright.sync_api import sync_playwright
from datetime import datetime, timedelta
import pandas as pd
import os, time, schedule, json

# ══════════════════════════════════════════
#  ⚙️  CONFIG — only edit this section
# ══════════════════════════════════════════
URL      = "http://10.19.32.29:9090/SM980/index.do"
USERNAME = "DUMMY"
PASSWORD = "Qwer!234"

DASHBOARD_NAME = "HC Report Internal OT (Global)"

SCRIPT_DIR    = os.path.dirname(os.path.abspath(__file__))
DOWNLOAD_PATH = os.path.join(SCRIPT_DIR, "downloads")
DATA_JSON     = os.path.join(SCRIPT_DIR, "data.json")   # dashboard reads this
os.makedirs(DOWNLOAD_PATH, exist_ok=True)

UOC_NODES = [
    {"name": "UOC1", "url": "http://10.19.33.83:3000/workspaces/HOME_WS"},
    {"name": "UOC2", "url": "http://10.19.33.176:3000/workspaces/home_ws"},
    {"name": "UOC3", "url": "http://10.19.33.27:3000/workspaces/home_ws_tx_ent"},
    {"name": "UOC4", "url": "http://10.19.33.84:3000/workspaces/home_ws"},
]
UOC_USERNAME       = "admin"
UOC_PASSWORD       = "uoc@123"
UOC_THRESHOLD_SECS = 10
RUN_EVERY_MINUTES  = 60

# ══════════════════════════════════════════
# [UPDATED] Location column name in the CSV
# Change this if your CSV uses a different column name for location/circle
# ══════════════════════════════════════════
LOCATION_COLUMN = "Location Full Name"   # [UPDATED] matched to actual CSV column name

# ══════════════════════════════════════════
# [UPDATED] Category column name in the CSV
# Change this if your CSV uses a different column name for service category
# ══════════════════════════════════════════
CATEGORY_COLUMN = None   # None = auto-detect (Service/Category/Type/Service_Type)
# ══════════════════════════════════════════

SCROLL_JS = """(function(){
    const t=[document.scrollingElement,document.body,
             document.querySelector('.x-panel-body'),
             document.querySelector('.x-tab-panel-body')];
    t.forEach(el=>{if(el)el.scrollTop=el.scrollHeight;});
    const g=document.querySelectorAll('div[id^="smGadget"]');
    g.forEach(x=>{try{x.scrollIntoView({behavior:'instant',block:'center'});}catch(e){}});
    return g.length;
})();"""

SCRAPE_JS = """(function(){
    function cellText(c){return(c.innerText||c.textContent||'').trim();}
    function findPanel(span){
        let e=span;
        for(let i=0;i<25;i++){if(!e||!e.parentElement)break;e=e.parentElement;
            if(e.id&&/^smGadget-\\d+$/.test(e.id))return e;}
        e=span;
        for(let i=0;i<25;i++){if(!e||!e.parentElement)break;e=e.parentElement;
            if(e.tagName==='DIV'&&e.querySelector&&e.querySelector('table tr'))return e;}
        return null;
    }
    function findGadget(title){
        for(const s of document.querySelectorAll('span[id$="_header_hd-textEl"],span.x-panel-header-text'))
            if(cellText(s)===title){const p=findPanel(s);if(p)return p;}
        return null;
    }
    function lastNum(row){
        const cells=Array.from(row.querySelectorAll('td,th'));
        for(let i=cells.length-1;i>=0;i--){
            const v=cellText(cells[i]).replace(/,/g,'');
            if(v!==''&&!isNaN(v)&&parseInt(v)>=0)return v;}
        return null;
    }
    function grandTotal(pan){
        if(!pan)return null;
        for(const row of pan.querySelectorAll('tr'))
            if(Array.from(row.querySelectorAll('td,th')).some(c=>cellText(c).includes('Totals'))){
                const v=lastNum(row);if(v!==null)return v;}
        return null;
    }
    function successTotal(pan){
        if(!pan)return null;
        for(const row of pan.querySelectorAll('tr')){
            const cells=Array.from(row.querySelectorAll('td,th'));
            const hasSuccess=cells.some(c=>cellText(c).toLowerCase()==='success');
            const isTotal=cells.some(c=>cellText(c).includes('Totals'));
            if(hasSuccess&&!isTotal){const v=lastNum(row);if(v!==null)return v;}
        }
        return null;
    }
    const titles=Array.from(document.querySelectorAll(
        'span[id$="_header_hd-textEl"],span.x-panel-header-text'))
        .map(s=>cellText(s)).filter(Boolean);
    const pc=findGadget('PAGE COUNT'),wfm=findGadget('WFM New Count---All Domain'),
          eml=findGadget('Email Count'),ad=findGadget('Autodialer Last 1Hrs Report');
    function debugAD(panel){
        if(!panel)return 'NO_PANEL';
        const rows=Array.from(panel.querySelectorAll('tr'));
        const sr=rows.find(r=>Array.from(r.querySelectorAll('td,th'))
            .some(c=>cellText(c).toLowerCase()==='success'));
        return sr?'SUCCESS_ROW='+cellText(sr).replace(/\\s+/g,' ').slice(0,60)
                 :'NO_SUCCESS_ROW(rows='+rows.length+')';
    }
    return{sms_count:pc?grandTotal(pc):null,wfm_count:wfm?grandTotal(wfm):null,
           email_count:eml?grandTotal(eml):null,ad_success:ad?successTotal(ad):null,
           gadget_titles:titles.slice(0,20),debug_ad:debugAD(ad)};
})();"""


# ══════════════════════════════════════════
#  📊 DASHBOARD JSON WRITER
#  This is what makes the dashboard work.
#  Called after every successful run.
# ══════════════════════════════════════════
_history = []   # kept in memory across runs

# [UPDATED] write_dashboard_json now accepts bss_location_df and cen_location_df
def write_dashboard_json(state, login_dur, now, h_back, w_start, w_end,
                          today_cnt, lw_cnt, metrics, uoc, categories,
                          elapsed_min, error=None,
                          bss_location_df=None, cen_location_df=None):
    """Write data.json — the dashboard reads this every 30 seconds."""

    # Build category list for table
    cat_list = []
    if categories is not None:
        for _, row in categories.iterrows():
            cat_list.append({
                "name":  str(row["CATEGORY"]),
                "today": int(row["Today"]),
                "lw":    int(row["Last Week"])
            })

    # [UPDATED] Build BSS location breakdown list
    bss_loc_list = []
    if bss_location_df is not None:
        for _, row in bss_location_df.iterrows():
            bss_loc_list.append({
                "name":  str(row["LOCATION"]),
                "today": int(row["Today"]),
                "lw":    int(row["Last Week"])
            })

    # [UPDATED] Build CEN location breakdown list
    cen_loc_list = []
    if cen_location_df is not None:
        for _, row in cen_location_df.iterrows():
            cen_loc_list.append({
                "name":  str(row["LOCATION"]),
                "today": int(row["Today"]),
                "lw":    int(row["Last Week"])
            })

    # Build history entry
    entry = {
        "time":    now.strftime("%d-%b-%y %I:%M %p"),
        "state":   state,
        "elapsed": elapsed_min,
        "error":   str(error)[:120] if error else None
    }
    global _history
    _history = ([entry] + _history)[:10]

    data = {
        "state":       state,                                      # running|success|error
        "last_run":    now.strftime("%d-%b-%Y %I:%M %p"),
        "next_run":    (now + timedelta(minutes=RUN_EVERY_MINUTES)).strftime("%I:%M %p"),
        "login_dur":   login_dur,
        "elapsed_min": elapsed_min,
        "error":       str(error)[:200] if error else None,
        "today_range": f"{h_back.strftime('%I:%M %p')} – {now.strftime('%I:%M %p')}",
        "lw_range":    f"{w_start.strftime('%d-%b')} {w_start.strftime('%I:%M %p')} – {w_end.strftime('%I:%M %p')}",
        "today_count": today_cnt,
        "lw_count":    lw_cnt,
        "metrics": {
            "sms_count":        metrics.get("sms_count")        if metrics else None,
            "wfm_count":        metrics.get("wfm_count")        if metrics else None,
            "email_count":      metrics.get("email_count")      if metrics else None,
            "ad_success_count": metrics.get("ad_success_count") if metrics else None,
        },
        "uoc":            uoc or [],
        "categories":     cat_list,
        "history":        _history,
        # [UPDATED] Added BSS and CEN location breakdowns to JSON output
        "bss_locations":  bss_loc_list,
        "cen_locations":  cen_loc_list,
    }

    with open(DATA_JSON, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    print(f"  ✅ data.json updated → dashboard refreshed")


# ══════════════════════════════════════════
#  🛠  HELPERS
# ══════════════════════════════════════════
def fmt_t(dt):  return dt.strftime("%d/%m/%y %H:%M:%S")

def to_int(val):
    if val is None: return "N/A"
    try: return int(str(val).replace(",","").strip())
    except: return "N/A"

def get_time_ranges():
    now=datetime.now(); h=now-timedelta(hours=1)
    return h, now, h-timedelta(days=7), now-timedelta(days=7)

def format_time(dt): return dt.strftime("%d/%m/%y %H:%M:%S")


# ══════════════════════════════════════════
#  🔐 LOGIN
# ══════════════════════════════════════════
def login(page):
    t=datetime.now()
    page.goto(URL)
    page.fill("#LoginUsername", USERNAME)
    page.fill("#LoginPassword", PASSWORD)
    page.click("button[type='submit']")
    page.wait_for_timeout(6000)
    return round((datetime.now()-t).total_seconds())


# ══════════════════════════════════════════
#  🌐 UOC
# ══════════════════════════════════════════
def check_uoc_login(context):
    results=[]; print("\n🔐 UOC Login check...")
    for node in UOC_NODES:
        name,url=node["name"],node["url"]
        print(f"  🌐 {name}: {url}")
        page=context.new_page(); t0=time.time()
        try:
            page.goto(url,wait_until="domcontentloaded",timeout=30000)
            page.wait_for_timeout(2000)
            uf=pf=None
            for sel in ["input[name='user']","input[name='username']",
                        "input[placeholder*='email' i]","input[type='text']","#user"]:
                try:
                    f=page.locator(sel).first
                    if f.count()>0: f.wait_for(state="visible",timeout=3000); uf=f; break
                except: pass
            for sel in ["input[name='password']","input[type='password']","#password"]:
                try:
                    f=page.locator(sel).first
                    if f.count()>0: f.wait_for(state="visible",timeout=3000); pf=f; break
                except: pass
            if uf and pf:
                uf.fill(UOC_USERNAME); pf.fill(UOC_PASSWORD)
                submitted=False
                for btn_sel in ["button[type='submit']","button:has-text('Login')","button:has-text('Log in')"]:
                    try:
                        btn=page.locator(btn_sel).first
                        if btn.count()>0: btn.click(); submitted=True; break
                    except: pass
                if not submitted: pf.press("Enter")
                try: page.wait_for_load_state("networkidle",timeout=20000)
                except: page.wait_for_timeout(5000)
            else:
                print(f"    ⚠️  No login form — measuring load only")
                try: page.wait_for_load_state("networkidle",timeout=15000)
                except: page.wait_for_timeout(3000)
            elapsed=round(time.time()-t0,1)
            status="✅" if elapsed<UOC_THRESHOLD_SECS else "⚠️"
            print(f"    {status} {name}: {elapsed}s")
            results.append({"name":name,"url":url,"seconds":elapsed,"status":status,"error":None})
        except Exception as e:
            elapsed=round(time.time()-t0,1)
            print(f"    ❌ {name}: ERROR after {elapsed}s")
            results.append({"name":name,"url":url,"seconds":elapsed,"status":"❌","error":str(e)[:80]})
        finally:
            try: page.close()
            except: pass
    return results


# ══════════════════════════════════════════
#  📊 DASHBOARD METRICS
# ══════════════════════════════════════════
def select_dashboard(page):
    print(f"\n🔀 Switching to: {DASHBOARD_NAME}")
    combo_opened=False
    for ctx in [page]+list(page.frames):
        try:
            combo=ctx.locator("input[aria-label='Switch dashboard']").first
            if combo.count()==0: combo=ctx.locator("input.x-form-field[role='combobox']").first
            if combo.count()==0: continue
            combo.wait_for(state="visible",timeout=5000)
            combo.click(click_count=3); combo.fill("")
            combo.type(DASHBOARD_NAME[:10],delay=60)
            page.wait_for_timeout(2000)
            print("✅ Combo opened"); combo_opened=True; break
        except: pass
    if not combo_opened:
        for ctx in [page]+list(page.frames):
            try:
                r=ctx.evaluate(f"""(function(){{
                    const c=document.querySelector("input[aria-label='Switch dashboard']")
                           ||document.querySelector("input.x-form-field[role='combobox']");
                    if(!c)return false;
                    c.value='{DASHBOARD_NAME[:10]}';
                    ['input','keydown','keyup','change'].forEach(ev=>
                        c.dispatchEvent(new Event(ev,{{bubbles:true}})));
                    return true;}})();""")
                if r: page.wait_for_timeout(2000); print("✅ Combo JS"); combo_opened=True; break
            except: pass
    selected=False
    for sel in [f"li.x-boundlist-item:text-is('{DASHBOARD_NAME}')",
                f"li.x-boundlist-item:has-text('{DASHBOARD_NAME}')",
                f"li[role='option']:has-text('{DASHBOARD_NAME}')",
                f"li:has-text('{DASHBOARD_NAME}')"]:
        for ctx in [page]+list(page.frames):
            try:
                item=ctx.locator(sel).first
                item.wait_for(state="visible",timeout=4000)
                item.click(); page.wait_for_timeout(2000)
                print("✅ Dashboard selected"); selected=True; break
            except: pass
        if selected: break
    if not selected:
        for ctx in [page]+list(page.frames):
            try:
                r=ctx.evaluate(f"""(function(){{
                    const items=document.querySelectorAll('li.x-boundlist-item,li[role="option"]');
                    for(const li of items)if(li.textContent.trim()==='{DASHBOARD_NAME}'){{li.click();return true;}}
                    return false;}})();""")
                if r: page.wait_for_timeout(2000); print("✅ Selected JS"); selected=True; break
            except: pass
    if not selected: print("⚠️  Dashboard selection uncertain")
    page.wait_for_timeout(5000)
    return selected

def scroll_all_gadgets_into_view(page):
    detail_frames=[f for f in page.frames if 'detail.do' in (f.url or '')]
    targets=detail_frames if detail_frames else [page]
    for ctx in targets:
        try:
            count=ctx.evaluate(SCROLL_JS)
            print(f"  📜 Scrolled {count} gadgets")
        except: pass
    try:
        page.keyboard.press("End"); page.wait_for_timeout(500); page.keyboard.press("Home")
    except: pass
    page.wait_for_timeout(3000)

def scrape_dashboard_metrics(page):
    page.goto(URL); page.wait_for_timeout(8000)
    select_dashboard(page)
    print("📜 Scrolling gadgets...")
    scroll_all_gadgets_into_view(page)
    print("\n⏳ Polling gadgets...")
    raw={}
    for attempt in range(30):
        detail=[f for f in page.frames if 'detail.do' in (f.url or '')]
        others=[f for f in page.frames if 'detail.do' not in (f.url or '')]
        for i,ctx in enumerate(detail+[page]+others):
            try:
                result=ctx.evaluate(SCRAPE_JS)
                if not result: continue
                sms_ok=result.get('sms_count') is not None
                email_ok=result.get('email_count') is not None
                ad_ok=result.get('ad_success') is not None
                if attempt==0 and result.get('gadget_titles'):
                    print(f"  📋 ctx[{i}] titles: {result['gadget_titles'][:8]}")
                    print(f"       debug_ad: {result.get('debug_ad')}")
                if sms_ok:
                    if not raw: raw=result
                    if email_ok and ad_ok:
                        raw=result
                        print(f"  ✅ SMS={result['sms_count']} WFM={result['wfm_count']} "
                              f"Email={result['email_count']} AD={result['ad_success']}")
                        break
                    else:
                        missing=[]
                        if not email_ok: missing.append("Email")
                        if not ad_ok: missing.append(f"AD({result.get('debug_ad','?')})")
                        if attempt%3==0: print(f"  📊 Missing: {missing} — scrolling...")
                        scroll_all_gadgets_into_view(page)
            except: pass
        if raw and raw.get('email_count') is not None and raw.get('ad_success') is not None: break
        if attempt%5==4:
            print(f"  ⏳ Attempt {attempt+1}/30 — scrolling...")
            scroll_all_gadgets_into_view(page)
        else: page.wait_for_timeout(2000)
    if not raw: print("⚠️  Could not load gadget data"); raw={}
    metrics={
        "sms_count":        to_int(raw.get("sms_count")),
        "wfm_count":        to_int(raw.get("wfm_count")),
        "email_count":      to_int(raw.get("email_count")),
        "ad_success_count": to_int(raw.get("ad_success")),
    }
    print(f"\n📊 SMS={metrics['sms_count']} WFM={metrics['wfm_count']} "
          f"Email={metrics['email_count']} AD={metrics['ad_success_count']}")
    return metrics


# ══════════════════════════════════════════
#  🔍 SEARCH & EXPORT
# ══════════════════════════════════════════
def navigate_to_search(page):
    print("Navigating to Search Incidents...")
    page.goto(URL); page.wait_for_timeout(5000)
    page.locator("text=Incident Management").first.click()
    page.wait_for_timeout(2000)
    page.locator("span:has-text('Search Incidents')").first.click()
    page.wait_for_timeout(5000)

def get_search_frame(page):
    for frame in page.frames:
        try:
            if frame.locator("#X45").count()>0:
                print("✅ Search frame found"); return frame
        except: continue
    raise Exception("❌ Search frame not found")

def get_result_frame(page):
    print("🔍 Finding result frame..."); page.wait_for_timeout(5000)
    for frame in page.frames:
        try:
            if frame.locator("text=Count Records").count()>0:
                print("✅ Result frame found"); return frame
        except: continue
    raise Exception("❌ Result frame not found")

def fill_date_field(frame, field_id, value):
    try:
        frame.locator(f"input#{field_id}").fill(value)
        print(f"✅ {field_id} filled as input"); return
    except: pass
    try:
        frame.locator(f"#{field_id}").click()
        frame.locator(f"#{field_id}").fill("")
        frame.locator(f"#{field_id}").type(value,delay=50)
        print(f"✅ {field_id} filled with click+type"); return
    except: pass
    try:
        frame.evaluate(f"""(function(){{
            var elem=document.getElementById('{field_id}');
            var input=elem?elem.closest('div').querySelector('input'):null;
            if(input){{input.value='{value}';
                var ev=document.createEvent('Event');
                ev.initEvent('change',true,true);input.dispatchEvent(ev);}}}})();""")
        print(f"✅ {field_id} filled via evaluation")
    except Exception as e:
        print(f"⚠️ Could not fill {field_id}: {e}")

def apply_filter(page, start, end):
    frame=get_search_frame(page)
    print(f"Filling dates: {start} to {end}")
    fill_date_field(frame,"X45",start); time.sleep(1)
    fill_date_field(frame,"X46",end);   time.sleep(2)
    print("Triggering Search...")
    page.evaluate("""var btn=Array.from(document.querySelectorAll('button'))
        .find(function(b){return b.innerText&&b.innerText.trim()==='Search';});
        if(btn)btn.click();""")
    page.wait_for_timeout(6000)

def rename_export_file(download_file, label):
    if download_file:
        try:
            ts=datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]
            ext=os.path.splitext(download_file)[1]
            unique=os.path.join(DOWNLOAD_PATH,f"export_{label}_{ts}{ext}")
            os.rename(download_file,unique)
            print(f"✅ Renamed: {os.path.basename(unique)}")
            return unique
        except Exception as e:
            print(f"⚠️ Rename failed: {e}"); return download_file
    return download_file

def export_csv(page, label="export", already_downloaded=None):
    if already_downloaded is None: already_downloaded=[]
    existing=set()
    try:
        existing=set(os.path.join(DOWNLOAD_PATH,f) for f in os.listdir(DOWNLOAD_PATH)
                     if f.endswith('.csv') or f.endswith('.txt'))
    except: pass
    frame=get_result_frame(page)
    print("Waiting for result grid...")
    try:
        frame.wait_for_selector("#ext-comp-1004",state="attached",timeout=20000)
        print("✅ Result grid ready")
    except:
        try:
            frame.wait_for_selector("table.x-grid3-row-table",state="attached",timeout=15000)
            print("✅ Result grid ready (row table)")
        except:
            print("⚠️ Grid load uncertain"); page.wait_for_timeout(5000)
    print("Clicking Count Records...")
    frame.evaluate("""var btn=Array.from(document.querySelectorAll('button'))
        .find(function(b){return b.innerText&&b.innerText.trim()==='Count Records';});
        if(btn)btn.click();""")
    page.wait_for_timeout(3000)

    # More button
    print("\n📋 STEP 1: More button...")
    more_clicked=False
    for idx,tf in enumerate(page.frames):
        try: tf.locator("button#ext-gen-listdetail-1-60").click(timeout=3000); print(f"✅ More (frame {idx})"); more_clicked=True; break
        except: pass
    if not more_clicked:
        try: page.locator("button#ext-gen-listdetail-1-60").click(timeout=3000); more_clicked=True
        except: pass
    if not more_clicked:
        for idx,tf in enumerate(page.frames):
            try: tf.locator("button[aria-haspopup='true']").first.click(timeout=3000); more_clicked=True; break
            except: pass
    if not more_clicked:
        try: page.locator("button[aria-haspopup='true']").first.click(timeout=3000); more_clicked=True
        except: pass
    if not more_clicked:
        raise Exception("❌ Could not find More button")
    page.wait_for_timeout(2000)

    # Export to Text File
    print("\n📤 STEP 2: Export to Text File...")
    export_clicked=False
    for idx,tf in enumerate(page.frames):
        try: tf.locator("text=Export to Text File").first.click(timeout=3000); export_clicked=True; break
        except: pass
    if not export_clicked:
        try: page.locator("text=Export to Text File").first.click(timeout=3000); export_clicked=True
        except: pass
    if not export_clicked: raise Exception("❌ Could not find Export to Text File")
    page.wait_for_timeout(3000)

    # Select CSV
    print("\n🔘 STEP 3: Select CSV...")
    csv_selected=False
    for idx,tf in enumerate(page.frames):
        try: tf.locator("#X6").click(timeout=3000); csv_selected=True; break
        except: pass
    if not csv_selected:
        try: page.locator("#X6").click(timeout=3000); csv_selected=True
        except: pass
    if not csv_selected:
        for idx,tf in enumerate(page.frames):
            try: tf.locator("label[for='X6']").click(timeout=3000); csv_selected=True; break
            except: pass
    if not csv_selected: print("⚠️ Could not select CSV")
    page.wait_for_timeout(2000)

    # Download
    print("\n⬇️  STEP 4: Download...")
    download_file=None
    for idx,tf in enumerate(page.frames):
        try:
            with page.expect_download(timeout=15000) as dl:
                tf.locator("img#X21").click(timeout=5000)
            d=dl.value; fp=os.path.join(DOWNLOAD_PATH,d.suggested_filename)
            d.save_as(fp); download_file=fp
            print(f"✅ Downloaded: {os.path.basename(fp)}"); break
        except: pass
    if not download_file:
        try:
            with page.expect_download(timeout=15000) as dl:
                page.locator("img#X21").click(timeout=5000)
            d=dl.value; fp=os.path.join(DOWNLOAD_PATH,d.suggested_filename)
            d.save_as(fp); download_file=fp
            print(f"✅ Downloaded: {os.path.basename(fp)}")
        except: pass
    if not download_file:
        for idx,tf in enumerate(page.frames):
            try:
                with page.expect_download(timeout=15000) as dl:
                    tf.evaluate("var img=document.querySelector('img#X21');if(img)img.click();")
                d=dl.value; fp=os.path.join(DOWNLOAD_PATH,d.suggested_filename)
                d.save_as(fp); download_file=fp
                print(f"✅ Downloaded via eval: {os.path.basename(fp)}"); break
            except: pass
    if not download_file:
        print("⏳ Folder scan (5s)..."); time.sleep(5)
        try:
            current=set(os.path.join(DOWNLOAD_PATH,f) for f in os.listdir(DOWNLOAD_PATH)
                        if f.endswith('.csv') or f.endswith('.txt'))
            new=sorted(current-existing-set(already_downloaded),key=os.path.getmtime,reverse=True)
            if new: download_file=new[0]; print(f"✅ Found: {os.path.basename(download_file)}")
            else: print("❌ No new file found")
        except Exception as e: print(f"❌ Scan error: {e}")
    return rename_export_file(download_file,label)

def get_row_count(csv_path):
    try: return len(pd.read_csv(csv_path))
    except Exception as e: print(f"⚠️ Row count error: {e}"); return 0


# ══════════════════════════════════════════
#  📝 REPORT BUILDERS
# ══════════════════════════════════════════
def display_summary_message(login_duration,now,one_hour_back,seven_start,seven_end,
                             current_count,past_count,metrics,uoc_results):
    def fmt(val): return f"{val:,}" if isinstance(val,int) else str(val)
    def icon(val,limit):
        if not isinstance(val,int): return ""
        return "✅" if val<limit else "⚠️"
    uoc_lines=""
    for r in uoc_results:
        if r['error']: uoc_lines+=f"   • {r['name']}: ERROR ❌\n"
        else: uoc_lines+=f"   • {r['name']}: {r['seconds']}s {r['status']}\n"
    msg=(
        f"\n📢Service Manager HC\n"
        f"🗓️  {now.strftime('%d-%b-%y')} {now.strftime('%I:%M %p')}\n"
        f"⏱  Login Duration: {login_duration:02d} Secs\n"
        f"👉🏻 Tickets (Last 1 Hour | {one_hour_back.strftime('%I:%M')}–{now.strftime('%I:%M %p')}): {fmt(current_count)}\n"
        f"👉🏻 Tickets Same Time Last Week ({seven_start.strftime('%d-%b-%Y')} | "
        f"{seven_start.strftime('%I:%M')}-{seven_end.strftime('%I:%M %p')}): {fmt(past_count)}\n"
        f"📧 Email Queue:\n   • Threshold: < 500\n"
        f"   • Current Count: {fmt(metrics.get('email_count','N/A'))} {icon(metrics.get('email_count'),500)}\n"
        f"📩 SMS Queue:\n   • Threshold: < 15,000\n"
        f"   • Current Count: {fmt(metrics.get('sms_count','N/A'))} {icon(metrics.get('sms_count'),15000)}\n"
        f"📩 SMtoWFM Queue:\n   • Threshold: < 300\n"
        f"   • Current Count: {fmt(metrics.get('wfm_count','N/A'))} {icon(metrics.get('wfm_count'),300)}\n"
        f"🔐 AD Success Count (Last 1 Hour): {fmt(metrics.get('ad_success_count','N/A'))}\n"
        f"👤 UOC Login Performance:\n   • Threshold: < {UOC_THRESHOLD_SECS} seconds\n"
        f"{uoc_lines}"
    )
    print(msg); return msg

def build_pivot_df(current_csv, past_csv):
    """Build overall category pivot (all categories)."""
    current_df=pd.read_csv(current_csv); past_df=pd.read_csv(past_csv)
    category_col=_detect_category_col(current_df)
    print(f"\n✅ Using '{category_col}' as category column")
    cc=current_df[category_col].value_counts().sort_index()
    pc=past_df[category_col].value_counts().sort_index()
    all_cats=sorted(set(cc.index)|set(pc.index))
    rows=[{"CATEGORY":cat,"Today":int(cc.get(cat,0)),"Last Week":int(pc.get(cat,0))} for cat in all_cats]
    df=pd.DataFrame(rows)
    total=pd.DataFrame([{"CATEGORY":"TOTAL","Today":df["Today"].sum(),"Last Week":df["Last Week"].sum()}])
    return pd.concat([df,total],ignore_index=True)


# [UPDATED] Helper: auto-detect the category column in a dataframe
def _detect_category_col(df):
    """Return the category/service column name from a dataframe."""
    if CATEGORY_COLUMN and CATEGORY_COLUMN in df.columns:
        return CATEGORY_COLUMN
    for col in ['Service','Category','Type','Service_Type']:
        if col in df.columns:
            return col
    return df.columns[4] if len(df.columns) > 4 else df.columns[0]


# [UPDATED] Helper: auto-detect the location column in a dataframe
def _detect_location_col(df):
    """Return the location/circle column name from a dataframe."""
    if LOCATION_COLUMN and LOCATION_COLUMN in df.columns:
        return LOCATION_COLUMN
    for col in ['Location Full Name','Circle','Location','Region','Zone','State','City']:  # [UPDATED]
        if col in df.columns:
            return col
    print(f"  ⚠️  Location column not found. Available columns: {list(df.columns)}")
    return None


# [UPDATED] NEW FUNCTION: Build location breakdown for a specific category (e.g. BSS or CEN)
def build_location_pivot_df(current_csv, past_csv, category_name):
    """
    Filter rows where category == category_name, then group by location.
    Returns a DataFrame with columns: LOCATION, Today, Last Week
    with a TOTAL row at the bottom.
    """
    print(f"\n📍 Building location breakdown for category: {category_name}")
    current_df = pd.read_csv(current_csv)
    past_df    = pd.read_csv(past_csv)

    category_col  = _detect_category_col(current_df)
    location_col  = _detect_location_col(current_df)

    if location_col is None:
        print(f"  ⚠️  Skipping {category_name} location pivot — location column missing")
        return None

    # Filter rows for this category only
    cur_filtered = current_df[current_df[category_col].str.strip() == category_name.strip()]
    pas_filtered = past_df[past_df[_detect_category_col(past_df)].str.strip() == category_name.strip()]

    loc_col_past = _detect_location_col(past_df) or location_col

    cc = cur_filtered[location_col].value_counts().sort_index()
    pc = pas_filtered[loc_col_past].value_counts().sort_index()

    all_locs = sorted(set(cc.index) | set(pc.index))
    rows = [{"LOCATION": loc, "Today": int(cc.get(loc, 0)), "Last Week": int(pc.get(loc, 0))}
            for loc in all_locs]

    df = pd.DataFrame(rows)
    total = pd.DataFrame([{
        "LOCATION": "TOTAL",
        "Today":    int(df["Today"].sum()),
        "Last Week":int(df["Last Week"].sum())
    }])
    result = pd.concat([df, total], ignore_index=True)

    print(f"  ✅ {category_name} — Today Total: {df['Today'].sum()}  |  Last Week Total: {df['Last Week'].sum()}")
    print(result.to_string(index=False))
    return result


def generate_html_report(df, one_hour_back, now, summary_text, output_path,
                          bss_location_df=None, cen_location_df=None):
    """
    [UPDATED] HTML report now includes:
      1. Summary box
      2. Overall category comparison table (all categories)
      3. BSS location breakdown table (new)
      4. CEN location breakdown table (new)
    Each table has a Copy button.
    Colors match Image 3: green = higher today, red = lower today.
    """
    today_label   = f"{one_hour_back.strftime('%I:%M %p').lstrip('0')} – {now.strftime('%I:%M %p').lstrip('0')}"
    lastweek_label= f"Last Week ({one_hour_back.strftime('%I:%M %p').lstrip('0')} – {now.strftime('%I:%M %p').lstrip('0')})"
    title_range   = f"{one_hour_back.strftime('%H-%M')}_to_{now.strftime('%H-%M')}"
    date_label    = now.strftime("%d %b %Y")

    def build_table_rows(dataframe, col_name):
        """Build <tbody> rows with green/red coloring for today vs last week."""
        rows_html = ""
        data_rows = dataframe[dataframe[col_name] != "TOTAL"]
        for i, (_, row) in enumerate(data_rows.iterrows()):
            bg = "#ffffff" if i % 2 == 0 else "#f5f5f5"
            today_val = int(row['Today'])
            lw_val    = int(row['Last Week'])
            if today_val > lw_val:
                today_color = "color:#1a8a1a;font-weight:bold;"   # green — higher today
            elif today_val < lw_val:
                today_color = "color:#cc0000;font-weight:bold;"   # red   — lower today
            else:
                today_color = "font-weight:bold;"
            rows_html += f"""<tr style="background:{bg};">
            <td style="font-weight:bold;padding:8px 14px;border:1px solid #ccc;text-align:center;">{row[col_name]}</td>
            <td style="padding:8px 14px;border:1px solid #ccc;text-align:center;{today_color}">{today_val:,}</td>
            <td style="padding:8px 14px;border:1px solid #ccc;text-align:center;font-weight:bold;">{lw_val:,}</td>
            </tr>"""
        return rows_html

    # ── Overall category table ──────────────────────────────────────
    overall_rows  = build_table_rows(df, "CATEGORY")
    overall_total = df[df["CATEGORY"] == "TOTAL"].iloc[0]

    # ── BSS location table ──────────────────────────────────────────
    # [UPDATED] BSS location table HTML — empty string if no data
    bss_table_html = ""
    if bss_location_df is not None and not bss_location_df.empty:
        bss_rows  = build_table_rows(bss_location_df, "LOCATION")
        bss_total = bss_location_df[bss_location_df["LOCATION"] == "TOTAL"].iloc[0]
        bss_table_html = f"""
        <div class="section-header" style="margin-top:36px;">
            <h2>📍 BSS — Location Breakdown ({today_label} | {date_label})</h2>
            <button class="copy-btn" onclick="copyTable('bss-table')">📋 Copy BSS Table</button>
        </div>
        <p class="legend">Green = Higher than last week &nbsp;&nbsp; Red = Lower than last week</p>
        <table id="bss-table">
            <thead><tr>
                <th>LOCATION</th>
                <th>Today ({today_label})</th>
                <th>{lastweek_label}</th>
            </tr></thead>
            <tbody>{bss_rows}</tbody>
            <tfoot><tr>
                <td>TOTAL</td>
                <td>{int(bss_total['Today']):,}</td>
                <td>{int(bss_total['Last Week']):,}</td>
            </tr></tfoot>
        </table>"""

    # ── CEN location table ──────────────────────────────────────────
    # [UPDATED] CEN location table HTML — empty string if no data
    cen_table_html = ""
    if cen_location_df is not None and not cen_location_df.empty:
        cen_rows  = build_table_rows(cen_location_df, "LOCATION")
        cen_total = cen_location_df[cen_location_df["LOCATION"] == "TOTAL"].iloc[0]
        cen_table_html = f"""
        <div class="section-header" style="margin-top:36px;">
            <h2>📍 CEN — Location Breakdown ({today_label} | {date_label})</h2>
            <button class="copy-btn" onclick="copyTable('cen-table')">📋 Copy CEN Table</button>
        </div>
        <p class="legend">Green = Higher than last week &nbsp;&nbsp; Red = Lower than last week</p>
        <table id="cen-table">
            <thead><tr>
                <th>LOCATION</th>
                <th>Today ({today_label})</th>
                <th>{lastweek_label}</th>
            </tr></thead>
            <tbody>{cen_rows}</tbody>
            <tfoot><tr>
                <td>TOTAL</td>
                <td>{int(cen_total['Today']):,}</td>
                <td>{int(cen_total['Last Week']):,}</td>
            </tr></tfoot>
        </table>"""

    summary_html = summary_text.strip().replace("\n","<br>")

    # [UPDATED] Full HTML with copy buttons and colour legend
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>HC Ticket Report — {date_label}</title>
<style>
  body{{font-family:Arial,sans-serif;margin:30px;background:#fff}}
  .summary-box{{background:#f0f7ff;border:1px solid #b0cfe8;border-radius:8px;
    padding:16px 22px;margin-bottom:28px;font-size:14px;line-height:1.8;max-width:700px}}
  .section-header{{display:flex;align-items:center;gap:16px;flex-wrap:wrap;margin-bottom:6px}}
  h2{{font-size:17px;margin:0;color:#222}}
  .legend{{font-size:12px;color:#555;font-style:italic;margin:0 0 10px;}}
  .copy-btn{{background:#1565c0;color:#fff;border:none;border-radius:5px;
    padding:7px 16px;font-size:13px;cursor:pointer;}}
  .copy-btn:hover{{background:#0d47a1}}
  table{{border-collapse:collapse;width:100%;max-width:800px;margin:0 auto 10px;font-size:13px}}
  thead tr th{{background-color:#1565c0;color:#fff;font-weight:bold;
    padding:10px 14px;border:1px solid #aaa;text-align:center}}
  tfoot tr td{{background-color:#1a2a44;color:#fff;font-weight:bold;
    padding:10px 14px;border:1px solid #aaa;text-align:center;font-size:14px}}
  .copy-toast{{position:fixed;bottom:30px;right:30px;background:#333;color:#fff;
    padding:10px 20px;border-radius:6px;display:none;font-size:14px;z-index:999}}
</style>
</head>
<body>
<div class="summary-box">{summary_html}</div>

<!-- Overall category comparison -->
<div class="section-header">
  <h2>📊 Ticket Count Comparison ({today_label} | {date_label})</h2>
  <button class="copy-btn" onclick="copyTable('main-table')">📋 Copy Main Table</button>
</div>
<p class="legend">Green = Higher than last week &nbsp;&nbsp; Red = Lower than last week</p>
<table id="main-table">
  <thead><tr>
    <th>CATEGORY</th>
    <th>Today ({today_label})</th>
    <th>{lastweek_label}</th>
  </tr></thead>
  <tbody>{overall_rows}</tbody>
  <tfoot><tr>
    <td>TOTAL</td>
    <td>{int(overall_total['Today']):,}</td>
    <td>{int(overall_total['Last Week']):,}</td>
  </tr></tfoot>
</table>

{bss_table_html}
{cen_table_html}

<div class="copy-toast" id="toast">✅ Copied to clipboard!</div>

<script>
/* [UPDATED] Copy table as tab-separated text for pasting into Excel/chat */
function copyTable(tableId) {{
  const table = document.getElementById(tableId);
  if (!table) return;
  let text = '';
  for (const row of table.querySelectorAll('tr')) {{
    const cells = Array.from(row.querySelectorAll('th,td'));
    text += cells.map(c => c.innerText.trim()).join('\\t') + '\\n';
  }}
  navigator.clipboard.writeText(text).then(() => {{
    const toast = document.getElementById('toast');
    toast.style.display = 'block';
    setTimeout(() => toast.style.display = 'none', 2000);
  }});
}}
</script>
</body></html>"""

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"\n✅ HTML report saved: {output_path}")
    return output_path

def display_pivot_table(df,one_hour_back,now):
    today_label=f"Today ({one_hour_back.strftime('%I:%M %p')} – {now.strftime('%I:%M %p')})"
    lastweek_label=f"Last Week ({one_hour_back.strftime('%I:%M %p')} – {now.strftime('%I:%M %p')})"
    display_df=df.rename(columns={"Today":today_label,"Last Week":lastweek_label})
    print("\n"+"="*90+"\n📊 TICKET COUNT COMPARISON\n"+"="*90)
    pd.set_option('display.max_rows',None); pd.set_option('display.max_columns',None)
    pd.set_option('display.width',None); pd.set_option('display.max_colwidth',None)
    print(display_df.to_string(index=False)); print("="*90)


# ══════════════════════════════════════════
#  🚀 MAIN RUN
# ══════════════════════════════════════════
def run_once():
    run_start=datetime.now()
    print("\n"+"█"*60)
    print(f"  🚀 HC REPORT RUN STARTED: {run_start.strftime('%d-%b-%Y %I:%M %p')}")
    print("█"*60)
    one_hour_back,now,seven_start,seven_end=get_time_ranges()

    # Mark dashboard as "running"
    write_dashboard_json("running",None,now,one_hour_back,seven_start,seven_end,
                          None,None,None,[],None,None)

    login_duration=0
    metrics={}
    uoc_results=[]
    pivot_df=None
    # [UPDATED] Initialize location pivot dataframes
    bss_location_df=None
    cen_location_df=None

    with sync_playwright() as p:
        browser=p.chromium.launch(headless=False)
        context=browser.new_context(accept_downloads=True)
        page=context.new_page()
        try:
            # 1. LOGIN
            login_duration=login(page)

            # 2. UOC
            print("\n"+"="*50+"\n👤 CHECKING UOC LOGIN PERFORMANCE\n"+"="*50)
            uoc_results=check_uoc_login(context)

            # 3. DASHBOARD METRICS
            print("\n"+"="*50+"\n📊 SCRAPING HC DASHBOARD METRICS\n"+"="*50)
            metrics=scrape_dashboard_metrics(page)

            # 4. TODAY
            print("\n"+"="*50+"\n📊 FETCHING TODAY'S DATA (Last 1 Hour)\n"+"="*50)
            navigate_to_search(page)
            apply_filter(page,format_time(one_hour_back),format_time(now))
            current_csv=export_csv(page,label="TODAY",already_downloaded=[])
            print(f"  TODAY CSV: {current_csv}")

            # 5. LASTWEEK
            print("\n"+"="*50+"\n📊 FETCHING LAST WEEK'S DATA (Same Time)\n"+"="*50)
            navigate_to_search(page)
            apply_filter(page,format_time(seven_start),format_time(seven_end))
            past_csv=export_csv(page,label="LASTWEEK",
                                already_downloaded=[current_csv] if current_csv else [])
            print(f"  LASTWEEK CSV: {past_csv}")

            if not current_csv or not os.path.exists(current_csv):
                raise FileNotFoundError(f"TODAY CSV missing: {current_csv}")
            if not past_csv or not os.path.exists(past_csv):
                raise FileNotFoundError(f"LASTWEEK CSV missing: {past_csv}")

            current_count=get_row_count(current_csv)
            past_count=get_row_count(past_csv)

            # 6. SUMMARY
            print("\n"+"="*50+"\n📢 HC SUMMARY MESSAGE\n"+"="*50)
            summary_text=display_summary_message(login_duration,now,one_hour_back,
                seven_start,seven_end,current_count,past_count,metrics,uoc_results)

            # 7. PIVOT (overall)
            print("\n"+"="*50+"\n📈 CREATING OVERALL PIVOT TABLE\n"+"="*50)
            pivot_df=build_pivot_df(current_csv,past_csv)
            display_pivot_table(pivot_df,one_hour_back,now)

            # [UPDATED] 7b. BSS LOCATION PIVOT
            print("\n"+"="*50+"\n📍 CREATING BSS LOCATION BREAKDOWN\n"+"="*50)
            bss_location_df = build_location_pivot_df(current_csv, past_csv, "BSS")

            # [UPDATED] 7c. CEN LOCATION PIVOT
            print("\n"+"="*50+"\n📍 CREATING CEN LOCATION BREAKDOWN\n"+"="*50)
            cen_location_df = build_location_pivot_df(current_csv, past_csv, "CEN")

            # 8. HTML REPORT — [UPDATED] now passes BSS & CEN location dataframes
            ts=datetime.now().strftime("%Y%m%d_%H%M%S")
            html_path=os.path.join(DOWNLOAD_PATH,f"HC_Report_{ts}.html")
            generate_html_report(pivot_df, one_hour_back, now, summary_text, html_path,
                                  bss_location_df=bss_location_df,
                                  cen_location_df=cen_location_df)

            # 9. PIVOT CSV
            csv_out=os.path.join(DOWNLOAD_PATH,f"pivot_{ts}.csv")
            pivot_df.to_csv(csv_out,index=False)
            print(f"✅ Pivot CSV saved: {csv_out}")

            # [UPDATED] Save BSS and CEN location CSVs separately
            if bss_location_df is not None:
                bss_csv=os.path.join(DOWNLOAD_PATH,f"bss_location_{ts}.csv")
                bss_location_df.to_csv(bss_csv,index=False)
                print(f"✅ BSS Location CSV saved: {bss_csv}")

            if cen_location_df is not None:
                cen_csv=os.path.join(DOWNLOAD_PATH,f"cen_location_{ts}.csv")
                cen_location_df.to_csv(cen_csv,index=False)
                print(f"✅ CEN Location CSV saved: {cen_csv}")

            elapsed=round((datetime.now()-run_start).total_seconds()/60,1)

            # ✅ 10. UPDATE DASHBOARD JSON — [UPDATED] passes BSS & CEN location data
            write_dashboard_json(
                "success", login_duration, now, one_hour_back, seven_start, seven_end,
                current_count, past_count, metrics, uoc_results, pivot_df, elapsed,
                bss_location_df=bss_location_df,
                cen_location_df=cen_location_df
            )
            print(f"\n✅ Run complete in {elapsed} min. Next run in {RUN_EVERY_MINUTES} minutes.")

        except Exception as e:
            elapsed=round((datetime.now()-run_start).total_seconds()/60,1)
            print(f"\n❌ Error during run: {e}")
            import traceback; traceback.print_exc()
            # ✅ Update dashboard with error state — [UPDATED] passes location data even on error
            write_dashboard_json(
                "error", login_duration, now, one_hour_back, seven_start, seven_end,
                None, None, metrics, uoc_results, pivot_df, elapsed, error=e,
                bss_location_df=bss_location_df,
                cen_location_df=cen_location_df
            )
        finally:
            browser.close()


# ══════════════════════════════════════════
#  ⏰ SCHEDULER
# ══════════════════════════════════════════
def main():
    print(f"⏰ HC Report Scheduler — every {RUN_EVERY_MINUTES} minutes")
    print(f"📱 Dashboard: open dashboard.html after running:")
    print(f"   python -m http.server 8080")
    print(f"   then open http://YOUR_IP:8080/dashboard.html on phone")
    print(f"   Ctrl+C to stop\n")
    run_once()
    schedule.every(RUN_EVERY_MINUTES).minutes.do(run_once)
    while True:
        schedule.run_pending()
        time.sleep(30)

if __name__=="__main__":
    main() #;