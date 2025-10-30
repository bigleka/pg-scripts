"""
Vacuum Analyzer - Tkinter + ttkbootstrap GUI
Arquivo único: vacuum_analyzer.py

Objetivo:
 - Cola saída de VACUUM VERBOSE / ANALYZE VERBOSE
 - Analisa, extrai métricas por tabela e índices
 - Mostra resumo em Treeview, gráficos embutidos (matplotlib) e painel de detalhes
 - Gera alertas simples (dead tuples, vacuum truncado, long running)

Requisitos:
 - Python 3.10+
 - pip install ttkbootstrap matplotlib pandas

Execução:
 python vacuum_analyzer.py

Notas:
 - Este é um protótipo: parser baseado em expressões regulares que cobre os padrões comuns do VACUUM VERBOSE/ANALYZE.
 - Você pode colar a saída completa no campo "Saída" e clicar em "Analisar".
 - Ainda não está perfeito, mas é um ponto de partida.

"""

import re
import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext
import ttkbootstrap as tb

# Vacuum & Analyze Enhanced Analyzer
# - Parse ANALYZE VERBOSE and VACUUM VERBOSE (detailed blocks)
# - Show results in a scrollable Treeview
# - Compute Efficiency (%) and order recommendations by severity

def parse_analyze_blocks(text):
    """Parse ANALYZE VERBOSE lines like the example provided by the user.

    Returns dict: {table: {scanned, total_pages, live, dead, sample, estimated}}
    """
    pattern = re.compile(
        r'analyzing\s+"(?P<table>[^"]+)"\s*\n\s*"[^"]+":\s*scanned\s+(?P<scanned>\d+)\s+of\s+(?P<total_pages>\d+)\s+pages,\s*containing\s+(?P<live>\d+)\s+live\s+rows\s+and\s+(?P<dead>\d+)\s+dead\s+rows;\s*(?P<sample>\d+)\s+rows\s+in\s+sample,\s*(?P<estimated>\d+)\s+estimated',
        re.IGNORECASE | re.MULTILINE,
    )
    results = {}
    for m in pattern.finditer(text):
        gd = m.groupdict()
        table = gd['table']
        results[table] = {
            'scanned': int(gd['scanned']),
            'total_pages': int(gd['total_pages']),
            'live': int(gd['live']),
            'dead': int(gd['dead']),
            'sample': int(gd['sample']),
            'estimated': int(gd['estimated']),
            'source': 'ANALYZE'
        }
    return results

def parse_vacuum_blocks(text):
    """Parse detailed VACUUM VERBOSE blocks between vacuuming "..." and the finished vacuuming line.

    For each block we extract pages, tuples, removed, frozen info and elapsed time if present.

    Returns dict: {table: {pages_removed, pages_remain, pages_scanned, pages_pct, tuples_removed, tuples_remain, tuples_dead, elapsed_s, source}}
    """
    blocks = {}
    # Find vacuuming ... finished vacuuming blocks (non-greedy)
    block_pattern = re.compile(r'vacuuming\s+"(?P<table>[^"]+)"\s*(?P<body>.*?)finished\s+vacuuming\s+"(?P=table)":(?P<tail>.*?)(?=(?:\nvacuuming\s+"|\Z))', re.IGNORECASE | re.DOTALL)

    pages_re = re.compile(r'pages:\s*(?P<removed>\d+)\s*removed,\s*(?P<remain>\d+)\s*remain,\s*(?P<scanned>\d+)\s*scanned\s*\((?P<pct>[0-9\.]+)% of total\)', re.IGNORECASE)
    tuples_re = re.compile(r'tuples:\s*(?P<removed>\d+)\s*removed,\s*(?P<remain>\d+)\s*remain,\s*(?P<dead>\d+)\s*are dead', re.IGNORECASE)
    elapsed_re = re.compile(r'elapsed:\s*(?P<secs>[0-9\.]+)\s*s', re.IGNORECASE)

    for bm in block_pattern.finditer(text):
        table = bm.group('table')
        body = bm.group('body') + '\n' + (bm.group('tail') or '')
        pages_m = pages_re.search(body)
        tuples_m = tuples_re.search(body)
        elapsed_m = elapsed_re.search(body)

        pages_removed = pages_remain = pages_scanned = pages_pct = 0
        tup_removed = tup_remain = tup_dead = 0
        elapsed = 0.0

        if pages_m:
            pages_removed = int(pages_m.group('removed'))
            pages_remain = int(pages_m.group('remain'))
            pages_scanned = int(pages_m.group('scanned'))
            try:
                pages_pct = float(pages_m.group('pct'))
            except Exception:
                pages_pct = 0.0
        if tuples_m:
            tup_removed = int(tuples_m.group('removed'))
            tup_remain = int(tuples_m.group('remain'))
            tup_dead = int(tuples_m.group('dead'))
        if elapsed_m:
            elapsed = float(elapsed_m.group('secs'))

        blocks[table] = {
            'pages_removed': pages_removed,
            'pages_remain': pages_remain,
            'pages_scanned': pages_scanned,
            'pages_pct': pages_pct,
            'tuples_removed': tup_removed,
            'tuples_remain': tup_remain,
            'tuples_dead': tup_dead,
            'elapsed_s': elapsed,
            'source': 'VACUUM'
        }
    return blocks

def compute_efficiency(vac_entry):
    """Compute a simple efficiency metric (0-100).

    If there are no tuples reported, return 100.
    Otherwise efficiency = 100 - (dead / total) * 100
    Bound between 0 and 100.
    """
    dead = vac_entry.get('tuples_dead', 0)
    removed = vac_entry.get('tuples_removed', 0)
    remain = vac_entry.get('tuples_remain', 0)
    total = dead + removed + remain
    if total <= 0:
        return 100.0
    pct_dead = dead / total
    eff = 100.0 - pct_dead * 100.0
    if eff < 0:
        eff = 0.0
    if eff > 100:
        eff = 100.0
    return round(eff, 1)

def severity_score(entry):
    """Return numeric severity score (higher => more urgent).

    For ANALYZE entries, higher dead% => higher severity.
    For VACUUM entries, consider tuples_dead, pages_remain, elapsed.
    """
    src = entry.get('source')
    score = 0
    if src == 'ANALYZE':
        dead = entry.get('dead', 0)
        live = entry.get('live', 0)
        total = dead + live
        pct_dead = (dead / total * 100.0) if total > 0 else 0.0
        score += int(pct_dead * 2)
        if total == 0:
            score += 0
    else:  # VACUUM
        dead = entry.get('tuples_dead', 0)
        removed = entry.get('tuples_removed', 0)
        pages_remain = entry.get('pages_remain', 0)
        elapsed = entry.get('elapsed_s', 0.0)
        total = dead + removed + entry.get('tuples_remain', 0)
        pct_dead = (dead / total * 100.0) if total > 0 else 0.0
        score += int(pct_dead * 3)
        score += int(dead / 1000)
        if elapsed > 5.0:
            score += int(min(20, elapsed))
        if pages_remain > 10000:
            score += 10
    return score

def analyze_text_and_generate(entries_analyze, entries_vacuum):
    """Combine entries and produce table rows and ordered recommendations."""
    combined = {}
    # normalize keys (tables may appear in both)
    for k, v in entries_analyze.items():
        combined[k] = v.copy()
    for k, v in entries_vacuum.items():
        if k in combined:
            combined[k].update(v)
        else:
            combined[k] = v.copy()
    # compute efficiency for vacuum entries (if present)
    rows = []
    recs = []
    for table, val in combined.items():
        # merge source heuristics
        src = val.get('source', 'ANALYZE' if 'dead' in val else 'VACUUM')
        eff = 100.0
        if src == 'VACUUM' or 'tuples_dead' in val:
            eff = compute_efficiency(val)
        elif 'dead' in val or 'live' in val:
            # derive efficiency from analyze
            dead = val.get('dead', 0)
            live = val.get('live', 0)
            tot = dead + live
            eff = 100.0 if tot == 0 else round(100.0 - (dead / tot * 100.0), 1)
        val['efficiency'] = eff
        # build row
        row = {
            'table': table,
            'source': src,
            'eff': eff,
            'dead': val.get('tuples_dead', val.get('dead', 0)),
            'live': val.get('live', 0),
            'pages_scanned': val.get('pages_scanned', val.get('scanned', 0)),
            'pages_total': val.get('pages_remain', val.get('total_pages', 0)),
            'elapsed_s': val.get('elapsed_s', 0.0),
            'raw': val,
        }
        rows.append(row)
        # generate recommendation text with severity
        score = severity_score(val)
        # craft messages
        messages = []
        # critical conditions
        if row['dead'] > 100000 or (row['dead'] > 0 and row['eff'] < 50):
            messages.append((3, f'⚠️ CRÍTICO — {table}: {row["dead"]} linhas mortas; eficiência {row["eff"]}%'))
        elif row['dead'] > 10000 or (row['dead'] > 0 and row['eff'] < 70):
            messages.append((2, f'⚠️ ALTA — {table}: {row["dead"]} linhas mortas; eficiência {row["eff"]}%'))
        elif row['dead'] > 0:
            messages.append((1, f'⚠️ — {table}: {row["dead"]} linhas mortas; eficiência {row["eff"]}%'))
        else:
            messages.append((0, f'✅ {table}: sem linhas mortas aparente; eficiência {row["eff"]}%'))

        # extra hints
        if row['elapsed_s'] and row['elapsed_s'] > 5.0:
            messages.append((1, f'⏱️ Demorado: {row["elapsed_s"]:.2f} s'))
        if row['pages_scanned'] and row['pages_scanned'] > 1000:
            messages.append((1, f'📄 Muitas páginas escaneadas: {row["pages_scanned"]}'))

        for sev, msg in messages:
            recs.append((severity_score(val) + sev, sev, msg))

    # sort rows by dead desc then efficiency asc
    rows.sort(key=lambda r: (-r['dead'], r['eff']))
    # sort recs by primary key (higher first) then severity
    recs.sort(key=lambda t: (-t[0], -t[1]))
    sorted_msgs = [r[2] for r in recs]
    return rows, sorted_msgs

# ---------------- UI ----------------
class AnalyzerUI:
    def __init__(self, root):
        self.root = root
        self.root.title('Vacuum & Analyze Enhanced')
        self.root.geometry('1000x700')
        self.style = tb.Style('darkly')
        self._build()

    def _build(self):
        frm = ttk.Frame(self.root, padding=8)
        frm.pack(fill='both', expand=True)

        lbl = ttk.Label(frm, text='Cole a saída do VACUUM VERBOSE / ANALYZE VERBOSE aqui:')
        lbl.pack(anchor='w')

        self.input_txt = scrolledtext.ScrolledText(frm, height=14)
        self.input_txt.pack(fill='both', expand=False)

        btn = ttk.Button(frm, text='Analisar', command=self.on_analyze)
        btn.pack(pady=6)

        # Results frame with treeview + scrollbar
        res_frame = ttk.Frame(frm)
        res_frame.pack(fill='both', expand=True)

        cols = ('Tabela', 'Fonte', 'Dead', 'Live', 'Pgs Scanned', 'Pgs Total', 'Elapsed(s)', 'Eficiência(%)')
        self.tree = ttk.Treeview(res_frame, columns=cols, show='headings', selectmode='browse')
        for c in cols:
            self.tree.heading(c, text=c)
            # give efficiency a bit more width
            if c == 'Tabela':
                self.tree.column(c, width=350, anchor='w')
            elif c == 'Eficiência(%)':
                self.tree.column(c, width=110, anchor='center')
            else:
                self.tree.column(c, width=100, anchor='center')

        ysb = ttk.Scrollbar(res_frame, orient='vertical', command=self.tree.yview)
        self.tree.configure(yscroll=ysb.set)
        ysb.pack(side='right', fill='y')
        self.tree.pack(fill='both', expand=True, side='left')

        # Recommendations box
        rec_label = ttk.Label(frm, text='Recomendações (ordenadas por importância):')
        rec_label.pack(anchor='w', pady=(8,0))
        self.rec_txt = scrolledtext.ScrolledText(frm, height=10, state='disabled')
        self.rec_txt.pack(fill='both', expand=False)

        # double click row -> show raw details
        self.tree.bind('<Double-1>', self.on_row_double)

    def on_analyze(self):
        raw = self.input_txt.get('1.0', 'end').strip()
        if not raw:
            messagebox.showwarning('Nada para analisar', 'Cole a saída do VACUUM/ANALYZE antes de analisar.')
            return
        an = parse_analyze_blocks(raw)
        vac = parse_vacuum_blocks(raw)
        rows, recs = analyze_text_and_generate(an, vac)

        # populate tree
        self.tree.delete(*self.tree.get_children())
        for r in rows:
            values = (r['table'], r['source'], r['dead'], r['live'], r['pages_scanned'], r['pages_total'], f"{r['elapsed_s']:.2f}", f"{r['eff']:.1f}")
            item = self.tree.insert('', 'end', values=values)
            # tag by efficiency
            eff = r['eff']
            if eff < 30:
                self.tree.item(item, tags=('bad',))
            elif eff < 70:
                self.tree.item(item, tags=('warn',))
            else:
                self.tree.item(item, tags=('ok',))
        # tag styles
        self.tree.tag_configure('bad', background='#4c1f1f')
        self.tree.tag_configure('warn', background='#4c3a1f')
        self.tree.tag_configure('ok', background='#113322')

        # populate recommendations
        self.rec_txt.configure(state='normal')
        self.rec_txt.delete('1.0', 'end')
        if not recs:
            self.rec_txt.insert('end', 'Nenhuma recomendação gerada.')
        else:
            for msg in recs:
                self.rec_txt.insert('end', msg + '\n')
        self.rec_txt.configure(state='disabled')

    def on_row_double(self, event):
        sel = self.tree.selection()
        if not sel:
            return
        vals = self.tree.item(sel[0], 'values')
        table = vals[0]
        # show details
        raw = self.input_txt.get('1.0', 'end')
        an = parse_analyze_blocks(raw)
        vac = parse_vacuum_blocks(raw)
        details = ''
        if table in vac:
            details += f'-- VACUUM data for {table}:\n'
            for k, v in vac[table].items():
                details += f'{k}: {v}\n'
        if table in an:
            details += f'\n-- ANALYZE data for {table}:\n'
            for k, v in an[table].items():
                details += f'{k}: {v}\n'
        if not details:
            details = 'Sem detalhes estruturados para esta tabela.'
        # modal
        dlg = tk.Toplevel(self.root)
        dlg.title(f'Detalhes - {table}')
        txt = scrolledtext.ScrolledText(dlg, width=100, height=30)
        txt.pack(fill='both', expand=True)
        txt.insert('end', details)
        txt.configure(state='disabled')

if __name__ == '__main__':
    root = tb.Window(themename='darkly')
    app = AnalyzerUI(root)
    root.mainloop()
