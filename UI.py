# clean_professional_tkinter.py
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import sqlite3
import pandas as pd
import os
import traceback
from datetime import datetime

LOG_FILE = "app_actions.log"
DEFAULT_DB = "app.db"
MAX_ROWS_DISPLAY = 5000

def log_action(msg: str):
    ts = datetime.now().isoformat(sep=" ", timespec="seconds")
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(f"[{ts}] {msg}\n")
    except Exception:
        pass

class DBManager:
    def __init__(self, db_path: str = DEFAULT_DB):
        self.db_path = db_path
        self.conn = None

    def connect(self, path: str = None):
        if path:
            self.db_path = path
        if self.conn:
            try:
                self.conn.close()
            except Exception:
                pass
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        log_action(f"Connesso a DB: {self.db_path}")

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None
            log_action("Connessione DB chiusa")

    def list_tables(self):
        cur = self.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name;"
        )
        return [r[0] for r in cur.fetchall()]

    def table_schema(self, table: str):
        cur = self.conn.execute(f"PRAGMA table_info('{table}')")
        return cur.fetchall()  # (cid, name, type, notnull, dflt_value, pk)

    def fetch_df(self, sql: str, params: tuple = ()):
        df = pd.read_sql_query(sql, self.conn, params=params)
        return df

    def safe_insert(self, table: str, data: dict):
        cols = list(data.keys())
        placeholders = ",".join(["?"] * len(cols))
        col_list = ",".join([f'"{c}"' for c in cols])
        sql = f'INSERT INTO "{table}" ({col_list}) VALUES ({placeholders})'
        vals = tuple(data[c] for c in cols)
        with self.conn:
            self.conn.execute(sql, vals)
        log_action(f"INSERT in {table}: {data}")

    def safe_update(self, table: str, update_data: dict, pk_col: str, pk_val):
        set_parts = ", ".join([f'"{k}" = ?' for k in update_data.keys()])
        sql = f'UPDATE "{table}" SET {set_parts} WHERE "{pk_col}" = ?'
        vals = tuple(update_data[k] for k in update_data.keys()) + (pk_val,)
        with self.conn:
            self.conn.execute(sql, vals)
        log_action(f"UPDATE {table} WHERE {pk_col}={pk_val}: {update_data}")

# ---------- Dialogs ----------
class InsertDialog(tk.Toplevel):
    def __init__(self, parent, dbm: DBManager, table: str):
        super().__init__(parent)
        self.parent = parent
        self.dbm = dbm
        self.table = table
        self.title(f"Inserisci in {table}")
        self.resizable(False, False)
        frm = ttk.Frame(self, padding=12)
        frm.pack(fill="both", expand=True)
        schema = dbm.table_schema(table)
        self.entries = {}
        for i, col in enumerate(schema):
            cid, name, ctype, notnull, dflt, pk = col
            if pk == 1 and ("INT" in (ctype or "").upper() or ctype == ""):
                continue
            ttk.Label(frm, text=f"{name} ({ctype})").grid(row=i, column=0, sticky="w", padx=4, pady=6)
            ent = ttk.Entry(frm, width=40)
            if dflt is not None:
                ent.insert(0, str(dflt))
            ent.grid(row=i, column=1, sticky="we", padx=4, pady=6)
            self.entries[name] = ent
        btns = ttk.Frame(frm)
        btns.grid(row=len(self.entries)+1, column=0, columnspan=2, pady=(8,0))
        ttk.Button(btns, text="Inserisci", command=self.on_ok).pack(side="left", padx=6)
        ttk.Button(btns, text="Annulla", command=self.destroy).pack(side="left")

    def on_ok(self):
        data = {}
        for k, ent in self.entries.items():
            v = ent.get().strip()
            data[k] = v if v != "" else None
        try:
            self.dbm.safe_insert(self.table, data)
            messagebox.showinfo("Inserimento", "Inserimento completato.")
            self.destroy()
            self.parent.load_current_table()
        except Exception as e:
            messagebox.showerror("Errore INSERT", str(e))
            log_action(f"ERR INSERT: {traceback.format_exc()}")

class UpdateDialog(tk.Toplevel):
    def __init__(self, parent, dbm: DBManager, table: str, pk_col: str, pk_val, row_data: dict):
        super().__init__(parent)
        self.parent = parent
        self.dbm = dbm
        self.table = table
        self.pk_col = pk_col
        self.pk_val = pk_val
        self.title(f"Aggiorna {table} | {pk_col}={pk_val}")
        self.resizable(False, False)
        frm = ttk.Frame(self, padding=12)
        frm.pack(fill="both", expand=True)
        schema = dbm.table_schema(table)
        self.entries = {}
        row = 0
        for col in schema:
            cid, name, ctype, notnull, dflt, pk = col
            if name == pk_col:
                ttk.Label(frm, text=f"{name} (PK)").grid(row=row, column=0, sticky="w", padx=4, pady=6)
                ttk.Label(frm, text=str(pk_val)).grid(row=row, column=1, sticky="w", padx=4, pady=6)
                row += 1
                continue
            ttk.Label(frm, text=f"{name} ({ctype})").grid(row=row, column=0, sticky="w", padx=4, pady=6)
            ent = ttk.Entry(frm, width=40)
            val = row_data.get(name, "")
            ent.insert(0, "" if val is None else str(val))
            ent.grid(row=row, column=1, sticky="we", padx=4, pady=6)
            self.entries[name] = ent
            row += 1
        btns = ttk.Frame(frm)
        btns.grid(row=row, column=0, columnspan=2, pady=(8,0))
        ttk.Button(btns, text="Aggiorna", command=self.on_ok).pack(side="left", padx=6)
        ttk.Button(btns, text="Annulla", command=self.destroy).pack(side="left")

    def on_ok(self):
        data = {}
        for k, ent in self.entries.items():
            v = ent.get().strip()
            data[k] = v if v != "" else None
        try:
            self.dbm.safe_update(self.table, data, self.pk_col, self.pk_val)
            messagebox.showinfo("Aggiornamento", "Aggiornamento completato.")
            self.destroy()
            self.parent.load_current_table()
        except Exception as e:
            messagebox.showerror("Errore UPDATE", str(e))
            log_action(f"ERR UPDATE: {traceback.format_exc()}")

# ---------- App ----------
class CleanApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("SQLite Manager — Interfaccia Pulita")
        self.geometry("1200x760")
        self.style = ttk.Style(self)
        try:
            self.style.theme_use("clam")
        except Exception:
            pass
        default_font = ("Segoe UI", 10)
        self.option_add("*Font", default_font)

        self.dbm = DBManager()
        if os.path.exists(DEFAULT_DB):
            try:
                self.dbm.connect(DEFAULT_DB)
            except Exception:
                pass

        # Main frames
        main = ttk.Frame(self, padding=8)
        main.pack(fill="both", expand=True)

        left = ttk.Frame(main, width=260, padding=8)
        left.pack(side="left", fill="y")
        center = ttk.Frame(main, padding=8)
        center.pack(side="left", fill="both", expand=True)
        right = ttk.Frame(main, width=300, padding=8)
        right.pack(side="left", fill="y")

        # LEFT: DB + tables
        ttk.Label(left, text="Database", font=("Segoe UI", 11, "bold")).pack(anchor="w")
        self.db_label = ttk.Label(left, text=f"{self.dbm.db_path if self.dbm.conn else '(non connesso)'}", wraplength=240)
        self.db_label.pack(anchor="w", pady=(4,8))
        ttk.Button(left, text="Apri DB", command=self.open_db).pack(fill="x")
        ttk.Button(left, text="Ricarica tabelle", command=self.refresh_tables).pack(fill="x", pady=(6,0))
        ttk.Separator(left, orient="horizontal").pack(fill="x", pady=8)

        ttk.Label(left, text="Tabelle", font=("Segoe UI", 10, "bold")).pack(anchor="w")
        self.tbl_list = tk.Listbox(left, height=20)
        self.tbl_list.pack(fill="both", expand=True, pady=(6,6))
        self.tbl_list.bind("<<ListboxSelect>>", lambda e: self.on_table_select())
        ttk.Button(left, text="Esegui createTables.py", command=self.run_create_script).pack(fill="x")

        # CENTER: toolbar, table, details
        toolbar = ttk.Frame(center)
        toolbar.pack(fill="x", pady=(0,8))

        ttk.Button(toolbar, text="Apri DB", command=self.open_db).pack(side="left", padx=(0,6))
        ttk.Button(toolbar, text="Carica tabella", command=self.load_current_table).pack(side="left", padx=(0,6))
        ttk.Button(toolbar, text="Apri editor SQL", command=self.open_sql_editor).pack(side="left", padx=(0,6))
        ttk.Button(toolbar, text="Inserisci", command=self.insert_row).pack(side="left", padx=(12,6))
        ttk.Button(toolbar, text="Modifica selezione", command=self.update_row).pack(side="left", padx=(0,6))
        ttk.Button(toolbar, text="Esporta CSV", command=self.export_csv).pack(side="left", padx=(12,6))

        # filter row
        filter_frame = ttk.Frame(center)
        filter_frame.pack(fill="x", pady=(0,8))
        ttk.Label(filter_frame, text="Filtro WHERE:").pack(side="left")
        self.where_var = tk.StringVar()
        ttk.Entry(filter_frame, textvariable=self.where_var, width=60).pack(side="left", padx=(6,6))
        ttk.Button(filter_frame, text="Applica filtro", command=self.load_current_table).pack(side="left")

        # Table view
        table_frame = ttk.Frame(center)
        table_frame.pack(fill="both", expand=True)
        self.tree = ttk.Treeview(table_frame, show="headings")
        self.tree.pack(side="left", fill="both", expand=True)
        vs = ttk.Scrollbar(table_frame, orient="vertical", command=self.tree.yview)
        vs.pack(side="left", fill="y")
        hs = ttk.Scrollbar(center, orient="horizontal", command=self.tree.xview)
        hs.pack(fill="x")
        self.tree.configure(yscrollcommand=vs.set, xscrollcommand=hs.set)
        self.tree.bind("<<TreeviewSelect>>", lambda e: self.on_row_select())
        self.tree.bind("<Double-1>", lambda e: self.on_row_double_click())

        # detail area
        details_frame = ttk.LabelFrame(center, text="Dettaglio riga selezionata", padding=8)
        details_frame.pack(fill="x", pady=(8,0))
        self.detail_text = tk.Text(details_frame, height=6, wrap="word", state="disabled")
        self.detail_text.pack(fill="both", expand=True)

        # RIGHT: help / instructions
        ttk.Label(right, text="Guida rapida", font=("Segoe UI", 11, "bold")).pack(anchor="w")
        help_msg = (
            "1) Apri DB → seleziona file .db\n"
            "2) Seleziona tabella a sinistra\n"
            "3) Inserisci filtro WHERE (es. date >= '2025-01-01') → Carica tabella\n"
            "4) Doppio clic su una riga per modificarla\n"
            "5) Usa 'Editor SQL' per query complesse\n"
            "6) Esporta CSV per salvare la vista\n\n"
            "Note:\n- Operazioni scrittura usano query parametrizzate (sicurezza).\n- Se il DB non ha tabelle, utilizza createTables.py"
        )
        ttk.Label(right, text=help_msg, wraplength=280, justify="left").pack(anchor="w", pady=(6,0))

        # status
        self.status_var = tk.StringVar(value="Pronto")
        status_bar = ttk.Label(self, textvariable=self.status_var, relief="sunken", anchor="w")
        status_bar.pack(side="bottom", fill="x")

        self.current_table = None
        self.current_df = pd.DataFrame()
        self.refresh_tables()

    # ---------- actions ----------
    def open_db(self):
        path = filedialog.askopenfilename(title="Seleziona DB SQLite", filetypes=[("DB files","*.db;*.sqlite;*.sqlite3"), ("All","*.*")])
        if not path:
            return
        try:
            self.dbm.connect(path)
            self.db_label.config(text=f"{self.dbm.db_path}")
            self.status_var.set(f"Connesso {self.dbm.db_path}")
            self.refresh_tables()
        except Exception as e:
            messagebox.showerror("Errore connessione", str(e))
            log_action(f"ERR connect: {traceback.format_exc()}")

    def refresh_tables(self):
        try:
            if not self.dbm.conn and os.path.exists(DEFAULT_DB):
                self.dbm.connect(DEFAULT_DB)
                self.db_label.config(text=f"{self.dbm.db_path}")
            if not self.dbm.conn:
                self.tbl_list.delete(0, tk.END)
                self.status_var.set("Nessun DB connesso")
                return
            tables = self.dbm.list_tables()
            self.tbl_list.delete(0, tk.END)
            for t in tables:
                self.tbl_list.insert(tk.END, t)
            self.status_var.set(f"Tabelle caricate: {len(tables)}")
            log_action("Tabelle aggiornate")
        except Exception as e:
            messagebox.showerror("Errore", str(e))
            log_action(f"ERR refresh_tables: {traceback.format_exc()}")

    def on_table_select(self):
        sel = self.tbl_list.curselection()
        if not sel:
            return
        self.current_table = self.tbl_list.get(sel[0])
        self.status_var.set(f"Selezionata: {self.current_table}")

    def load_current_table(self):
        if not self.current_table:
            messagebox.showinfo("Seleziona tabella", "Seleziona prima una tabella a sinistra.")
            return
        where = self.where_var.get().strip()
        sql = f'SELECT * FROM "{self.current_table}"'
        if where:
            sql += " WHERE " + where
        try:
            df = self.dbm.fetch_df(sql)
            self.current_df = df
            self.populate_tree(df)
            self.status_var.set(f"Caricate {len(df)} righe da {self.current_table}")
            log_action(f"load_table {self.current_table} -> {len(df)} righe")
        except Exception as e:
            messagebox.showerror("Errore query", str(e))
            log_action(f"ERR load_table: {traceback.format_exc()}")

    def populate_tree(self, df: pd.DataFrame):
        # clear
        for c in self.tree.get_children():
            self.tree.delete(c)
        if df is None or df.empty:
            self.tree["columns"] = []
            self.detail_text.config(state="normal")
            self.detail_text.delete("1.0", tk.END)
            self.detail_text.insert("1.0", "Nessuna riga selezionata")
            self.detail_text.config(state="disabled")
            return
        self.tree["columns"] = list(df.columns)
        for col in df.columns:
            self.tree.heading(col, text=col)
            self.tree.column(col, width=max(100, min(300, len(str(col))*10)), anchor="w")
        to_show = df if len(df) <= MAX_ROWS_DISPLAY else df.head(MAX_ROWS_DISPLAY)
        for _, row in to_show.iterrows():
            vals = [("" if pd.isna(x) else str(x)) for x in row.tolist()]
            self.tree.insert("", "end", values=vals)
        if len(df) > MAX_ROWS_DISPLAY:
            self.status_var.set(f"Mostrate {MAX_ROWS_DISPLAY}/{len(df)} righe (limite UI)")

    def on_row_select(self):
        sel = self.tree.selection()
        if not sel:
            return
        vals = self.tree.item(sel[0])["values"]
        cols = self.tree["columns"]
        details = "\n".join([f"{c}: {vals[i] if i < len(vals) else ''}" for i, c in enumerate(cols)])
        self.detail_text.config(state="normal")
        self.detail_text.delete("1.0", tk.END)
        self.detail_text.insert("1.0", details)
        self.detail_text.config(state="disabled")

    def on_row_double_click(self):
        sel = self.tree.selection()
        if not sel:
            return
        if self.current_table is None:
            messagebox.showinfo("Operazione non valida", "L'UPDATE è disponibile solo quando hai caricato una tabella (non risultati SQL arbitrari).")
            return
        vals = self.tree.item(sel[0])["values"]
        cols = list(self.current_df.columns)
        # detect pk
        schema = self.dbm.table_schema(self.current_table)
        pk_cols = [c[1] for c in schema if c[5] == 1]
        if not pk_cols:
            messagebox.showwarning("No PK", "La tabella non ha PK definita; UPDATE non disponibile in modo sicuro.")
            return
        pk_col = pk_cols[0]
        if pk_col not in cols:
            messagebox.showwarning("PK non visibile", "La PK non è presente nella vista; ricarica la tabella includendo la PK.")
            return
        # find pk value based on column positions
        pk_index = cols.index(pk_col)
        pk_val = vals[pk_index] if pk_index < len(vals) else None
        if pk_val is None:
            messagebox.showwarning("PK mancante", "Valore PK non trovato nella riga selezionata.")
            return
        # find row in dataframe
        matched = self.current_df[self.current_df[pk_col].astype(str) == str(pk_val)]
        if matched.empty:
            messagebox.showerror("Riga non trovata", "Non ho trovato la riga nella tabella (possibile mismatch).")
            return
        row_data = matched.iloc[0].to_dict()
        UpdateDialog(self, self.dbm, self.current_table, pk_col, pk_val, row_data)

    def insert_row(self):
        if not self.current_table:
            messagebox.showinfo("Seleziona tabella", "Seleziona una tabella per inserire.")
            return
        InsertDialog(self, self.dbm, self.current_table)

    def update_row(self):
        sel = self.tree.selection()
        if not sel:
            messagebox.showinfo("Seleziona riga", "Seleziona prima una riga.")
            return
        self.on_row_double_click()

    def export_csv(self):
        if self.current_df is None or self.current_df.empty:
            messagebox.showinfo("Nessun dato", "La vista corrente è vuota.")
            return
        path = filedialog.asksaveasfilename(defaultextension=".csv", filetypes=[("CSV files","*.csv"),("All","*.*")])
        if not path:
            return
        try:
            self.current_df.to_csv(path, index=False)
            messagebox.showinfo("Esportato", f"CSV salvato in {path}")
            log_action(f"Export CSV -> {path}")
        except Exception as e:
            messagebox.showerror("Errore export", str(e))
            log_action(f"ERR export: {traceback.format_exc()}")

    def open_sql_editor(self):
        dlg = tk.Toplevel(self)
        dlg.title("Editor SQL")
        dlg.geometry("800x500")
        txt = tk.Text(dlg, wrap="none")
        txt.pack(fill="both", expand=True)
        btn_frame = ttk.Frame(dlg)
        btn_frame.pack(fill="x")
        def run_sql():
            sql = txt.get("1.0", tk.END).strip()
            if not sql:
                messagebox.showinfo("Vuoto", "Nessuna query presente.")
                return
            try:
                df = self.dbm.fetch_df(sql)
                self.current_table = None
                self.current_df = df
                self.populate_tree(df)
                dlg.destroy()
                log_action(f"Editor SQL eseguito -> {len(df)} righe")
            except Exception as e:
                messagebox.showerror("Errore SQL", str(e))
                log_action(f"ERR SQL: {traceback.format_exc()}")
        ttk.Button(btn_frame, text="Esegui SELECT", command=run_sql).pack(side="left", padx=6, pady=6)
        def open_file():
            path = filedialog.askopenfilename(title="Apri .sql", filetypes=[("SQL files","*.sql"),("All","*.*")])
            if not path:
                return
            try:
                with open(path,"r",encoding="utf-8") as f:
                    txt.delete("1.0", tk.END)
                    txt.insert("1.0", f.read())
            except Exception as e:
                messagebox.showerror("Errore", str(e))
        ttk.Button(btn_frame, text="Apri file .sql", command=open_file).pack(side="left", padx=6, pady=6)

    def run_create_script(self):
        path = filedialog.askopenfilename(title="Seleziona createTables.py", filetypes=[("Python files","*.py"),("All","*.*")])
        if not path:
            return
        try:
            with open(path, "r", encoding="utf-8") as f:
                code = f.read()
            exec_globals = {"__name__": "__main__"}
            exec(code, exec_globals)
            messagebox.showinfo("Eseguito", f"{path} eseguito.")
            log_action(f"Eseguito create script {path}")
            self.refresh_tables()
        except Exception as e:
            messagebox.showerror("Errore esecuzione", str(e))
            log_action(f"ERR run_create_script: {traceback.format_exc()}")

if __name__ == "__main__":
    app = CleanApp()
    app.mainloop()