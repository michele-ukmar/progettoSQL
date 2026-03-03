import tkinter as tk
from tkinter import ttk, messagebox
import sqlite3
import pandas as pd
import os

DEFAULT_DB = "movies.db"
QUERY_FILE = "analyticalQuery.sql"
MAX_ROWS_DISPLAY = 5000

class DBManager:
    def __init__(self, db_path=DEFAULT_DB):
        self.db_path = db_path
        self.connect()

    def connect(self):
        try:
            self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
            self.conn.row_factory = sqlite3.Row
        except Exception as e:
            messagebox.showerror("Errore", f"Connessione fallita: {e}")

    def list_tables(self):
        cur = self.conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name;")
        return [r[0] for r in cur.fetchall()]

    def table_schema(self, table):
        return self.conn.execute(f"PRAGMA table_info('{table}')").fetchall()

    def fetch_df(self, sql, params=()):
        return pd.read_sql_query(sql, self.conn, params=params)

    def safe_insert(self, table, data):
        cols, vals = list(data.keys()), tuple(data.values())
        sql = f'INSERT INTO "{table}" ({",".join([f'"{c}"' for c in cols])}) VALUES ({",".join(["?"]*len(cols))})'
        with self.conn: self.conn.execute(sql, vals)

    def safe_update(self, table, update_data, pk_col, pk_val):
        set_parts = ", ".join([f'"{k}" = ?' for k in update_data.keys()])
        vals = tuple(update_data.values()) + (pk_val,)
        sql = f'UPDATE "{table}" SET {set_parts} WHERE "{pk_col}" = ?'
        with self.conn: self.conn.execute(sql, vals)

class RecordDialog(tk.Toplevel):
    def __init__(self, parent, dbm, table, mode="INSERT", pk_info=None, current_values=None):
        super().__init__(parent)
        self.parent, self.dbm, self.table, self.mode = parent, dbm, table, mode
        self.pk_col, self.pk_val = pk_info if pk_info else (None, None)
        self.title(f"{mode} - {table}")
        self.geometry("400x500")
        self.grab_set()
        
        container = ttk.Frame(self, padding=20)
        container.pack(fill="both", expand=True)
        self.entries = {}
        schema = self.dbm.table_schema(table)
        
        for i, col in enumerate(schema):
            name, ctype, pk = col[1], col[2], col[5]
            if mode == "INSERT" and pk == 1 and "INT" in (ctype or "").upper(): continue
            
            ttk.Label(container, text=f"{name}:").grid(row=i, column=0, sticky="w", pady=2)
            ent = ttk.Entry(container, width=30)
            if mode == "UPDATE" and current_values:
                val = current_values.get(name, "")
                ent.insert(0, str(val) if val is not None else "")
                if pk == 1: ent.config(state="disabled")
            ent.grid(row=i, column=1, sticky="ew", pady=2)
            self.entries[name] = ent

        ttk.Button(container, text="Salva", command=self.save).grid(row=100, column=0, columnspan=2, pady=20)

    def save(self):
        data = {k: (v.get().strip() or None) for k, v in self.entries.items() if v.cget("state") != "disabled"}
        try:
            if self.mode == "INSERT": self.dbm.safe_insert(self.table, data)
            else: self.dbm.safe_update(self.table, data, self.pk_col, self.pk_val)
            self.parent.load_current_table()
            self.destroy()
        except Exception as e: messagebox.showerror("Errore", str(e))

class ProApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("DB Management Tool")
        self.geometry("1100x650")
        self.dbm = DBManager()
        self.current_table = None
        self.current_df = pd.DataFrame()
        self.setup_ui()
        self.refresh_tables()

    def setup_ui(self):
        self.paned = ttk.PanedWindow(self, orient="horizontal")
        self.paned.pack(fill="both", expand=True)
        
        # Sidebar
        self.sidebar = ttk.Frame(self.paned, width=200, padding=10)
        self.paned.add(self.sidebar)
        ttk.Label(self.sidebar, text="Tabelle", font=('Arial', 10, 'bold')).pack(pady=(0,5))
        self.tbl_list = tk.Listbox(self.sidebar, font=('Arial', 9))
        self.tbl_list.pack(fill="both", expand=True)
        self.tbl_list.bind("<<ListboxSelect>>", self.on_table_select)
        
        # Main Content
        self.content = ttk.Frame(self.paned, padding=10)
        self.paned.add(self.content)
        
        # Filtro
        self.f_frame = ttk.LabelFrame(self.content, text="Filtro WHERE", padding=5)
        self.f_frame.pack(fill="x")
        self.filter_var = tk.StringVar()
        ent = ttk.Entry(self.f_frame, textvariable=self.filter_var)
        ent.pack(side="left", fill="x", expand=True, padx=5)
        ent.bind("<Return>", lambda e: self.load_current_table())
        ttk.Button(self.f_frame, text="Applica", command=self.load_current_table).pack(side="left")

        # Azioni
        self.act = ttk.Frame(self.content)
        self.act.pack(fill="x", pady=5)
        self.btn_add = ttk.Button(self.act, text="Aggiungi", command=self.add_record)
        self.btn_add.pack(side="left", padx=2)
        self.btn_edit = ttk.Button(self.act, text="Modifica", command=self.edit_record)
        self.btn_edit.pack(side="left", padx=2)
        ttk.Button(self.act, text="Analisi (SQL)", command=self.run_analytic_query).pack(side="left", padx=2)
        self.btn_reset = ttk.Button(self.act, text="Ripristina Vista", command=self.reset_view)

        # Tabella
        self.tree = ttk.Treeview(self.content, show="headings", selectmode="browse")
        self.tree.pack(fill="both", expand=True)
        self.tree.bind("<Double-1>", lambda e: self.edit_record())

    def refresh_tables(self):
        self.tbl_list.delete(0, tk.END)
        for t in self.dbm.list_tables(): self.tbl_list.insert(tk.END, t)

    def on_table_select(self, e):
        if self.tbl_list.curselection() and self.tbl_list.cget("state") == "normal":
            # Reset filtro prima di cambiare tabella per evitare errori di colonne inesistenti
            self.filter_var.set("") 
            self.current_table = self.tbl_list.get(self.tbl_list.curselection())
            self.load_current_table()

    def load_current_table(self):
        if not self.current_table: return
        sql = f'SELECT * FROM "{self.current_table}"'
        if self.filter_var.get().strip():
            sql += f" WHERE {self.filter_var.get()}"
        self._execute_and_display(sql)

    def _execute_and_display(self, sql):
        try:
            df = self.dbm.fetch_df(sql)
            self.tree.delete(*self.tree.get_children())
            self.tree["columns"] = list(df.columns)
            for c in df.columns: 
                self.tree.heading(c, text=c)
                self.tree.column(c, width=120)
            for _, r in df.head(MAX_ROWS_DISPLAY).iterrows(): 
                self.tree.insert("", "end", values=[("" if pd.isna(x) else x) for x in r])
            self.current_df = df
        except Exception as e: 
            messagebox.showerror("Errore SQL", f"Errore: {e}\nIl filtro verrà resettato.")
            self.filter_var.set("")

    def add_record(self):
        if self.current_table: RecordDialog(self, self.dbm, self.current_table, "INSERT")

    def edit_record(self):
        if not self.tree.selection() or self.current_table is None: return
        schema = self.dbm.table_schema(self.current_table)
        pk_cols = [c[1] for c in schema if c[5] == 1]
        if not pk_cols: return messagebox.showwarning("Info", "PK mancante per la modifica")
        
        vals = self.tree.item(self.tree.selection()[0])['values']
        curr = dict(zip(list(self.current_df.columns), vals))
        RecordDialog(self, self.dbm, self.current_table, "UPDATE", (pk_cols[0], curr.get(pk_cols[0])), curr)

    def run_analytic_query(self):
        if not os.path.exists(QUERY_FILE):
            return messagebox.showerror("Errore", f"File '{QUERY_FILE}' non trovato nella cartella del progetto.")
        
        try:
            with open(QUERY_FILE, "r", encoding="utf-8") as f:
                sql = f.read()
            
            self._execute_and_display(sql)
            
            # Lock UI
            self.current_table = None 
            self.tbl_list.config(state="disabled")
            self.f_frame.pack_forget()
            self.btn_add.config(state="disabled")
            self.btn_edit.config(state="disabled")
            self.btn_reset.pack(side="left", padx=5)
            self.title("VISUALIZZAZIONE ANALITICA - Lista Tabelle Bloccata")
            
        except Exception as e:
            messagebox.showerror("Errore Analisi", str(e))

    def reset_view(self):
        self.tbl_list.config(state="normal")
        self.f_frame.pack(fill="x", before=self.act)
        self.btn_add.config(state="normal")
        self.btn_edit.config(state="normal")
        self.btn_reset.pack_forget()
        self.tree.delete(*self.tree.get_children())
        self.current_table = None
        self.filter_var.set("")
        self.title("DB Management Tool")

if __name__ == "__main__":
    ProApp().mainloop()