import ttkbootstrap as ttk
from ttkbootstrap.constants import *
from tkinter import filedialog, messagebox, scrolledtext
import pandas as pd
import psycopg2
import os
import glob
import re
import threading
import warnings # Pandas/Geopandas uyarıları için (gerçi bu scriptte geopandas yok)

# --- Yardımcı Fonksiyon: Tablo/Sütun Adlarını Güvenli Hale Getirme ---
def sanitize_db_identifier(name, is_schema=False):
    """
    PostgreSQL tablo/sütun/şema adları için bir ismi güvenli hale getirir.
    """
    name = str(name).strip()
    if not is_schema:
        name = name.lower()
    
    name = re.sub(r'[\s\-.\(\)]+', '_', name)
    name = re.sub(r'[^\w_]', '', name) 
    name = re.sub(r'__+', '_', name) 
    
    if not name: 
        name = "unnamed_object" if not is_schema else "public"
    
    if name and name[0].isdigit() and not is_schema: 
        name = '_' + name
        
    name = name.strip('_')
    return name[:63]

# --- Excel İşleme Fonksiyonu (Çoklu Sayfa Destekli) ---
def excel_multi_sheet_to_postgres(db_config, schema_name, folder_path, status_callback):
    status_callback(f"Excel Aktarımı Başlatılıyor: {db_config.get('host')}/{db_config.get('dbname')}")
    conn = None
    cur = None
    overall_success = True
    processed_files_count = 0
    try:
        conn = psycopg2.connect(**db_config)
        cur = conn.cursor()
        status_callback("Veritabanı bağlantısı başarılı (Excel).")

        safe_schema_name = sanitize_db_identifier(schema_name, is_schema=True)
        if schema_name != safe_schema_name:
            status_callback(f"Bilgi: Şema adı '{schema_name}' -> '{safe_schema_name}' olarak düzenlendi.")
        status_callback(f"Şema '{safe_schema_name}' kontrol ediliyor/oluşturuluyor...")
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS \"{safe_schema_name}\";")
        conn.commit()

        xlsx_files = glob.glob(os.path.join(folder_path, '*.xlsx'))
        if not xlsx_files:
            status_callback("Bilgi: Seçilen klasörde XLSX dosyası bulunamadı.")
            return

        total_files = len(xlsx_files)
        status_callback(f"Toplam {total_files} XLSX dosyası bulundu.")

        for i, xlsx_path in enumerate(xlsx_files):
            excel_file_base = os.path.splitext(os.path.basename(xlsx_path))[0]
            status_callback(f"İşleniyor ({i+1}/{total_files}): {excel_file_base}.xlsx")
            file_processed_at_least_one_sheet = False
            try:
                xls = pd.ExcelFile(xlsx_path)
                sheet_names = xls.sheet_names
                if not sheet_names:
                    status_callback(f"  UYARI: {excel_file_base}.xlsx içinde sayfa bulunamadı, atlanıyor.")
                    continue
            except Exception as e:
                status_callback(f"  HATA: {excel_file_base}.xlsx okunamadı/açılamadı: {e}")
                overall_success = False
                continue

            for sheet_name in sheet_names:
                safe_excel_base = sanitize_db_identifier(excel_file_base)
                safe_sheet_name = sanitize_db_identifier(sheet_name)
                table_name = f"{safe_excel_base}_{safe_sheet_name}"
                table_name = sanitize_db_identifier(table_name) 
                qualified_table_name = f'"{safe_schema_name}"."{table_name}"'
                status_callback(f"  Sayfa: '{sheet_name}' -> Tablo: {qualified_table_name}")

                try:
                    df = pd.read_excel(xls, sheet_name=sheet_name)
                    if df.empty:
                        status_callback(f"    UYARI: '{sheet_name}' sayfası boş, atlanıyor.")
                        continue

                    original_cols = df.columns.tolist()
                    safe_cols = []
                    seen_cols = set()
                    for idx, col in enumerate(original_cols):
                        s_col = sanitize_db_identifier(col if pd.notna(col) else f"column_{idx+1}")
                        if not s_col: s_col = f"unnamed_col_{idx+1}"
                        temp_s_col = s_col
                        counter = 1
                        while temp_s_col in seen_cols:
                            temp_s_col = f"{s_col}_{counter}"
                            counter += 1
                        s_col = temp_s_col
                        safe_cols.append(s_col)
                        seen_cols.add(s_col)
                    df.columns = safe_cols
                    quoted_safe_cols = [f'"{col}"' for col in safe_cols]

                    cur.execute(f"DROP TABLE IF EXISTS {qualified_table_name} CASCADE;") # CASCADE eklendi
                    create_sql = f"CREATE TABLE {qualified_table_name} ({', '.join([f'{c} TEXT' for c in quoted_safe_cols])});"
                    cur.execute(create_sql)

                    temp_csv_filename = sanitize_db_identifier(f"temp_excel_page_{table_name}") + ".csv"
                    temp_csv_path = os.path.join(folder_path, temp_csv_filename)
                    df.to_csv(temp_csv_path, index=False, header=False, encoding='utf-8', quoting=1)
                    with open(temp_csv_path, 'r', encoding='utf-8') as f:
                        copy_sql = f"COPY {qualified_table_name} ({', '.join(quoted_safe_cols)}) FROM STDIN WITH (FORMAT CSV, HEADER FALSE, ENCODING 'UTF8')"
                        cur.copy_expert(sql=copy_sql, file=f)
                    conn.commit()
                    if os.path.exists(temp_csv_path): os.remove(temp_csv_path)
                    status_callback(f"    '{qualified_table_name}' başarıyla aktarıldı.")
                    file_processed_at_least_one_sheet = True
                except Exception as e:
                    conn.rollback()
                    status_callback(f"    HATA ({qualified_table_name}): {e}")
                    overall_success = False
            if file_processed_at_least_one_sheet:
                processed_files_count +=1
        
        if processed_files_count == total_files and total_files > 0:
             status_callback(f"Tüm {total_files} Excel dosyası ve sayfaları başarıyla işlendi.")
        elif processed_files_count > 0:
             status_callback(f"{processed_files_count}/{total_files} Excel dosyası kısmen veya tamamen işlendi. Detaylar için logları kontrol edin.")
        elif total_files > 0:
             status_callback(f"Hiçbir Excel dosyası başarıyla işlenemedi. Detaylar için logları kontrol edin.")


    except Exception as e:
        status_callback(f"Excel Aktarımında Genel HATA: {e}")
        overall_success = False
    finally:
        if cur: cur.close()
        if conn: conn.close()
        status_callback("Veritabanı bağlantısı kapatıldı (Excel).")
    return overall_success

# --- CSV İşleme Fonksiyonu ---
def csv_files_to_postgres(db_config, schema_name, folder_path, status_callback):
    status_callback(f"CSV Aktarımı Başlatılıyor: {db_config.get('host')}/{db_config.get('dbname')}")
    conn = None
    cur = None
    overall_success = True
    processed_files_count = 0
    try:
        conn = psycopg2.connect(**db_config)
        cur = conn.cursor()
        status_callback("Veritabanı bağlantısı başarılı (CSV).")

        safe_schema_name = sanitize_db_identifier(schema_name, is_schema=True)
        if schema_name != safe_schema_name:
            status_callback(f"Bilgi: Şema adı '{schema_name}' -> '{safe_schema_name}' olarak düzenlendi.")
        status_callback(f"Şema '{safe_schema_name}' kontrol ediliyor/oluşturuluyor...")
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS \"{safe_schema_name}\";")
        conn.commit()

        csv_files = glob.glob(os.path.join(folder_path, '*.csv'))
        if not csv_files:
            status_callback("Bilgi: Seçilen klasörde CSV dosyası bulunamadı.")
            return
            
        total_files = len(csv_files)
        status_callback(f"Toplam {total_files} CSV dosyası bulundu.")

        for i, csv_path in enumerate(csv_files):
            csv_file_base = os.path.splitext(os.path.basename(csv_path))[0]
            table_name = sanitize_db_identifier(csv_file_base)
            qualified_table_name = f'"{safe_schema_name}"."{table_name}"'
            status_callback(f"İşleniyor ({i+1}/{total_files}): {csv_file_base}.csv -> Tablo: {qualified_table_name}")
            try:
                # CSV okurken olası encoding sorunları için bir deneme
                try:
                    df = pd.read_csv(csv_path, encoding='utf-8')
                except UnicodeDecodeError:
                    status_callback(f"  UYARI: {csv_file_base}.csv UTF-8 ile okunamadı, 'latin1' ile deneniyor.")
                    df = pd.read_csv(csv_path, encoding='latin1')
                
                if df.empty:
                    status_callback(f"  UYARI: {csv_file_base}.csv dosyası boş, atlanıyor.")
                    continue

                original_cols = df.columns.tolist()
                safe_cols = []
                seen_cols = set()
                for idx, col in enumerate(original_cols):
                    s_col = sanitize_db_identifier(col if pd.notna(col) else f"column_{idx+1}")
                    if not s_col: s_col = f"unnamed_col_{idx+1}"
                    temp_s_col = s_col
                    counter = 1
                    while temp_s_col in seen_cols:
                        temp_s_col = f"{s_col}_{counter}"
                        counter += 1
                    s_col = temp_s_col
                    safe_cols.append(s_col)
                    seen_cols.add(s_col)
                df.columns = safe_cols
                quoted_safe_cols = [f'"{col}"' for col in safe_cols]

                cur.execute(f"DROP TABLE IF EXISTS {qualified_table_name} CASCADE;") # CASCADE eklendi
                create_sql = f"CREATE TABLE {qualified_table_name} ({', '.join([f'{c} TEXT' for c in quoted_safe_cols])});"
                cur.execute(create_sql)

                temp_csv_filename = sanitize_db_identifier(f"temp_csv_direct_{table_name}") + ".csv"
                temp_csv_for_copy = os.path.join(folder_path, temp_csv_filename)
                df.to_csv(temp_csv_for_copy, index=False, header=False, encoding='utf-8', quoting=1)
                with open(temp_csv_for_copy, 'r', encoding='utf-8') as f:
                    copy_sql = f"COPY {qualified_table_name} ({', '.join(quoted_safe_cols)}) FROM STDIN WITH (FORMAT CSV, HEADER FALSE, ENCODING 'UTF8')"
                    cur.copy_expert(sql=copy_sql, file=f)
                conn.commit()
                if os.path.exists(temp_csv_for_copy): os.remove(temp_csv_for_copy)
                status_callback(f"  '{qualified_table_name}' başarıyla aktarıldı.")
                processed_files_count +=1
            except Exception as e:
                conn.rollback()
                status_callback(f"  HATA ({qualified_table_name}): {e}")
                overall_success = False
        
        if processed_files_count == total_files and total_files > 0:
             status_callback(f"Tüm {total_files} CSV dosyası başarıyla işlendi.")
        elif processed_files_count > 0:
             status_callback(f"{processed_files_count}/{total_files} CSV dosyası kısmen veya tamamen işlendi. Detaylar için logları kontrol edin.")
        elif total_files > 0:
             status_callback(f"Hiçbir CSV dosyası başarıyla işlenemedi. Detaylar için logları kontrol edin.")

    except Exception as e:
        status_callback(f"CSV Aktarımında Genel HATA: {e}")
        overall_success = False
    finally:
        if cur: cur.close()
        if conn: conn.close()
        status_callback("Veritabanı bağlantısı kapatıldı (CSV).")
    return overall_success

# --- Ana GUI Sınıfı ---
class DataImporterApp:
    def __init__(self, root_window):
        self.root = root_window
        self.root.title("Veri Aktarım Aracı (Excel/CSV'den PostgreSQL'e)")
        self.root.geometry("750x750") # Test butonu için biraz daha yükseklik

        # --- Değişkenler ---
        self.source_type_var = ttk.StringVar(value="Excel") 
        self.host_var = ttk.StringVar(value='localhost')
        self.port_var = ttk.StringVar(value='5432')
        self.dbname_var = ttk.StringVar(value='postgres')
        self.user_var = ttk.StringVar(value='postgres')
        self.password_var = ttk.StringVar(value='postgres')
        self.schema_var = ttk.StringVar(value='public')
        self.folder_path_display_var = ttk.StringVar(value="Lütfen dosyaların bulunduğu klasörü seçin.")
        self.selected_folder_internal = ""

        # --- Çerçeveler ---
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.pack(fill=BOTH, expand=YES)

        source_type_frame = ttk.LabelFrame(main_frame, text="Veri Kaynağı Türü", padding="10", bootstyle=INFO)
        source_type_frame.pack(fill=X, pady=5)

        db_frame = ttk.LabelFrame(main_frame, text="PostgreSQL Bağlantı Bilgileri", padding="10", bootstyle=INFO)
        db_frame.pack(fill=X, pady=5)

        folder_frame = ttk.LabelFrame(main_frame, text="Dosya Kaynağı Klasörü", padding="10", bootstyle=INFO)
        folder_frame.pack(fill=X, pady=5)

        action_frame = ttk.Frame(main_frame, padding="0 10 0 0")
        action_frame.pack(fill=X)

        log_frame = ttk.LabelFrame(main_frame, text="İşlem Günlüğü", padding="10", bootstyle=INFO)
        log_frame.pack(fill=BOTH, expand=YES, pady=(10,0))

        # --- Veri Kaynağı Türü Seçimi ---
        ttk.Radiobutton(source_type_frame, text="Excel (.xlsx)", variable=self.source_type_var, value="Excel", command=self.update_folder_label, bootstyle=TOOLBUTTON).pack(side=LEFT, padx=10, pady=5)
        ttk.Radiobutton(source_type_frame, text="CSV (.csv)", variable=self.source_type_var, value="CSV", command=self.update_folder_label, bootstyle=TOOLBUTTON).pack(side=LEFT, padx=10, pady=5)

        # --- PostgreSQL Bağlantı Bilgileri ---
        labels_db = ["Host:", "Port:", "Veritabanı Adı:", "Kullanıcı Adı:", "Şifre:", "Şema Adı:"]
        variables_db = [self.host_var, self.port_var, self.dbname_var, self.user_var, self.password_var, self.schema_var]
        
        for i, label_text in enumerate(labels_db):
            ttk.Label(db_frame, text=label_text).grid(row=i, column=0, sticky=W, padx=5, pady=3)
            entry_widget = ttk.Entry(db_frame, textvariable=variables_db[i], width=55)
            if label_text == "Şifre:":
                entry_widget.config(show="*")
            entry_widget.grid(row=i, column=1, sticky=EW, padx=5, pady=3)
        db_frame.grid_columnconfigure(1, weight=1)

        self.test_conn_button = ttk.Button(db_frame, text="Bağlantıyı Test Et", command=self.test_db_connection_thread, bootstyle=OUTLINE + INFO)
        self.test_conn_button.grid(row=len(labels_db), column=0, columnspan=2, pady=(10,5), sticky=EW)


        # --- Klasör Seçimi ---
        self.folder_display_label = ttk.Label(folder_frame, textvariable=self.folder_path_display_var, wraplength=600, justify=LEFT)
        self.folder_display_label.pack(side=LEFT, fill=X, expand=YES, padx=(0,10))
        self.select_folder_button = ttk.Button(folder_frame, text="Klasör Seç", command=self.select_folder, width=15, bootstyle=INFO)
        self.select_folder_button.pack(side=RIGHT)
        self.update_folder_label() 

        # --- Aktarım Butonu ---
        self.transfer_button = ttk.Button(action_frame, text="Veritabanına Aktar", command=self.start_transfer_thread, bootstyle=SUCCESS)
        self.transfer_button.pack(fill=X, ipady=8, pady=(10,0))

        # --- Log Alanı ---
        self.status_text = scrolledtext.ScrolledText(log_frame, height=15, width=80, wrap=WORD, relief="sunken", borderwidth=1, state=DISABLED, font=('Consolas', 9),
                                                     bg="#292929", fg="#cccccc", insertbackground="#ffffff") # Koyu tema renkleri
        self.status_text.pack(fill=BOTH, expand=YES)

    def update_folder_label(self):
        source_type = self.source_type_var.get()
        if not self.selected_folder_internal:
            self.folder_path_display_var.set(f"Lütfen {source_type} dosyalarının bulunduğu klasörü seçin.")
        else:
            # Kullanıcı kaynak türünü değiştirdiğinde, seçili klasör etiketini de güncelle
            self.folder_path_display_var.set(f"Seçilen {source_type} Klasörü: {self.selected_folder_internal}")


    def select_folder(self):
        folder_selected = filedialog.askdirectory()
        if folder_selected:
            self.selected_folder_internal = folder_selected
            self.update_folder_label() # Etiketi güncellemek için bu fonksiyonu çağır
            self.log_status(f"{self.source_type_var.get()} klasörü seçildi: {self.selected_folder_internal}")
        else:
            # Eğer bir klasör zaten seçiliyse ve kullanıcı iptal ederse, eski seçimi koru
            if not self.selected_folder_internal:
                self.update_folder_label()
            self.log_status("Klasör seçme işlemi iptal edildi.")

    def log_status(self, message):
        self.status_text.configure(state=NORMAL)
        self.status_text.insert(END, message + "\n")
        self.status_text.see(END)
        self.status_text.configure(state=DISABLED)
        self.root.update_idletasks()
    
    def log_status_thread_safe(self, message):
        self.root.after(0, self.log_status, message)

    def _get_db_config_and_validate(self):
        """Helper to get and validate DB config."""
        db_config = {
            'host': self.host_var.get().strip(),
            'port': self.port_var.get().strip(),
            'dbname': self.dbname_var.get().strip(),
            'user': self.user_var.get().strip(),
            'password': self.password_var.get()
        }
        required_fields_labels = {"Host": db_config['host'], "Port": db_config['port'], 
                                  "Veritabanı Adı": db_config['dbname'], "Kullanıcı Adı": db_config['user']}
        for label, value in required_fields_labels.items():
            if not value:
                messagebox.showerror("Eksik Bilgi", f"Lütfen '{label}' alanını doldurun.", parent=self.root)
                return None
        try:
            db_config['port'] = int(db_config['port'])
        except ValueError:
            messagebox.showerror("Hata", "Port numarası geçerli bir sayı olmalıdır.", parent=self.root)
            return None
        return db_config

    def _set_buttons_state(self, state):
        self.transfer_button.config(state=state)
        self.test_conn_button.config(state=state)


    def test_db_connection_thread(self):
        db_config = self._get_db_config_and_validate()
        if not db_config:
            return

        self._set_buttons_state(DISABLED)
        self.log_status_thread_safe(f"Bağlantı test ediliyor: {db_config['host']}:{db_config['port']}/{db_config['dbname']}...")
        
        thread = threading.Thread(target=self._execute_test_connection, args=(db_config,), daemon=True)
        thread.start()
        self.root.after(100, self._check_thread_completion, thread, "Bağlantı testi")

    def _execute_test_connection(self, db_config):
        conn = None
        try:
            with warnings.catch_warnings(): # Pandas uyarılarını bastır
                warnings.simplefilter("ignore", UserWarning)
                conn = psycopg2.connect(**db_config, connect_timeout=5)
            self.log_status_thread_safe("Bağlantı testi BAŞARILI!")
        except psycopg2.Error as e:
            self.log_status_thread_safe(f"Bağlantı testi BAŞARISIZ: {e}")
            messagebox.showerror("Bağlantı Testi Başarısız", f"Bağlantı kurulamadı:\n{e}", parent=self.root)
        except Exception as e:
            self.log_status_thread_safe(f"Bağlantı testi sırasında beklenmedik HATA: {e}")
            messagebox.showerror("Bağlantı Testi Hatası", f"Beklenmedik bir hata oluştu:\n{e}", parent=self.root)
        finally:
            if conn: conn.close()
            # Butonlar _check_thread_completion tarafından aktif edilecek


    def start_transfer_thread(self):
        db_config = self._get_db_config_and_validate()
        if not db_config:
            return

        schema_name = self.schema_var.get().strip()
        if not schema_name:
            messagebox.showerror("Eksik Bilgi", "Lütfen Şema Adı alanını doldurun.", parent=self.root)
            return
        
        if not db_config['password']:
            if not messagebox.askyesno("Şifre Eksik", "PostgreSQL şifresi girmediniz. Devam etmek istiyor musunuz?", parent=self.root):
                return
        
        folder_path = self.selected_folder_internal
        source_type = self.source_type_var.get()
        if not folder_path or not os.path.isdir(folder_path):
            messagebox.showerror("Eksik Bilgi", f"Lütfen geçerli bir {source_type} dosyalarının bulunduğu klasörü seçin.", parent=self.root)
            return

        self._set_buttons_state(DISABLED)
        self.status_text.configure(state=NORMAL)
        self.status_text.delete('1.0', END)
        self.status_text.configure(state=DISABLED)
        self.log_status_thread_safe(f"{source_type} aktarım işlemi başlatılıyor...")

        target_function = None
        if source_type == "Excel":
            target_function = excel_multi_sheet_to_postgres
        elif source_type == "CSV":
            target_function = csv_files_to_postgres
        
        if target_function:
            transfer_thread = threading.Thread(
                target=target_function,
                args=(db_config, schema_name, folder_path, self.log_status_thread_safe),
                daemon=True
            )
            transfer_thread.start()
            self.root.after(100, self._check_thread_completion, transfer_thread, f"{source_type} aktarımı")
        else:
            messagebox.showerror("Hata", "Geçersiz veri kaynağı türü seçildi.", parent=self.root)
            self._set_buttons_state(NORMAL)


    def _check_thread_completion(self, thread, operation_name="İşlem"):
        if thread.is_alive():
            self.root.after(100, self._check_thread_completion, thread, operation_name)
        else:
            self._set_buttons_state(NORMAL)
            self.log_status_thread_safe(f"{operation_name} tamamlandı veya durdu.")


if __name__ == "__main__":
    main_root = ttk.Window(themename="darkly") # Koyu tema: darkly, superhero, cyborg
    app = DataImporterApp(main_root)
    main_root.mainloop()
