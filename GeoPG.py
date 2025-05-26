import ttkbootstrap as ttk
from ttkbootstrap.constants import *
from tkinter import filedialog, messagebox, scrolledtext, Toplevel
import psycopg2
import geopandas as gpd
from tkintermapview import TkinterMapView
from PIL import Image, ImageTk, ImageDraw
import threading
import warnings
import pandas as pd
from tkinter import Toplevel
from EXECSV2PG import DataImporterApp 

# --- KÜÇÜK DAİRE İKONU OLUŞTURMA ---
def create_small_circle_icon(size, color_tuple):
    """Belirtilen boyutta ve renkte (RGBA tuple) dairesel bir PIL Image nesnesi oluşturur."""
    image = Image.new('RGBA', (size, size), (0, 0, 0, 0))
    draw = ImageDraw.Draw(image)
    draw.ellipse((0, 0, size - 1, size - 1), fill=color_tuple, outline=color_tuple)
    return ImageTk.PhotoImage(image)

class PostGISApp:
    def __init__(self, root):
        self.root = root
        self.root.title("PostGIS Sorgu ve Harita Görüntüleyici")
        self.root.geometry("1200x800")
        
        # --- YENİ: Menü Çubuğu ve Pencere Takibi ---
        self.importer_window = None # Aktarım penceresini takip etmek için
        self.create_menubar()
        # --- YENİ BÖLÜM SONU ---

        self.db_params = None
        self.connection_window = None
        self.small_orange_icon = create_small_circle_icon(10, (255, 165, 0, 255))

        main_paned_window = ttk.PanedWindow(self.root, orient=HORIZONTAL)
        main_paned_window.pack(fill=BOTH, expand=YES, padx=10, pady=10)

        # --- 1. Sol Kenar Çubuğu (Sidebar) ---
        sidebar_frame = ttk.Frame(main_paned_window, padding=5)
        self.connect_button = ttk.Button(sidebar_frame, text="Veritabanına Bağlan", command=self.open_connection_dialog, bootstyle=INFO)
        self.connect_button.pack(side=TOP, fill=X, pady=(0, 10))
        tree_frame = ttk.LabelFrame(sidebar_frame, text="Veritabanı Nesneleri", padding=5)
        tree_frame.pack(side=TOP, fill=BOTH, expand=YES)
        self.db_tree = ttk.Treeview(tree_frame, bootstyle=INFO)
        self.db_tree.pack(fill=BOTH, expand=YES)
        self.db_tree.bind("<Double-1>", self.on_tree_double_click)
        self.db_tree.heading("#0", text="Şema / Tablo", anchor=W)
        main_paned_window.add(sidebar_frame, weight=1)

        # --- 2. Sağ İçerik Alanı ---
        right_content_frame = ttk.Frame(main_paned_window)
        query_frame = ttk.LabelFrame(right_content_frame, text="SQL Sorgusu (Coğrafi kolon 'geom' olmalı)", padding="10", bootstyle=INFO)
        query_frame.pack(pady=0, fill=X)
        self.query_text = scrolledtext.ScrolledText(query_frame, wrap=WORD, height=8, relief="sunken", borderwidth=1,
                                                    font=('Consolas', 10), bg="#292929", fg="#cccccc", insertbackground="#ffffff")
        self.query_text.pack(expand=YES, fill=BOTH)
        self.query_text.insert(INSERT, "SELECT * FROM public.your_spatial_table LIMIT 100;")
        self.run_button = ttk.Button(right_content_frame, text="Sorguyu Çalıştır ve Haritada Göster",
                                     command=self.run_query_and_map_thread, bootstyle=SUCCESS)
        self.run_button.pack(pady=10, fill=X, ipady=5)
        
# PostGISApp __init__ metodunuzda, bu bölümü bulun ve güncelleyin:

        # --- GÜNCELLENMİŞ BÖLÜM: Harita ve Tablo Sekmeleri ---
        # Ana çerçeve (Sekmeler ve Log için)
        display_frame = ttk.Frame(right_content_frame)
        display_frame.pack(fill=BOTH, expand=YES, pady=(5,0))
        
        # Sekmeli Defter (Notebook) Oluştur
        self.notebook = ttk.Notebook(display_frame)
        self.notebook.pack(side=TOP, fill=BOTH, expand=YES)

        # Sekme 1: Harita
        map_tab = ttk.Frame(self.notebook, padding=0) # Padding'i sıfırla
        self.notebook.add(map_tab, text="Harita")

        # --- YENİ: Harita Altlığı Seçim Menüsü ---
        basemap_frame = ttk.Frame(map_tab, padding=5)
        basemap_frame.pack(side=TOP, fill=X)

        ttk.Label(basemap_frame, text="Harita Altlığı:").pack(side=LEFT, padx=(0, 5))

        # Kullanılabilir harita sunucuları
        self.tile_servers = {
            "Google Uydu": "https://mt0.google.com/vt/lyrs=s&hl=tr&x={x}&y={y}&z={z}&s=Ga",
            "OpenStreetMap": "https://tile.openstreetmap.org/{z}/{x}/{y}.png",
            "Google Sokak": "https://mt0.google.com/vt/lyrs=m&hl=tr&x={x}&y={y}&z={z}&s=Ga",
            "Esri World Imagery": "https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
            "CartoDB Positron (Açık Tema)": "https://a.basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png"
        }
        
        self.basemap_var = ttk.StringVar(value="Google Uydu")
        self.basemap_combo = ttk.Combobox(basemap_frame, 
                                          textvariable=self.basemap_var, 
                                          values=list(self.tile_servers.keys()), 
                                          state="readonly",
                                          bootstyle=INFO)
        self.basemap_combo.pack(side=LEFT, fill=X, expand=YES)
        self.basemap_combo.bind("<<ComboboxSelected>>", self.on_basemap_changed)
        # --- YENİ BÖLÜM SONU ---
        
        self.map_widget = TkinterMapView(map_tab, corner_radius=0)
        self.map_widget.pack(fill=BOTH, expand=YES)
        
        # Başlangıç haritasını ayarla
        self.map_widget.set_tile_server(self.tile_servers["Google Uydu"], max_zoom=22)
        self.map_widget.set_position(39.925533, 32.866287) # Ankara
        self.map_widget.set_zoom(6)
        
        # ... __init__ metodunun geri kalanı aynı şekilde devam eder ...

        # Sekme 2: Tablo (Veri Izgarası)
        table_tab = ttk.Frame(self.notebook, padding=5)
        self.notebook.add(table_tab, text="Tablo")
        
        grid_frame = ttk.Frame(table_tab)
        grid_frame.pack(fill=BOTH, expand=YES)
        
        # Veri Izgarası için Treeview ve Kaydırma Çubukları
        self.data_grid = ttk.Treeview(grid_frame, bootstyle=PRIMARY, show="headings")
        
        vsb = ttk.Scrollbar(grid_frame, orient="vertical", command=self.data_grid.yview)
        hsb = ttk.Scrollbar(grid_frame, orient="horizontal", command=self.data_grid.xview)
        self.data_grid.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)

        vsb.pack(side=RIGHT, fill=Y)
        hsb.pack(side=BOTTOM, fill=X)
        self.data_grid.pack(side=LEFT, fill=BOTH, expand=YES)

        # İşlem Günlüğü (Log) Sekmelerin Altına Taşındı
        log_labelframe = ttk.LabelFrame(display_frame, text="İşlem Günlüğü", padding=5, bootstyle=SECONDARY)
        log_labelframe.pack(side=BOTTOM, fill=X, expand=NO, pady=(10,0))
        self.status_log = scrolledtext.ScrolledText(log_labelframe, wrap=WORD, height=5, relief="sunken", borderwidth=1,
                                                    font=('Consolas', 9), bg="#292929", fg="#cccccc", insertbackground="#ffffff",
                                                    state=DISABLED)
        self.status_log.pack(fill=BOTH, expand=YES)
        
        main_paned_window.add(right_content_frame, weight=4)
        
    # Bu iki yeni metodu PostGISApp sınıfınızın içine ekleyin.
# Bu yeni metodu PostGISApp sınıfınızın içine ekleyin
    # PostGISApp sınıfınıza bu iki yeni metodu ekleyin

        
    def on_basemap_changed(self, event=None):
        """
        Harita altlığı seçim menüsünden yeni bir seçim yapıldığında çalışır.
        """
        # Seçilen altlığın adını al
        selected_name = self.basemap_var.get()
        
        # Adı kullanarak URL'yi sözlükten bul
        new_url = self.tile_servers.get(selected_name)
        
        if new_url:
            # Harita widget'ının tile sunucusunu güncelle
            self.map_widget.set_tile_server(new_url)
            self._log_status(f"Harita altlığı değiştirildi: {selected_name}")
        else:
            self._log_status(f"HATA: '{selected_name}' için geçerli bir URL bulunamadı.")
    def create_menubar(self):
        """Uygulamanın ana menü çubuğunu oluşturur."""
        menubar = ttk.Menu(self.root)
    
        # Dosya Menüsü
        file_menu = ttk.Menu(menubar, tearoff=0)
        file_menu.add_command(
            label="Veri Aktar (Excel/CSV)...",
            command=self.open_importer_window
        )
        file_menu.add_separator()
        file_menu.add_command(label="Çıkış", command=self.root.quit)
    
        menubar.add_cascade(label="Dosya", menu=file_menu)
    
        self.root.config(menu=menubar)

# PostGISApp sınıfınızdaki bu metodu güncelleyin
    def open_importer_window(self):
        """Veri aktarım aracını yeni bir Toplevel penceresinde açar."""
        # Eğer pencere zaten açıksa, yenisini açma, mevcut olanı öne getir.
        if self.importer_window is not None and self.importer_window.winfo_exists():
            self.importer_window.focus()
            return
            
        # Yeni bir Toplevel penceresi oluştur
        self.importer_window = Toplevel(self.root)
        
        # EXECSV2PG.py'den gelen DataImporterApp sınıfını bu yeni pencere ile başlat
        importer_app = DataImporterApp(self.importer_window)
        
        # --- DEĞİŞİKLİK BURADA ---
        # Pencere kapatıldığında özel kapatma fonksiyonumuzu çağır
        self.importer_window.protocol("WM_DELETE_WINDOW", self._on_importer_close)
    
    
    # PostGISApp sınıfınıza bu YENİ METODU ekleyin
    def _on_importer_close(self):
        """Aktarım penceresi kapatıldığında işlemleri doğru sırada yapar."""
        if self.importer_window:
            # 1. Önce pencere nesnesini yok et
            self.importer_window.destroy()
            # 2. Sonra takip değişkenini None olarak ayarla
            self.importer_window = None

    # --- YENİ METOT: Veri Izgarasını Doldurmak İçin ---
    def _populate_data_grid(self, dataframe):
        """
        Veri çerçevesini (DataFrame) alarak Tablo sekmesindeki ızgarayı doldurur.
        Geometri sütununu göstermez.
        """
        # Önceki verileri ve sütunları temizle
        self.data_grid.delete(*self.data_grid.get_children())
        self.data_grid["columns"] = []

        if dataframe.empty:
            return
            
        # Geometri sütununu çıkar
        df_attributes = dataframe.drop(columns=[dataframe.geometry.name])
        
        # Sütunları ayarla
        columns = list(df_attributes.columns)
        self.data_grid["columns"] = columns
        self.data_grid["show"] = "headings" # Sütun başlıklarını göster

        for col in columns:
            self.data_grid.heading(col, text=col)
            self.data_grid.column(col, width=120, anchor=W) # Varsayılan sütun genişliği
            
        # Satırları ekle
        for index, row in df_attributes.iterrows():
            self.data_grid.insert("", END, values=list(row))
        
        self._log_status(f"Tabloya {len(df_attributes)} kayıt yüklendi.")


    def _log_status(self, message):
        def _update_log():
            self.status_log.config(state=NORMAL)
            self.status_log.insert(END, message + "\n")
            self.status_log.see(END)
            self.status_log.config(state=DISABLED)
        self.root.after(0, _update_log)

    def _set_buttons_state(self, state):
        self.connect_button.config(state=state)
        self.run_button.config(state=state)
    
    # ... (open_connection_dialog, connect_and_populate_thread, _execute_populate_tree, _populate_treeview, on_tree_double_click metotları değişmeden kalır) ...
    def open_connection_dialog(self):
        """Yeni bir pencerede bağlantı bilgilerini isteyen dialog oluşturur."""
        if self.connection_window and self.connection_window.winfo_exists():
             self.connection_window.focus()
             return

        self.connection_window = Toplevel(self.root)
        self.connection_window.title("Bağlantı Kur")
        self.connection_window.geometry("400x300")
        self.connection_window.transient(self.root) # Ana pencerenin üzerinde kalır
        self.connection_window.grab_set() # Odağı bu pencereye kilitler

        conn_frame = ttk.Frame(self.connection_window, padding="10")
        conn_frame.pack(fill=BOTH, expand=YES)

        labels_db = ['Host:', 'Port:', 'Veritabanı Adı:', 'Kullanıcı Adı:', 'Şifre:']
        defaults_db = ['localhost', '5432', 'postgres', 'postgres', '']
        self.dialog_db_entries = {}

        for i, label_text in enumerate(labels_db):
            ttk.Label(conn_frame, text=label_text).grid(row=i, column=0, sticky=W, padx=5, pady=4)
            entry = ttk.Entry(conn_frame, width=35)
            entry.insert(0, defaults_db[i])
            entry.grid(row=i, column=1, sticky=EW, padx=5, pady=4)
            if label_text == 'Şifre:':
                entry.config(show="*")
            key_name = label_text.lower().replace(":", "").replace(" ", "_")
            self.dialog_db_entries[key_name] = entry
        conn_frame.grid_columnconfigure(1, weight=1)

        connect_btn = ttk.Button(conn_frame, text="Bağlan ve Şemaları Listele", 
                                 command=self.connect_and_populate_thread, bootstyle=SUCCESS)
        connect_btn.grid(row=len(labels_db), column=0, columnspan=2, pady=(15,0), sticky=EW)

    def connect_and_populate_thread(self):
        """Bağlantıyı test eder ve başarılı olursa şema/tablo ağacını doldurur."""
        # Bağlantı bilgilerini dialog'dan al
        params = {}
        try:
            params['dbname'] = self.dialog_db_entries['veritabanı_adı'].get()
            params['user'] = self.dialog_db_entries['kullanıcı_adı'].get()
            params['password'] = self.dialog_db_entries['şifre'].get()
            params['host'] = self.dialog_db_entries['host'].get()
            params['port'] = self.dialog_db_entries['port'].get()
            if not all([params['host'], params['port'], params['dbname'], params['user']]):
                 messagebox.showerror("Eksik Bilgi", "Lütfen tüm gerekli alanları doldurun.", parent=self.connection_window)
                 return
        except KeyError:
             messagebox.showerror("Program Hatası", "Dialog penceresi elemanları bulunamadı.", parent=self.connection_window)
             return
        
        self.db_params = params # Parametreleri sakla
        
        self._set_buttons_state(DISABLED)
        self._log_status(f"Bağlanılıyor: {self.db_params['host']}:{self.db_params['port']}...")
        
        thread = threading.Thread(target=self._execute_populate_tree, daemon=True)
        thread.start()

    def _execute_populate_tree(self):
        """Veritabanından şema ve coğrafi tablo bilgilerini çeker."""
        conn = None
        try:
            conn = psycopg2.connect(**self.db_params, connect_timeout=5)
            self._log_status("Bağlantı başarılı. Coğrafi tablolar sorgulanıyor...")
            
            # PostGIS'in metadata tablosundan coğrafi tabloları çeken sorgu
            sql = """
            SELECT f_table_schema, f_table_name, type 
            FROM geometry_columns
            WHERE f_table_schema NOT IN ('pg_catalog', 'information_schema', 'topology')
            ORDER BY f_table_schema, f_table_name;
            """
            
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", UserWarning)
                spatial_tables_df = pd.read_sql(sql, conn)

            self._log_status(f"{len(spatial_tables_df)} coğrafi tablo bulundu.")
            
            # Ana thread'de Treeview'ı güncellemek için `after` kullan
            self.root.after(0, self._populate_treeview, spatial_tables_df)

        except psycopg2.Error as e:
            self._log_status(f"Bağlantı/Sorgu Hatası: {e}")
            messagebox.showerror("Bağlantı Başarısız", f"Veritabanına bağlanılamadı:\n{e}", parent=self.connection_window or self.root)
            self.db_params = None # Başarısız olursa parametreleri temizle
        except Exception as e:
            self._log_status(f"Beklenmedik bir hata oluştu: {e}")
            messagebox.showerror("Hata", f"Beklenmedik bir hata oluştu:\n{e}", parent=self.connection_window or self.root)
            self.db_params = None
        finally:
            if conn:
                conn.close()
            # İşlem bitince butonları tekrar aktif et
            self.root.after(0, self._set_buttons_state, NORMAL)


    def _populate_treeview(self, df):
        """Veri çerçevesindeki bilgileri kullanarak TreeView'ı doldurur."""
        # Ağacı temizle
        for item in self.db_tree.get_children():
            self.db_tree.delete(item)

        schemas = {}
        for index, row in df.iterrows():
            schema_name = row['f_table_schema']
            table_name = row['f_table_name']
            geom_type = row['type']

            # Eğer şema daha önce eklenmediyse, ana düğüm olarak ekle
            if schema_name not in schemas:
                # Şemaların başlangıçta kapalı gelmesi için 'open=False' yapıldı.
                schema_node = self.db_tree.insert("", END, text=f" {schema_name}", open=False, image=self.small_orange_icon)
                schemas[schema_name] = schema_node
            
            # Tabloyu ilgili şemanın altına ekle
            self.db_tree.insert(schemas[schema_name], END, text=f" {table_name} ({geom_type})")

        # Dialog penceresini kapat
        if self.connection_window and self.connection_window.winfo_exists():
            self.connection_window.destroy()
        self._log_status("Veritabanı yapısı kenar çubuğuna yüklendi.")

    def on_tree_double_click(self, event):
        """
        Treeview'daki bir öğeye çift tıklandığında çalışır.
        Eğer tıklanan bir tablo ise, sorgu ekranına ilgili SELECT sorgusunu yazar.
        """
        # Tıklanan öğenin ID'sini al
        item_id = self.db_tree.focus()
        
        # Tıklanan öğenin bir ebeveyni (parent) var mı diye kontrol et.
        # Sadece tabloların (çocuk öğelerin) bir ebeveyni vardır, şemaların yoktur.
        parent_id = self.db_tree.parent(item_id)
        
        if parent_id: # Eğer bir ebeveyn varsa, bu bir tablodur.
            # Tablo adını al ve temizle (örn: " depremler (POINT)" -> "depremler")
            table_info = self.db_tree.item(item_id, "text")
            table_name = table_info.strip().split(" ")[0]
            
            # Şema adını al ve temizle (örn: " public" -> "public")
            schema_info = self.db_tree.item(parent_id, "text")
            schema_name = schema_info.strip()
            
            # SQL Sorgusunu oluştur
            query = f"SELECT * FROM {schema_name}.{table_name} LIMIT 100;"
            
            # Mevcut sorgu ekranını temizle
            self.query_text.delete("1.0", END)
            # Yeni sorguyu ekrana yaz
            self.query_text.insert("1.0", query)
            
            self._log_status(f"'{schema_name}.{table_name}' için sorgu oluşturuldu.")
    
    def run_query_and_map_thread(self):
        if not self.db_params:
            messagebox.showwarning("Bağlantı Yok", "Lütfen önce veritabanına bağlanın.", parent=self.root)
            return

        self._set_buttons_state(DISABLED)
        self._log_status("Sorgu ve haritalama işlemi başlatılıyor...")
        thread = threading.Thread(target=self._execute_run_query_and_map, daemon=True)
        thread.start()

    # --- _execute_run_query_and_map İÇİNDE GÜNCELLEME ---
    def _execute_run_query_and_map(self):
        sql_sorgusu = self.query_text.get("1.0", END).strip()
        if not self.db_params or not sql_sorgusu:
            if not sql_sorgusu:
                messagebox.showerror("Eksik Bilgi", "Lütfen SQL sorgusunu girin.", parent=self.root)
            self.root.after(0, self._set_buttons_state, NORMAL)
            return

        # Haritayı ve veri ızgarasını temizle
        self.map_widget.delete_all_polygon()
        self.map_widget.delete_all_marker()
        self.map_widget.delete_all_path()
        self.root.after(0, self._populate_data_grid, pd.DataFrame()) # Boş DataFrame ile ızgarayı temizle
        
        conn = None
        try:
            self._log_status("Veritabanına bağlanılıyor...")
            conn = psycopg2.connect(**self.db_params, connect_timeout=10)
            self._log_status("Bağlantı başarılı. Sorgu çalıştırılıyor...")
            
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", category=UserWarning)
                gdf = gpd.read_postgis(sql_sorgusu, conn, geom_col='geom')

            if gdf.empty:
                self._log_status("Sorgu sonuç döndürmedi.")
                messagebox.showinfo("Bilgi", "Sorgu sonuç döndürmedi.", parent=self.root)
                return

            # YENİ: Veri ızgarasını ana thread'de doldur
            self.root.after(0, self._populate_data_grid, gdf)
            
            self._log_status(f"{len(gdf)} adet coğrafi obje bulundu. Haritaya toplu çizim başlıyor...")

            # ... (feature_generator ve on_drawing_complete metotları aynı kalır) ...
            def feature_generator(geodataframe):
                geometry_col = geodataframe.geometry.name
                for index, row in geodataframe.iterrows():
                    geom = row[geometry_col]
                    if geom is None or geom.is_empty:
                        continue
                    
                    popup_text = str(row['aciklama']) if 'aciklama' in geodataframe.columns and pd.notna(row['aciklama']) else f"Obje {index + 1}"
                    geom_type = geom.geom_type

                    if geom_type == 'Polygon':
                        coords = list(geom.exterior.coords)
                        if len(coords) >= 4:
                            yield ('polygon', coords, popup_text)
                        else:
                            self._log_status(f"Uyarı: Geçersiz poligon atlandı (Nokta Sayısı: {len(coords)}).")
                    
                    elif geom_type == 'MultiPolygon':
                        for poly in geom.geoms:
                            coords = list(poly.exterior.coords)
                            if len(coords) >= 4:
                                yield ('polygon', coords, popup_text)
                            else:
                                self._log_status(f"Uyarı: Geçersiz çoklu-poligon parçası atlandı (Nokta Sayısı: {len(coords)}).")

                    elif geom_type == 'Point':
                        yield ('point', (geom.y, geom.x), popup_text)

                    elif geom_type == 'LineString':
                        coords = list(geom.coords)
                        if len(coords) >= 2:
                            yield ('path', coords, popup_text)
                        else:
                            self._log_status(f"Uyarı: Geçersiz çizgi atlandı (Nokta Sayısı: {len(coords)}).")

                    elif geom_type == 'MultiLineString':
                        for line in geom.geoms:
                            coords = list(line.coords)
                            if len(coords) >= 2:
                                yield ('path', coords, popup_text)
                            else:
                                self._log_status(f"Uyarı: Geçersiz çoklu-çizgi parçası atlandı (Nokta Sayısı: {len(coords)}).")
            
            def on_drawing_complete():
                self._log_status("Haritaya çizim tamamlandı.")
                if not gdf.empty:
                    if not gdf.total_bounds.any() is None and all(gdf.total_bounds != float('inf')) and all(gdf.total_bounds != float('-inf')):
                        min_lon, min_lat, max_lon, max_lat = gdf.total_bounds
                        self.map_widget.fit_bounding_box((max_lat, min_lon), (min_lat, max_lon))
                        current_zoom = self.map_widget.zoom
                        if current_zoom > 18:
                            self.map_widget.set_zoom(18)
                        elif len(gdf) == 1 and gdf.geom_type.iloc[0] == 'Point':
                            self.map_widget.set_position(gdf.geometry.y.iloc[0], gdf.geometry.x.iloc[0], zoom=15)
                    else:
                        self._log_status("Uyarı: Geçerli bir kapsama alanı (bounds) hesaplanamadı.")

            
            gen = feature_generator(gdf)
            self.root.after(10, self._draw_features_in_batches, gen, on_drawing_complete)

        except psycopg2.Error as e:
            self._log_status(f"Veritabanı veya SQL Hatası: {e}")
            messagebox.showerror("Veritabanı/SQL Hatası", f"Detaylar: {str(e)}", parent=self.root)
        except Exception as e:
            import traceback
            tb_str = traceback.format_exc()
            self._log_status(f"Genel Bir Hata Oluştu: {e}\n{tb_str}")
            messagebox.showerror("Bir Hata Oluştu", f"Detaylar: {str(e)}\n\nTraceback:\n{tb_str}", parent=self.root)
        finally:
            if conn:
                conn.close()
                self._log_status("Veritabanı bağlantısı kapatıldı.")
            self.root.after(0, self._set_buttons_state, NORMAL)
    
    # ... (_draw_features_in_batches metodu değişmeden kalır) ...
    def _draw_features_in_batches(self, feature_generator, on_complete_callback, batch_size=25):
        try:
            for _ in range(batch_size):
                geom_type, data, _ = next(feature_generator) # popup_text'i almıyoruz

                if geom_type == 'polygon':
                    coords_lon_lat = data
                    coords_lat_lon = [(lat, lon) for lon, lat in coords_lon_lat]
                    self.map_widget.set_polygon(coords_lat_lon,
                                                outline_color="yellow",
                                                fill_color="#FFFF00")
                elif geom_type == 'path':
                    coords_lon_lat = data
                    coords_lat_lon = [(lat, lon) for lon, lat in coords_lon_lat]
                    self.map_widget.set_path(coords_lat_lon,
                                             color="yellow",
                                             width=2)
                elif geom_type == 'point':
                    lat, lon = data
                    self.map_widget.set_marker(lat, lon,
                                               text="",
                                               icon=self.small_orange_icon)

            self.root.after(1, self._draw_features_in_batches, feature_generator, on_complete_callback, batch_size)

        except StopIteration:
            if on_complete_callback:
                on_complete_callback()
        except Exception as e:
            self._log_status(f"Haritaya çizerken hata oluştu: {e}")
            messagebox.showerror("Çizim Hatası", f"Bir obje çizilirken hata oluştu:\n{e}", parent=self.root)


if __name__ == "__main__":
    root = ttk.Window(themename="darkly")
    app = PostGISApp(root)
    root.mainloop()
