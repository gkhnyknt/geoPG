import ttkbootstrap as ttk
from ttkbootstrap.constants import *
from tkinter import filedialog, messagebox, scrolledtext
import psycopg2
import geopandas as gpd
from tkintermapview import TkinterMapView
from PIL import Image, ImageTk, ImageDraw
import threading # Bağlantı testi için
import warnings # YENİ: Uyarıları yönetmek için

# --- KÜÇÜK DAİRE İKONU OLUŞTURMA ---
def create_small_circle_icon(size, color_tuple):
    """Belirtilen boyutta ve renkte (RGBA tuple) dairesel bir PIL Image nesnesi oluşturur."""
    image = Image.new('RGBA', (size, size), (0, 0, 0, 0))  # Şeffaf arka plan
    draw = ImageDraw.Draw(image)
    draw.ellipse((0, 0, size - 1, size - 1), fill=color_tuple, outline=color_tuple)
    return ImageTk.PhotoImage(image)

class PostGISApp:
    def __init__(self, root):
        self.root = root
        self.root.title("PostGIS Sorgu ve Harita Görüntüleyici (Modern Tema)")
        self.root.geometry("850x850") # Boyutu biraz artırdık

        # --- İkonlar ---
        # Noktalar için turuncu ikon
        self.small_orange_icon = create_small_circle_icon(10, (255, 165, 0, 255)) # RGBA for orange

        # --- ARAYÜZ ELEMANLARI ---
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.pack(fill=BOTH, expand=YES)

        # Üst Bölüm: Bağlantı, Sorgu, Butonlar
        top_controls_frame = ttk.Frame(main_frame)
        top_controls_frame.pack(side=TOP, fill=X, pady=(0, 10))

        # Sol Bölüm (Bağlantı ve Sorgu)
        left_panel = ttk.Frame(top_controls_frame)
        left_panel.pack(side=LEFT, fill=X, expand=YES, padx=(0,10))

        # Sağ Bölüm (Butonlar) - Bunu doğrudan left_panel altına alabiliriz veya ayrı bir çerçeve
        # Şimdilik butonları left_panel'in altına ekleyelim.

        # 1. Bağlantı Bilgileri
        connection_frame = ttk.LabelFrame(left_panel, text="1. PostgreSQL Bağlantı Bilgileri", padding="10", bootstyle=INFO)
        connection_frame.pack(pady=5, fill=X)
        
        labels_db = ['Host:', 'Port:', 'Veritabanı Adı:', 'Kullanıcı Adı:', 'Şifre:']
        defaults_db = ['localhost', '5432', 'veritabani_adi', 'kullanici_adi', 'sifre']
        self.db_entries = {}

        for i, label_text in enumerate(labels_db):
            ttk.Label(connection_frame, text=label_text).grid(row=i, column=0, sticky=W, padx=5, pady=3)
            entry = ttk.Entry(connection_frame, width=40) # Genişliği ayarladık
            entry.insert(0, defaults_db[i])
            entry.grid(row=i, column=1, sticky=EW, padx=5, pady=3)
            if label_text == 'Şifre:':
                entry.config(show="*")
            # Anahtar oluştururken Türkçe karakterleri koruyalım ama : ve boşlukları temizleyelim
            key_name = label_text.lower().replace(":", "").replace(" ", "_")
            self.db_entries[key_name] = entry
        connection_frame.grid_columnconfigure(1, weight=1)

        # Bağlantı Test Butonu
        self.test_conn_button = ttk.Button(connection_frame, text="Bağlantıyı Test Et",
                                           command=self.test_db_connection_thread, bootstyle=OUTLINE + INFO)
        self.test_conn_button.grid(row=len(labels_db), column=0, columnspan=2, pady=(10,5), sticky=EW)


        # 2. SQL Sorgusu
        query_frame = ttk.LabelFrame(left_panel, text="2. SQL Sorgusu (Coğrafi kolon 'geom' olmalı)", padding="10", bootstyle=INFO)
        query_frame.pack(pady=10, fill=X)
        self.query_text = scrolledtext.ScrolledText(query_frame, wrap=WORD, height=6, relief="sunken", borderwidth=1,
                                                    font=('Consolas', 10), bg="#292929", fg="#cccccc", insertbackground="#ffffff")
        self.query_text.pack(expand=YES, fill=BOTH)
        self.query_text.insert(INSERT, "SELECT il_adi as aciklama, geom FROM iller") # Örnek sorgu

        # Çalıştır Butonu
        self.run_button = ttk.Button(left_panel, text="Sorguyu Çalıştır ve Haritada Göster",
                                     command=self.run_query_and_map_thread, bootstyle=SUCCESS)
        self.run_button.pack(pady=10, fill=X, ipady=5)

        # Alt Bölüm: Harita ve Log
        bottom_display_frame = ttk.Frame(main_frame)
        bottom_display_frame.pack(side=BOTTOM, fill=BOTH, expand=YES)
        
        # 3. Harita
        map_labelframe = ttk.LabelFrame(bottom_display_frame, text="3. Harita", padding="5", bootstyle=PRIMARY) # Bootstyle değişti
        map_labelframe.pack(side=LEFT, fill=BOTH, expand=YES, padx=(0,5))
        
        self.map_widget = TkinterMapView(map_labelframe, width=600, height=500, corner_radius=0) # Boyutlar ayarlandı
        self.map_widget.pack(fill=BOTH, expand=YES)
        self.map_widget.set_position(39.925533, 32.866287) # Ankara
        self.map_widget.set_zoom(6)
        self.map_widget.set_tile_server("https://mt0.google.com/vt/lyrs=s&hl=tr&x={x}&y={y}&z={z}&s=Ga", max_zoom=22) # Google Satellite (Türkçe)

        # 4. İşlem Günlüğü
        log_labelframe = ttk.LabelFrame(bottom_display_frame, text="4. İşlem Günlüğü", padding="5", bootstyle=SECONDARY) # Bootstyle değişti
        log_labelframe.pack(side=RIGHT, fill=Y, expand=NO, ipadx=5) # Genişlemesin, sadece Y'de dolsun
        
        self.status_log = scrolledtext.ScrolledText(log_labelframe, wrap=WORD, height=10, width=35, relief="sunken", borderwidth=1,
                                                    font=('Consolas', 9), bg="#292929", fg="#cccccc", insertbackground="#ffffff",
                                                    state=DISABLED)
        self.status_log.pack(fill=BOTH, expand=YES)

    def _log_status(self, message):
        """İşlem günlüğüne mesaj ekler (thread-safe)."""
        def _update_log():
            self.status_log.config(state=NORMAL)
            self.status_log.insert(END, message + "\n")
            self.status_log.see(END)
            self.status_log.config(state=DISABLED)
        self.root.after(0, _update_log)

    def _get_db_params(self):
        """Arayüzden veritabanı parametrelerini alır."""
        params = {}
        try:
            params['dbname'] = self.db_entries['veritabanı_adı'].get()
            params['user'] = self.db_entries['kullanıcı_adı'].get()
            params['password'] = self.db_entries['şifre'].get() 
            params['host'] = self.db_entries['host'].get()
            params['port'] = self.db_entries['port'].get()

            if not params['host'] or not params['port'] or not params['dbname'] or not params['user']:
                messagebox.showerror("Eksik Bilgi", "Host, Port, Veritabanı Adı ve Kullanıcı Adı alanları doldurulmalıdır.", parent=self.root)
                return None
            
            if params['port']:
                try:
                    int(params['port']) 
                except ValueError:
                    messagebox.showerror("Geçersiz Port", "Port geçerli bir sayı olmalıdır.", parent=self.root)
                    return None
            else: 
                messagebox.showerror("Eksik Bilgi", "Port numarası boş bırakılamaz.", parent=self.root)
                return None

        except KeyError as e:
            self._log_status(f"Program Hatası: DB giriş alanı anahtarı bulunamadı: {e}. self.db_entries: {self.db_entries.keys()}")
            messagebox.showerror("Program Hatası", f"DB giriş alanı bulunamadı: {e}\nLütfen geliştiriciye bildirin.", parent=self.root)
            return None
        return params

    def _set_buttons_state(self, state):
        """Butonların durumunu ayarlar (NORMAL veya DISABLED)."""
        self.test_conn_button.config(state=state)
        self.run_button.config(state=state)

    def test_db_connection_thread(self):
        """Bağlantı testini ayrı bir thread'de başlatır."""
        self._set_buttons_state(DISABLED)
        self._log_status("Bağlantı testi başlatılıyor...")
        thread = threading.Thread(target=self._execute_test_db_connection, daemon=True)
        thread.start()
        self.root.after(100, self._check_thread_completion, thread)


    def _execute_test_db_connection(self):
        """Veritabanı bağlantısını test eder."""
        db_params = self._get_db_params()
        if not db_params:
            return

        conn = None
        try:
            self._log_status(f"Bağlanılıyor: {db_params['host']}:{db_params['port']}/{db_params['dbname']}")
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", category=UserWarning, module="pandas.io.sql")
                warnings.filterwarnings("ignore", category=UserWarning, module="geopandas.io.sql")
                conn = psycopg2.connect(**db_params, connect_timeout=5)
            self._log_status("Bağlantı testi BAŞARILI!")
        except psycopg2.Error as e:
            self._log_status(f"Bağlantı testi BAŞARISIZ: {e}")
            messagebox.showerror("Bağlantı Testi Başarısız", f"Bağlantı kurulamadı:\n{e}", parent=self.root)
        except Exception as e:
            self._log_status(f"Bağlantı testi sırasında beklenmedik HATA: {e}")
            messagebox.showerror("Bağlantı Testi Hatası", f"Beklenmedik bir hata oluştu:\n{e}", parent=self.root)
        finally:
            if conn:
                conn.close()


    def run_query_and_map_thread(self):
        """Sorgu ve haritalama işlemini ayrı bir thread'de başlatır."""
        self._set_buttons_state(DISABLED)
        self._log_status("Sorgu ve haritalama işlemi başlatılıyor...")
        thread = threading.Thread(target=self._execute_run_query_and_map, daemon=True)
        thread.start()
        self.root.after(100, self._check_thread_completion, thread)

    def _check_thread_completion(self, thread):
        """Thread'in tamamlanıp tamamlanmadığını kontrol eder."""
        if thread.is_alive():
            self.root.after(100, self._check_thread_completion, thread)
        else:
            self._set_buttons_state(NORMAL) 
            self._log_status("İşlem tamamlandı veya durdu.")


    def _execute_run_query_and_map(self):
        """
        Arayüzdeki bilgileri alarak PostgreSQL'e bağlanır, sorguyu çalıştırır,
        sonuçları arayüzdeki harita bileşenine çizer.
        """
        db_params = self._get_db_params()
        sql_sorgusu = self.query_text.get("1.0", END).strip()

        if not db_params:
            return
        if not sql_sorgusu:
            messagebox.showerror("Eksik Bilgi", "Lütfen SQL sorgusunu girin.", parent=self.root)
            return

        self.map_widget.delete_all_polygon()
        self.map_widget.delete_all_marker()
        self.map_widget.delete_all_path()
        
        conn = None
        try:
            self._log_status("Veritabanına bağlanılıyor...")
            conn = psycopg2.connect(**db_params, connect_timeout=10)
            self._log_status("Bağlantı başarılı. Sorgu çalıştırılıyor...")
            
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", category=UserWarning, module="pandas.io.sql")
                warnings.filterwarnings("ignore", category=UserWarning, module="geopandas.io.sql")
                gdf = gpd.read_postgis(sql_sorgusu, conn, geom_col='geom')

            if gdf.empty:
                self._log_status("Sorgu sonuç döndürmedi.")
                messagebox.showinfo("Bilgi", "Sorgu sonuç döndürmedi.", parent=self.root)
                return

            geometry_col_name = gdf.geometry.name 
            self._log_status(f"{len(gdf)} adet coğrafi obje bulundu. Haritaya çiziliyor...")

            min_lat, min_lon, max_lat, max_lon = float('inf'), float('inf'), float('-inf'), float('-inf')
            feature_count = 0

            for index, row in gdf.iterrows():
                geom = row[geometry_col_name]
                if geom is None or geom.is_empty:
                    self._log_status(f"  Uyarı: {index}. satırdaki geometri boş veya tanımsız, atlanıyor.")
                    continue
                
                feature_count += 1
                popup_text = str(row['aciklama']) if 'aciklama' in gdf.columns and pd.notna(row['aciklama']) else f"Obje {index + 1}"

                g_min_lon, g_min_lat, g_max_lon, g_max_lat = geom.bounds
                min_lon = min(min_lon, g_min_lon)
                min_lat = min(min_lat, g_min_lat)
                max_lon = max(max_lon, g_max_lon)
                max_lat = max(max_lat, g_max_lat)

                if geom.geom_type == 'Polygon':
                    coords = list(geom.exterior.coords) 
                    polygon_coords_latlon = [(lat, lon) for lon, lat in coords]
                    self.map_widget.set_polygon(polygon_coords_latlon,
                                                name=popup_text, 
                                                outline_color="yellow",
                                                fill_color="#FFFF00"
                                                # DÜZELTME: width parametresi kaldırıldı
                                                ) 
                elif geom.geom_type == 'MultiPolygon':
                    for poly in geom.geoms:
                        coords = list(poly.exterior.coords)
                        polygon_coords_latlon = [(lat, lon) for lon, lat in coords]
                        self.map_widget.set_polygon(polygon_coords_latlon,
                                                    name=popup_text,
                                                    outline_color="yellow",
                                                    fill_color="#FFFF00"
                                                    # DÜZELTME: width parametresi kaldırıldı
                                                    )
                elif geom.geom_type == 'Point':
                    self.map_widget.set_marker(geom.y, geom.x, text="", 
                                               icon=self.small_orange_icon,
                                               command=lambda p=popup_text: self.show_marker_info(p)) 
                elif geom.geom_type == 'LineString':
                    coords = list(geom.coords) 
                    path_coords_latlon = [(lat, lon) for lon, lat in coords]
                    self.map_widget.set_path(path_coords_latlon,
                                             color="yellow", 
                                             width=2)
                elif geom.geom_type == 'MultiLineString':
                    for line in geom.geoms:
                        coords = list(line.coords)
                        path_coords_latlon = [(lat, lon) for lon, lat in coords]
                        self.map_widget.set_path(path_coords_latlon,
                                                 color="yellow",
                                                 width=2)
            
            if feature_count > 0:
                if min_lat != float('inf'): 
                    self.map_widget.fit_bounding_box((max_lat, min_lon), (min_lat, max_lon))
                    current_zoom = self.map_widget.zoom
                    if current_zoom > 18: 
                         self.map_widget.set_zoom(18)
                    elif feature_count == 1 and gdf.geom_type.iloc[0] == 'Point':
                         self.map_widget.set_position(gdf.geometry.y.iloc[0], gdf.geometry.x.iloc[0], zoom=15)

            self._log_status("Haritaya çizim tamamlandı.")

        except psycopg2.Error as e:
            self._log_status(f"Veritabanı veya SQL Hatası: {e}")
            messagebox.showerror("Veritabanı/SQL Hatası", f"Detaylar: {str(e)}", parent=self.root)
        except Exception as e:
            self._log_status(f"Genel Bir Hata Oluştu: {e}")
            messagebox.showerror("Bir Hata Oluştu", f"Detaylar: {str(e)}", parent=self.root)
        finally:
            if conn:
                conn.close()
                self._log_status("Veritabanı bağlantısı kapatıldı.")

    def show_marker_info(self, text_content):
        """Nokta işaretçisine tıklandığında bilgi mesajı gösterir."""
        messagebox.showinfo("Nokta Bilgisi", text_content, parent=self.root)


if __name__ == "__main__":
    root = ttk.Window(themename="darkly") 
    app = PostGISApp(root)
    root.mainloop()
