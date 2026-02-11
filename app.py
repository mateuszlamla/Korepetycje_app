import streamlit as st
import pandas as pd
import os
import altair as alt
from datetime import datetime, timedelta, date, time
from dateutil.relativedelta import relativedelta
from streamlit_calendar import calendar
from st_supabase_connection import SupabaseConnection
from supabase import create_client, Client
import httpx

# Alternatywny sposób połączenia z wyłączonym HTTP/2
@st.cache_resource
def get_supabase_client():
    url = st.secrets["connections"]["supabase"]["url"]
    key = st.secrets["connections"]["supabase"]["key"]
    # Wymuszamy HTTP/1.1 przez własny httpx.Client
    http_client = httpx.Client(http2=False)
    return create_client(url, key, options=httpx.Client(http2=False))

# Używaj tego klienta zamiast st.connection jeśli błędy nie ustąpią
supabase_client = get_supabase_client()

# --- KONFIGURACJA STRONY ---
st.set_page_config(page_title="Menedżer Korepetycji", layout="wide", page_icon="📚")

# --- PLIKI DANYCH ---
FILE_DB = 'uczniowie.csv'
FILE_SETTLEMENTS = 'rozliczenia.csv'
FILE_CANCELLATIONS = 'odwolane.csv'
FILE_EXTRA = 'dodatkowe.csv'
FILE_SCHEDULE = 'harmonogram.csv'

# Definicja kolumn
COLUMNS = [
    'ID', 'Imie', 'Nazwisko', 'H_w_tygodniu', 'Stawka', 
    'Dojazd',
    'Nieobecnosci', 'Odrabiania', 'Do_odrobienia_umowione', 'Do_odrobienia_nieumowione',
    'Szkola', 'Klasa', 'Poziom', 'Nr_tel', 
    'Data_rozp', 'Data_zak', 'Dzien_tyg', 'Godzina', 'Adres',
    'Tryb_platnosci'
]
COLUMNS_SETTLEMENTS = ['Uczen_ID', 'Okres', 'Kwota_Wymagana', 'Wplacono']
COLUMNS_CANCELLATIONS = ['Uczen_ID', 'Data', 'Powod']
# Dodajemy kolumnę 'Status' do śledzenia czy odrabianie zostało już "zaliczone" (minął termin)
COLUMNS_EXTRA = ['Uczen_ID', 'Data', 'Godzina', 'Stawka', 'Typ', 'Czas', 'Status']
COLUMNS_SCHEDULE = ['Uczen_ID', 'Dzien_tyg', 'Godzina', 'Czas_trwania', 'Data_od', 'Data_do', 'Stawka']

# Mapowanie dni tygodnia
DNI_MAPA = {
    "Poniedziałek": 0, "Wtorek": 1, "Środa": 2, "Czwartek": 3, 
    "Piątek": 4, "Sobota": 5, "Niedziela": 6
}
MIESIACE_PL = {
    1: 'Styczeń', 2: 'Luty', 3: 'Marzec', 4: 'Kwiecień', 5: 'Maj', 6: 'Czerwiec',
    7: 'Lipiec', 8: 'Sierpień', 9: 'Wrzesień', 10: 'Październik', 11: 'Listopad', 12: 'Grudzień'
}

# --- NOWE FUNKCJE ŁADOWANIA DANYCH (SUPABASE) ---

def load_data():
    res = supabase_client.table("uczniowie").select("*").execute()
    df = pd.DataFrame(res.data)
    if df.empty: return pd.DataFrame(columns=COLUMNS)
    # Konwersja typów dla stabilności obliczeń
    cols_num = ['Stawka', 'Dojazd', 'Odrabiania', 'Nieobecnosci', 'Do_odrobienia_umowione', 'Do_odrobienia_nieumowione']
    for c in cols_num:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0.0)
    return df

def save_data(df):
    data = df.to_dict(orient='records')
    supabase_client.table("uczniowie").upsert(data).execute()

def load_settlements():
    res = supabase_client.table("rozliczenia").select("*").execute()
    return pd.DataFrame(res.data) if res.data else pd.DataFrame(columns=COLUMNS_SETTLEMENTS)

def save_settlements(df):
    supabase_client.table("rozliczenia").upsert(df.to_dict(orient='records')).execute()

def load_cancellations():
    res = supabase_client.table("odwolane").select("*").execute()
    return pd.DataFrame(res.data) if res.data else pd.DataFrame(columns=COLUMNS_CANCELLATIONS)

def save_cancellations(df):
    supabase_client.table("odwolane").upsert(df.to_dict(orient='records')).execute()

def load_extra():
    res = supabase_client.table("dodatkowe").select("*").execute()
    return pd.DataFrame(res.data) if res.data else pd.DataFrame(columns=COLUMNS_EXTRA)

def save_extra(df):
    supabase_client.table("dodatkowe").upsert(df.to_dict(orient='records')).execute()

def load_schedule():
    res = supabase_client.table("harmonogram").select("*").execute()
    return pd.DataFrame(res.data) if res.data else pd.DataFrame(columns=COLUMNS_SCHEDULE)

def save_schedule(df):
    supabase_client.table("harmonogram").upsert(df.to_dict(orient='records')).execute()

# --- LOGIKA MIGRACJI I POMOCNICZA ---
def parse_student_terms_legacy(row):
    days = str(row['Dzien_tyg']).split(';')
    times = str(row['Godzina']).split(';')
    durations = str(row['H_w_tygodniu']).split(';')
    terms = []
    for i, day in enumerate(days):
        day = day.strip()
        if not day or day == '-': continue
        time_str = times[i].strip() if i < len(times) else (times[-1].strip() if times else "00:00")
        try:
            dur_str = durations[i].strip() if i < len(durations) else (durations[-1].strip() if durations else "1.0")
            dur_val = float(dur_str)
        except: dur_val = 1.0
        terms.append({'day_name': day, 'time_str': time_str, 'duration': dur_val})
    return terms

def check_and_migrate_schedule(df_students):
    if not os.path.exists(FILE_SCHEDULE) or (os.path.exists(FILE_SCHEDULE) and os.stat(FILE_SCHEDULE).st_size == 0):
        if not df_students.empty:
            new_rows = []
            for _, s_row in df_students.iterrows():
                terms = parse_student_terms_legacy(s_row)
                for t in terms:
                    new_rows.append({
                        'Uczen_ID': s_row['ID'],
                        'Dzien_tyg': t['day_name'],
                        'Godzina': t['time_str'],
                        'Czas_trwania': t['duration'],
                        'Data_od': s_row['Data_rozp'],
                        'Data_do': s_row['Data_zak'],
                        'Stawka': s_row['Stawka']
                    })
            if new_rows:
                df_sch = pd.DataFrame(new_rows, columns=COLUMNS_SCHEDULE)
                save_schedule(df_sch)
                st.toast("Zmigrowano stary plan zajęć do nowego harmonogramu!")
                return True
    return False

def process_past_makeups(df_students, df_extra):
    today = date.today()
    changes = False
    for idx, row in df_extra.iterrows():
        if row.get('Typ') == 'Odrabianie':
            try:
                l_date = pd.to_datetime(row['Data']).date()
                status = row.get('Status', 'Zaplanowana')
                if l_date < today and status != 'Zrealizowana':
                    s_idx = df_students.index[df_students['ID'] == row['Uczen_ID']].tolist()
                    if s_idx:
                        s_idx = s_idx[0]
                        curr_val = df_students.at[s_idx, 'Do_odrobienia_umowione']
                        duration = float(row.get('Czas', 1.0))
                        if curr_val > 0:
                            df_students.at[s_idx, 'Do_odrobienia_umowione'] = max(0.0, curr_val - duration)
                        df_extra.at[idx, 'Status'] = 'Zrealizowana'
                        changes = True
            except Exception: pass
    if changes:
        save_data(df_students)
        save_extra(df_extra)
        return True
    return False

def parse_student_terms(row):
    days = str(row['Dzien_tyg']).split(';')
    times = str(row['Godzina']).split(';')
    durations = str(row['H_w_tygodniu']).split(';')
    terms = []
    for i, day in enumerate(days):
        day = day.strip()
        if not day or day == '-': continue
        time_str = times[i].strip() if i < len(times) else (times[-1].strip() if times else "00:00")
        try:
            dur_str = durations[i].strip() if i < len(durations) else (durations[-1].strip() if durations else "1.0")
            dur_val = float(dur_str)
        except: dur_val = 1.0
        terms.append({'day_name': day, 'time_str': time_str, 'duration': dur_val})
    return terms

# --- GŁÓWNA LOGIKA KALENDARZA I FINANSÓW ---

def get_lessons_in_period(df_students, start_date, end_date):
    """Zwraca listę lekcji (stałych i dodatkowych) w zadanym okresie, uwzględniając WSZYSTKIE odwołania (do Kalendarza i listy)."""
    lessons = []
    df_cancel = load_cancellations()
    df_extra = load_extra()
    df_schedule = load_schedule()
    
    cancelled_set = set()
    if not df_cancel.empty:
        for _, row in df_cancel.iterrows():
            cancelled_set.add((row['Uczen_ID'], str(row['Data'])))

    student_map = df_students.set_index('ID').to_dict('index')
    
    current_day = start_date
    while current_day <= end_date:
        weekday_num = current_day.weekday()
        current_day_str = str(current_day)
        
        for _, sch_row in df_schedule.iterrows():
            if DNI_MAPA.get(sch_row['Dzien_tyg']) == weekday_num:
                try:
                    s_valid_start = pd.to_datetime(sch_row['Data_od']).date()
                    s_valid_end = pd.to_datetime(sch_row['Data_do']).date()
                    
                    if s_valid_start <= current_day <= s_valid_end:
                        uid = sch_row['Uczen_ID']
                        if uid in student_map: 
                            if (uid, current_day_str) not in cancelled_set:
                                s_info = student_map[uid]
                                dur = float(sch_row['Czas_trwania'])
                                sch_rate = float(sch_row.get('Stawka', 0))
                                hourly_rate = sch_rate if sch_rate > 0 else s_info['Stawka']
                                full_rate = (hourly_rate * dur) + s_info.get('Dojazd', 0)
                                
                                lessons.append({
                                    'Data': current_day, 'Uczen_ID': uid, 'Stawka': full_rate, 
                                    'Godzina': sch_row['Godzina'], 
                                    'Imie': s_info['Imie'], 'Nazwisko': s_info['Nazwisko'],
                                    'Typ': 'Stała', 'Czas': dur
                                })
                except: pass
        current_day += timedelta(days=1)

    if not df_extra.empty:
        for _, row in df_extra.iterrows():
            try:
                l_date = pd.to_datetime(row['Data']).date()
                if start_date <= l_date <= end_date:
                    uid = row['Uczen_ID']
                    if uid in student_map:
                        s_info = student_map[uid]
                        duration = float(row.get('Czas', 1.0))
                        lessons.append({
                            'Data': l_date, 'Uczen_ID': uid, 'Stawka': row['Stawka'],
                            'Godzina': row['Godzina'], 
                            'Imie': s_info['Imie'], 'Nazwisko': s_info['Nazwisko'],
                            'Typ': row.get('Typ', 'Dodatkowa'), 'Czas': duration
                        })
            except: pass
            
    return lessons

def get_predicted_lessons(df_students, start_date, end_date):
    """
    Zwraca listę lekcji do PLANU finansowego.
    ZASADA: Liczymy lekcje stałe, nawet jeśli odwołane (chyba że Święto/Edycja). NIE liczymy dodatkowych.
    """
    lessons = []
    df_cancel = load_cancellations()
    df_schedule = load_schedule()
    
    # Wykluczamy TYLKO te odwołane z powodu "Święto" lub "Edycja"
    excluded_set = set()
    if not df_cancel.empty:
        ex_recs = df_cancel[df_cancel['Powod'].astype(str).str.contains("Święto|Edycja")]
        for _, r in ex_recs.iterrows():
            excluded_set.add((r['Uczen_ID'], str(r['Data'])))
            
    student_map = df_students.set_index('ID').to_dict('index')
    
    current_day = start_date
    while current_day <= end_date:
        weekday_num = current_day.weekday()
        current_day_str = str(current_day)
        
        for _, sch_row in df_schedule.iterrows():
            if DNI_MAPA.get(sch_row['Dzien_tyg']) == weekday_num:
                try:
                    s_valid_start = pd.to_datetime(sch_row['Data_od']).date()
                    s_valid_end = pd.to_datetime(sch_row['Data_do']).date()
                    
                    if s_valid_start <= current_day <= s_valid_end:
                        uid = sch_row['Uczen_ID']
                        if uid in student_map:
                            if (uid, current_day_str) not in excluded_set:
                                s_info = student_map[uid]
                                dur = float(sch_row['Czas_trwania'])
                                sch_rate = float(sch_row.get('Stawka', 0))
                                hourly_rate = sch_rate if sch_rate > 0 else s_info['Stawka']
                                full_rate = (hourly_rate * dur) + s_info.get('Dojazd', 0)
                                
                                lessons.append({
                                    'Data': current_day, 'Uczen_ID': uid, 'Stawka': full_rate, 
                                    'Godzina': sch_row['Godzina'], 
                                    'Imie': s_info['Imie'], 'Nazwisko': s_info['Nazwisko'],
                                    'Typ': 'Stała', 'Czas': dur
                                })
                except: pass
        current_day += timedelta(days=1)
        
    return lessons

def calculate_predicted_income(df_students, start_date, end_date):
    lessons = get_predicted_lessons(df_students, start_date, end_date)
    return sum(l['Stawka'] for l in lessons)

def calculate_monthly_breakdown(df_students, student_id, target_month_date):
    breakdown = []
    total_amount = 0.0
    
    student_row = df_students[df_students['ID'] == student_id].iloc[0]
    tryb = student_row.get('Tryb_platnosci', 'Co zajęcia')
    
    y, m = target_month_date.year, target_month_date.month
    curr_start = date(y, m, 1)
    curr_end = curr_start + relativedelta(months=1) - timedelta(days=1)
    
    df_schedule = load_schedule()
    student_schedule = df_schedule[df_schedule['Uczen_ID'] == student_id]
    
    lessons_count = 0
    base_cost_accumulated = 0.0
    day_ptr = curr_start
    
    df_cancel = load_cancellations()
    holidays_set = set()
    if not df_cancel.empty:
        holiday_recs = df_cancel[
            (df_cancel['Uczen_ID'] == student_id) & 
            (df_cancel['Powod'].astype(str).str.contains("Święto|Edycja"))
        ]
        for _, r in holiday_recs.iterrows(): holidays_set.add(str(r['Data']))

    while day_ptr <= curr_end:
        weekday_num = day_ptr.weekday()
        if str(day_ptr) not in holidays_set:
            for _, sch in student_schedule.iterrows():
                if DNI_MAPA.get(sch['Dzien_tyg']) == weekday_num:
                    try:
                        s_od = pd.to_datetime(sch['Data_od']).date()
                        s_do = pd.to_datetime(sch['Data_do']).date()
                        if s_od <= day_ptr <= s_do:
                            lessons_count += 1
                            dur = float(sch['Czas_trwania'])
                            sch_rate = float(sch.get('Stawka', 0))
                            hourly_rate = sch_rate if sch_rate > 0 else student_row['Stawka']
                            cost = (hourly_rate * dur) + student_row.get('Dojazd', 0)
                            base_cost_accumulated += cost
                    except: pass
        day_ptr += timedelta(days=1)
            
    total_amount += base_cost_accumulated
    label_base = f"Abonament: {MIESIACE_PL[m]}" if tryb == 'Miesięcznie' else f"Planowe zajęcia: {MIESIACE_PL[m]}"
    
    breakdown.append({
        "Opis": f"{label_base} (Liczba: {lessons_count})",
        "Kwota": base_cost_accumulated,
        "Typ": "Baza"
    })
    
    query_start = curr_start
    query_end = curr_end
    
    df_extra = load_extra()
    if not df_extra.empty:
        extras = df_extra[(df_extra['Uczen_ID'] == student_id) & (pd.to_datetime(df_extra['Data']).dt.date >= query_start) & (pd.to_datetime(df_extra['Data']).dt.date <= query_end)]
        for _, row in extras.iterrows():
            typ = row.get('Typ', 'Dodatkowa')
            dur = row.get('Czas', 1.0)
            
            kwota_do_sumy = 0.0
            kwota_do_wyswietlenia = 0.0
            opis_typ = ""
            
            if typ == 'Dodatkowa':
                kwota_do_wyswietlenia = row['Stawka']
                opis_typ = f"Płatna ekstra ({dur}h)"
                if tryb == 'Miesięcznie': kwota_do_sumy = 0.0; opis_typ += " (Osobna poz.)"
                else: kwota_do_sumy = row['Stawka']
            
            elif typ == 'Edytowana':
                kwota_do_wyswietlenia = row['Stawka']
                kwota_do_sumy = row['Stawka']
                opis_typ = f"Zmiana w planie ({dur}h)"
                
            elif typ in ['Odrabianie', 'Przełożona']:
                if tryb == 'Co zajęcia':
                    kwota_do_sumy = row['Stawka']
                    kwota_do_wyswietlenia = row['Stawka']
                    opis_typ = f"Odrabianie ({dur}h)"
                else:
                    kwota_do_sumy = 0.0
                    kwota_do_wyswietlenia = 0.0
                    opis_typ = f"Odrabianie - bez dopłaty ({dur}h)"
            
            total_amount += kwota_do_sumy
            breakdown.append({"Opis": f"{typ}: {row['Data']}", "Kwota": kwota_do_wyswietlenia, "Typ": opis_typ})
            
    if not df_cancel.empty:
        cancels = df_cancel[(df_cancel['Uczen_ID'] == student_id) & (pd.to_datetime(df_cancel['Data']).dt.date >= query_start) & (pd.to_datetime(df_cancel['Data']).dt.date <= query_end)]
        for _, row in cancels.iterrows():
            powod = row.get('Powod', 'Nieznany')
            if "Święto" in str(powod) or "Edycja" in str(powod): continue
            
            if tryb == 'Miesięcznie':
                kwota_cancel = 0.0
                desc = f"Odwołana: {row['Data']} (Brak zwrotu)"
            else:
                day_of_cancel = pd.to_datetime(row['Data']).weekday()
                cost_of_lesson = 0.0
                found = False
                for _, sch in student_schedule.iterrows():
                    if DNI_MAPA.get(sch['Dzien_tyg']) == day_of_cancel:
                        s_od = pd.to_datetime(sch['Data_od']).date()
                        s_do = pd.to_datetime(sch['Data_do']).date()
                        target_d = pd.to_datetime(row['Data']).date()
                        if s_od <= target_d <= s_do:
                            dur = float(sch['Czas_trwania'])
                            sch_rate = float(sch.get('Stawka', 0))
                            hourly_rate = sch_rate if sch_rate > 0 else student_row['Stawka']
                            cost_of_lesson = (hourly_rate * dur) + student_row.get('Dojazd', 0)
                            found = True
                            break
                
                if found:
                    kwota_cancel = -cost_of_lesson
                    desc = f"Odwołana: {row['Data']} (Odliczenie)"
                else:
                    kwota_cancel = 0.0
                    desc = f"Odwołana: {row['Data']}"

            total_amount += kwota_cancel
            breakdown.append({"Opis": desc, "Kwota": kwota_cancel, "Typ": "Korekta"})
            
    return total_amount, breakdown

def generate_calendar_events(df_students):
    today = date.today()
    end_date = today + timedelta(days=365)
    start_date = today - timedelta(days=365)
    lessons = get_lessons_in_period(df_students, start_date, end_date)
    events = []
    for l in lessons:
        try:
            godzina_str = str(l['Godzina'])
            if len(godzina_str.split(':')) == 2: godzina_str += ":00"
            start_time = datetime.combine(l['Data'], datetime.strptime(godzina_str, "%H:%M:%S").time())
            duration_hours = l.get('Czas', 1.0)
            end_time = start_time + timedelta(hours=duration_hours)
            
            color = "#3788d8"
            typ = l['Typ']
            if typ == 'Dodatkowa': color = "#28a745"
            elif typ == 'Odrabianie': color = "#fd7e14"
            elif typ == 'Przełożona': color = "#6f42c1"
            elif typ == 'Edytowana': color = "#17a2b8" 
            
            events.append({
                "title": f"{l['Imie']} {l['Nazwisko']}",
                "start": start_time.isoformat(), "end": end_time.isoformat(),
                "backgroundColor": color, "borderColor": color,
                "extendedProps": {
                    "Uczen_ID": l['Uczen_ID'], "Typ": typ, "Data": l['Data'].strftime("%Y-%m-%d"),
                    "Godzina": str(l['Godzina']), "Stawka": l['Stawka'], "Imie": l['Imie'],
                    "Nazwisko": l['Nazwisko'], "Czas": duration_hours
                }
            })
        except: pass
    return events

# --- START APLIKACJI ---
df = load_data()
df_settlements = load_settlements()
df_cancellations = load_cancellations()
df_extra = load_extra()
df_schedule = load_schedule()

if check_and_migrate_schedule(df):
    df_schedule = load_schedule()

if process_past_makeups(df, df_extra):
    df = load_data()
    df_extra = load_extra()

with st.sidebar:
    st.title("📚 Korepetycje")
    menu = st.radio("Menu", ["📅 Kalendarz", "👤 Szczegóły Ucznia", "💰 Finanse (Wykres)", "➕ Dodaj Ucznia", "📋 Baza Danych"])

# --- ZAKŁADKA KALENDARZ ---
if menu == "📅 Kalendarz":
    st.header("Grafik Zajęć")
    
    with st.expander("➕ Dodaj dodatkową lekcję / odrabianie"):
        student_opts = {f"{r['Imie']} {r['Nazwisko']}": r['ID'] for i, r in df.iterrows()}
        c1, c2 = st.columns(2)
        e_name = c1.selectbox("Kto?", list(student_opts.keys()), key="extra_who")
        e_id = student_opts[e_name]
        
        s_row = df[df['ID'] == e_id].iloc[0]
        sched_rows = df_schedule[df_schedule['Uczen_ID'] == e_id]
        def_dur = 1.0
        if not sched_rows.empty:
            def_dur = float(sched_rows.iloc[0]['Czas_trwania'])
        else:
            try: def_dur = float(str(s_row.get('H_w_tygodniu', 1.0)).split(';')[0])
            except: pass
            
        c3, c4 = st.columns(2)
        e_date = c3.date_input("Kiedy?", date.today(), key="extra_date")
        e_time = c4.time_input("O której?", time(17,0), key="extra_time")
        c5, c6 = st.columns(2)
        e_dur = c5.number_input("Czas trwania (h)", value=float(def_dur), step=0.25, key="extra_dur")
        
        base_hourly = float(s_row['Stawka'])
        e_hourly = c6.number_input("Stawka (zł/h)", value=base_hourly, key="extra_hourly")
        
        dojazd_koszt = float(s_row.get('Dojazd', 0))
        final_total = (e_hourly * e_dur) + dojazd_koszt
        st.caption(f"ℹ️ Wyliczenie: {e_hourly} zł/h × {e_dur}h + {dojazd_koszt} zł (dojazd) = **{final_total:.2f} zł**")
        typ_lekcji_ui = st.radio("Typ:", ["Odrabianie", "Dodatkowa"], horizontal=True)
        if st.button("Dodaj lekcję"):
            typ_save = "Odrabianie" if typ_lekcji_ui == "Odrabianie" else "Dodatkowa"
            new_extra = pd.DataFrame([{
                'Uczen_ID': e_id, 'Data': e_date, 'Godzina': e_time, 
                'Stawka': final_total, 'Typ': typ_save, 'Czas': e_dur, 'Status': 'Zaplanowana'
            }])
            df_extra = pd.concat([df_extra, new_extra], ignore_index=True)
            save_extra(df_extra)
            if typ_save == "Odrabianie":
                idx = df.index[df['ID'] == e_id].tolist()
                if idx:
                    idx = idx[0]
                    df.at[idx, 'Do_odrobienia_umowione'] += e_dur
                    current_pending = df.at[idx, 'Do_odrobienia_nieumowione']
                    if current_pending > 0:
                        df.at[idx, 'Do_odrobienia_nieumowione'] = max(0.0, current_pending - e_dur)
                    save_data(df)
                    st.success(f"Dodano lekcję (Odrabianie {e_dur}h) i zaktualizowano liczniki!")
            else:
                st.success("Dodano lekcję dodatkową!")
            st.rerun()

    calendar_options = {
        "editable": "true", "locale": "pl", "firstDay": 1,
        "headerToolbar": {"left": "prev,next today", "center": "title", "right": "dayGridMonth,timeGridWeek"},
        "buttonText": {"today": "Dziś", "month": "Miesiąc", "week": "Tydzień", "day": "Dzień"},
        "slotMinTime": "08:00:00", "slotMaxTime": "22:00:00", "allDaySlot": False,
        "eventTimeFormat": {"hour": "2-digit", "minute": "2-digit", "hour12": False}
    }
    events = generate_calendar_events(df)
    cal_state = calendar(events=events, options=calendar_options)
    
    if cal_state.get("eventClick"):
        props = cal_state["eventClick"]["event"]["extendedProps"]
        st.divider()
        st.subheader(f"Zarządzanie: {props['Imie']} {props['Nazwisko']} ({props['Data']})")
        col1, col2 = st.columns(2)
        with col1:
            st.info(f"Typ: {props['Typ']} | Czas: {props.get('Czas', 1.0)}h | Stawka: {props['Stawka']} zł")
        with col2:
            tab_edit, tab_del = st.tabs(["✏️ Edytuj", "🗑️ Usuń / Odwołaj"])
            
            with tab_edit:
                c_e1, c_e2 = st.columns(2)
                new_rate = c_e1.number_input("Nowa stawka (Total)", value=float(props['Stawka']))
                new_dur = c_e2.number_input("Nowy czas (h)", value=float(props.get('Czas', 1.0)), step=0.25)
                
                if st.button("Zapisz zmiany"):
                    if props['Typ'] == 'Stała':
                        nc = pd.DataFrame([{
                            'Uczen_ID': props['Uczen_ID'], 'Data': props['Data'], 'Powod': 'Edycja (Zmiana stawki)'
                        }])
                        df_cancellations = pd.concat([df_cancellations, nc], ignore_index=True)
                        save_cancellations(df_cancellations)
                        
                        ne = pd.DataFrame([{
                            'Uczen_ID': props['Uczen_ID'], 'Data': props['Data'], 'Godzina': props['Godzina'], 
                            'Stawka': new_rate, 'Typ': 'Edytowana', 'Czas': new_dur, 'Status': 'Zaplanowana'
                        }])
                        df_extra = pd.concat([df_extra, ne], ignore_index=True)
                        save_extra(df_extra)
                    else:
                        mask = (df_extra['Uczen_ID'] == props['Uczen_ID']) & (df_extra['Data'] == props['Data']) & (df_extra['Godzina'].astype(str).str.contains(str(props['Godzina'])[:5]))
                        if mask.any():
                            idx = df_extra[mask].index[0]
                            df_extra.at[idx, 'Stawka'] = new_rate
                            df_extra.at[idx, 'Czas'] = new_dur
                            save_extra(df_extra)
                    st.success("Zapisano!"); st.rerun()

            with tab_del:
                if props['Typ'] == 'Stała':
                    powod_del = st.radio("Kto zawinił?", ["Wina Ucznia", "Wina Korepetytora", "Święto / Inne (Bez liczników)"], key="del_reason_click")
                    if st.button("❌ Odwołaj zajęcia"):
                        nc = pd.DataFrame([{'Uczen_ID': props['Uczen_ID'], 'Data': props['Data'], 'Powod': powod_del}])
                        df_cancellations = pd.concat([df_cancellations, nc], ignore_index=True)
                        save_cancellations(df_cancellations)
                        if "Święto" not in powod_del:
                            idx = df.index[df['ID'] == props['Uczen_ID']].tolist()[0]
                            duration_to_add = float(props.get('Czas', 1.0))
                            if powod_del == "Wina Ucznia":
                                df.at[idx, 'Nieobecnosci'] += 1
                                df.at[idx, 'Odrabiania'] += 1
                                df.at[idx, 'Do_odrobienia_nieumowione'] += duration_to_add
                            else:
                                df.at[idx, 'Do_odrobienia_nieumowione'] += duration_to_add
                            save_data(df)
                        st.success("Odwołano."); st.rerun()
                else:
                    if st.button("🗑️ Usuń z kalendarza"):
                        mask = (df_extra['Uczen_ID'] == props['Uczen_ID']) & (df_extra['Data'] == props['Data']) & (df_extra['Godzina'].astype(str).str.contains(str(props['Godzina'])[:5]))
                        if mask.any():
                            idx = df_extra[mask].index[0]
                            if props['Typ'] == 'Odrabianie':
                                s_idx = df.index[df['ID'] == props['Uczen_ID']].tolist()
                                if s_idx:
                                    s_idx = s_idx[0]
                                    dur_to_rev = float(props.get('Czas', 1.0))
                                    if df.at[s_idx, 'Do_odrobienia_umowione'] >= dur_to_rev:
                                        df.at[s_idx, 'Do_odrobienia_umowione'] -= dur_to_rev
                                    df.at[s_idx, 'Do_odrobienia_nieumowione'] += dur_to_rev 
                                    save_data(df)
                                    st.toast("Cofnięto status odrabiania.")
                            df_extra = df_extra.drop(idx).reset_index(drop=True)
                            save_extra(df_extra)
                        st.success("Usunięto."); st.rerun()

elif menu == "👤 Szczegóły Ucznia":
    st.header("Karta Ucznia")
    if df.empty:
        st.warning("Brak uczniów.")
    else:
        student_options = {f"{r['Imie']} {r['Nazwisko']}": r['ID'] for i, r in df.iterrows()}
        selected_student_name = st.selectbox("Wybierz ucznia:", list(student_options.keys()))
        selected_id = student_options[selected_student_name]
        student_row = df[df['ID'] == selected_id].iloc[0]
        tryb = student_row.get('Tryb_platnosci', 'Co zajęcia')
        
        st.markdown("---")
        with st.expander("📅 Historia i Zmiany Planu (Harmonogram)"):
            st.caption("Tutaj możesz zmienić dzień/godzinę zajęć w czasie.")
            s_sch = df_schedule[df_schedule['Uczen_ID'] == selected_id].copy()
            c_h1, c_h2, c_h3, c_h4, c_h5, c_h6, c_h7 = st.columns([2, 2, 1.5, 1.5, 2, 2, 1])
            new_day = c_h1.selectbox("Dzień", list(DNI_MAPA.keys()), key="ns_d")
            new_hour = c_h2.time_input("Godz", time(16,0), key="ns_t")
            new_dur = c_h3.number_input("h", 0.5, 3.0, 1.0, 0.25, key="ns_dur")
            new_rate_val = float(student_row['Stawka'])
            new_rate = c_h4.number_input("Stawka", value=new_rate_val, key="ns_rate")
            new_start = c_h5.date_input("Od", date.today(), key="ns_od")
            new_end = c_h6.date_input("Do", date(2026, 6, 26), key="ns_do")
            if c_h7.button("➕", help="Dodaj nowy okres"):
                new_sch_entry = pd.DataFrame([{
                    'Uczen_ID': selected_id, 'Dzien_tyg': new_day, 'Godzina': new_hour, 
                    'Czas_trwania': new_dur, 'Data_od': new_start, 'Data_do': new_end,
                    'Stawka': new_rate
                }])
                df_schedule = pd.concat([df_schedule, new_sch_entry], ignore_index=True)
                save_schedule(df_schedule); st.rerun()
            
            if not s_sch.empty:
                try: s_sch['Godzina'] = pd.to_datetime(s_sch['Godzina'].astype(str)).dt.time
                except: pass
                try: 
                    s_sch['Data_od'] = pd.to_datetime(s_sch['Data_od']).dt.date
                    s_sch['Data_do'] = pd.to_datetime(s_sch['Data_do']).dt.date
                except: pass

                edited_sch = st.data_editor(
                    s_sch, 
                    column_config={
                        "Uczen_ID": None,
                        "Dzien_tyg": st.column_config.SelectboxColumn("Dzień", options=list(DNI_MAPA.keys()), required=True),
                        "Godzina": st.column_config.TimeColumn("Godzina", required=True),
                        "Czas_trwania": st.column_config.NumberColumn("Czas (h)", min_value=0.5, max_value=4.0, step=0.25),
                        "Stawka": st.column_config.NumberColumn("Stawka (zł/h)", min_value=0.0, step=5.0),
                        "Data_od": st.column_config.DateColumn("Od", required=True),
                        "Data_do": st.column_config.DateColumn("Do", required=True)
                    },
                    hide_index=True, use_container_width=True, key="sch_editor"
                )
                if st.button("Zapisz zmiany w planie"):
                    df_schedule = df_schedule[df_schedule['Uczen_ID'] != selected_id]
                    df_schedule = pd.concat([df_schedule, edited_sch], ignore_index=True)
                    save_schedule(df_schedule); st.success("Plan zaktualizowany!"); st.rerun()
            else: st.warning("Brak zdefiniowanego planu.")

        st.markdown("---")
        col_info1, col_info2, col_info3 = st.columns(3)
        with col_info1:
            st.markdown("##### 📞 Dane")
            st.write(f"{student_row['Nr_tel']} | {student_row['Szkola']}")
            st.write(f"**Adres:** {student_row['Adres']}")
        with col_info2:
            st.markdown("##### 📚 Finanse")
            dojazd_info = f"+ {student_row.get('Dojazd', 0)} zł dojazd"
            st.write(f"Stawka: {student_row['Stawka']} zł/h {dojazd_info}")
        with col_info3:
            st.markdown("##### 📊 Status i Liczniki")
            c_stat1, c_stat2 = st.columns(2)
            c_stat1.metric("Nieobecności", int(student_row['Nieobecnosci']))
            c_stat1.metric("Odrabiania", int(student_row['Odrabiania']))
            c_stat2.metric("Do odrobienia (UMÓWIONE)", f"{float(student_row['Do_odrobienia_umowione']):.1f}h")
            c_stat2.metric("Do odrobienia (WISZĄCE)", f"{float(student_row['Do_odrobienia_nieumowione']):.1f}h", delta_color="inverse")

        start_date = pd.to_datetime(student_row['Data_rozp']).date()
        end_date = pd.to_datetime(student_row['Data_zak']).date()
        effective_end = min(end_date, date.today())
        saved_for_student = df_settlements[df_settlements['Uczen_ID'] == selected_id]
        if not saved_for_student.empty: saved_for_student = saved_for_student.set_index('Okres')
        
        table_data = []
        details_map = {}

        if tryb == "Miesięcznie":
            curr = start_date.replace(day=1)
            view_limit = (date.today().replace(day=1) + relativedelta(months=1)) - timedelta(days=1)
            df_extra_all = load_extra()
            while curr <= view_limit:
                m_str = curr.strftime("%Y-%m")
                calc_amount, details = calculate_monthly_breakdown(df, selected_id, curr)
                details_map[m_str] = details
                final_req = float(calc_amount)
                paid_val = 0.0
                if not saved_for_student.empty and m_str in saved_for_student.index:
                    record = saved_for_student.loc[m_str]
                    if isinstance(record, pd.DataFrame): record = record.iloc[0]
                    paid_val = float(record['Wplacono'])
                table_data.append({"ID Okresu": m_str, "Termin": f"{MIESIACE_PL.get(curr.month)} {curr.year}", "Kwota do zapłaty": float(calc_amount), "Ile wpłacono": paid_val})
                
                month_start, month_end = curr, curr + relativedelta(months=1) - timedelta(days=1)
                extras_in_month = df_extra_all[(df_extra_all['Uczen_ID'] == selected_id) & (df_extra_all['Typ'] == 'Dodatkowa') & (pd.to_datetime(df_extra_all['Data']).dt.date >= month_start) & (pd.to_datetime(df_extra_all['Data']).dt.date <= month_end)]
                for _, ex_row in extras_in_month.iterrows():
                    d_str = ex_row['Data']
                    label = f"Lekcja dodatkowa: {d_str}"
                    req = float(ex_row['Stawka'])
                    paid = 0.0
                    if not saved_for_student.empty and d_str in saved_for_student.index:
                        record = saved_for_student.loc[d_str]
                        if isinstance(record, pd.DataFrame): record = record.iloc[0]
                        paid = float(record['Wplacono'])
                    table_data.append({"ID Okresu": d_str, "Termin": label, "Kwota do zapłaty": req, "Ile wpłacono": paid})
                curr += relativedelta(months=1)
            table_data.sort(key=lambda x: x['ID Okresu'], reverse=True)
        else:
            all_lessons = get_lessons_in_period(df[df['ID'] == selected_id], start_date, effective_end)
            all_lessons.sort(key=lambda x: x['Data'], reverse=True)
            for l in all_lessons:
                d_str = l['Data'].strftime("%Y-%m-%d")
                label = f"{l['Data'].day} {MIESIACE_PL.get(l['Data'].month)} {l['Data'].year}"
                if l['Typ'] != 'Stała': label += f" ({l['Typ']})"
                paid_val = 0.0
                if not saved_for_student.empty and d_str in saved_for_student.index:
                    record = saved_for_student.loc[d_str]
                    if isinstance(record, pd.DataFrame): record = record.iloc[0]
                    paid_val = float(record['Wplacono'])
                table_data.append({"ID Okresu": d_str, "Termin": label, "Kwota do zapłaty": float(l['Stawka']), "Ile wpłacono": paid_val})

        total_req = sum(r['Kwota do zapłaty'] for r in table_data)
        total_paid = sum(r['Ile wpłacono'] for r in table_data)
        saldo = total_paid - total_req
        with col_info3:
            st.markdown("##### 💰 SALDO")
            color = "green" if saldo >= 0 else "red"
            st.markdown(f"<h2 style='color:{color}'>{saldo:+.2f} zł</h2>", unsafe_allow_html=True)

        st.subheader("💳 Rejestr wpłat")
        if not table_data: st.info("Brak danych.")
        else:
            df_disp = pd.DataFrame(table_data)
            edited = st.data_editor(df_disp, column_config={"ID Okresu": None, "Termin": st.column_config.TextColumn(disabled=True), "Kwota do zapłaty": st.column_config.NumberColumn(format="%.2f zł", disabled=True), "Ile wpłacono": st.column_config.NumberColumn(format="%.2f zł", min_value=0, step=10)}, hide_index=True, use_container_width=True, key=f"edit_{selected_id}", num_rows="fixed")
            if st.button("💾 Zapisz wpłaty", type="primary"):
                other = df_settlements[df_settlements['Uczen_ID'] != selected_id]
                new_rows = []
                for _, r in edited.iterrows():
                    new_rows.append({'Uczen_ID': selected_id, 'Okres': r['ID Okresu'], 'Kwota_Wymagana': r['Kwota do zapłaty'], 'Wplacono': r['Ile wpłacono']})
                updated = pd.concat([other, pd.DataFrame(new_rows)], ignore_index=True)
                save_settlements(updated)
                st.success("Zapisano!"); st.rerun()

        st.divider()
        st.subheader("🔍 Szczegóły wyliczeń dla miesiąca (Plan)")
        month_map = {}
        iter_date = start_date.replace(day=1)
        safe_end = end_date if end_date >= start_date else date.today() + relativedelta(years=1)
        while iter_date <= safe_end:
            m_label = f"{MIESIACE_PL.get(iter_date.month)} {iter_date.year}"
            month_map[m_label] = iter_date
            iter_date += relativedelta(months=1)
        sorted_opts = sorted(month_map.keys(), key=lambda x: month_map[x], reverse=True)
        if sorted_opts:
            sel_month_label = st.selectbox("Wybierz miesiąc do analizy:", sorted_opts)
            target_date = month_map[sel_month_label]
            calc_amount, details = calculate_monthly_breakdown(df, selected_id, target_date)
            if details:
                det_df = pd.DataFrame(details)
                st.dataframe(det_df, hide_index=True, use_container_width=True)
                sum_info = sum(d['Kwota'] for d in details)
                st.caption(f"Suma wyliczona z planu (Abonament + Dodatki): **{sum_info:.2f} zł**")
            else: st.info("Brak pozycji w rachunku.")
        else: st.info("Brak miesięcy do wyświetlenia.")

elif menu == "💰 Finanse (Wykres)":
    st.header("Analiza Finansowa")
    if df.empty: st.info("Brak danych.")
    else:
        today = date.today()
        start_year = date(today.year if today.month >= 9 else today.year - 1, 9, 1)
        end_year = date(today.year + 1 if today.month >= 9 else today.year, 6, 30)
        
        # income_total = calculate_predicted_income(df, start_year, end_year)
        # Using calculate_predicted_income now for prediction consistency
        income_total = calculate_predicted_income(df, start_year, end_year)
        
        paid_total = df_settlements['Wplacono'].sum()
        c1, c2 = st.columns(2)
        c1.metric("Przychód Przewidywany (Rok)", f"{income_total:.2f} PLN")
        c2.metric("Rzeczywiście Wpłacono (Total)", f"{paid_total:.2f} PLN")
        
        real_income_map = {}
        if not df_settlements.empty:
            temp_settle = df_settlements.copy()
            temp_settle['MonthKey'] = temp_settle['Okres'].astype(str).str.slice(0, 7)
            real_income_map = temp_settle.groupby('MonthKey')['Wplacono'].sum().to_dict()

        chart_data = []
        curr = start_year
        while curr <= end_year:
            nm = curr + relativedelta(months=1)
            e_m = nm - timedelta(days=1)
            month_key = curr.strftime("%Y-%m")
            
            # Using calculate_predicted_income for charts as well
            val_pred = calculate_predicted_income(df, curr, e_m)
            
            val_real = real_income_map.get(month_key, 0.0)
            label = f"{MIESIACE_PL.get(curr.month)} {curr.year}"
            chart_data.append({"Miesiąc": label, "Przewidywany": val_pred, "Rzeczywisty": val_real, "SortKey": month_key})
            curr = nm
            
        st.divider()
        st.subheader("Porównanie: Plan vs Rzeczywistość (Miesiącami)")
        df_chart = pd.DataFrame(chart_data)
        df_melted = df_chart.melt(id_vars=['Miesiąc', 'SortKey'], value_vars=['Przewidywany', 'Rzeczywisty'], var_name='Typ', value_name='Kwota')
        c = alt.Chart(df_melted).mark_bar().encode(x=alt.X('Miesiąc:N', sort=alt.SortField(field='SortKey', order='ascending'), axis=alt.Axis(title=None, labelAngle=-45)), y=alt.Y('Kwota:Q', title='Kwota (PLN)'), color=alt.Color('Typ:N', scale=alt.Scale(domain=['Przewidywany', 'Rzeczywisty'], range=['#a0c4ff', '#28a745']), legend=alt.Legend(title=None, orient='top')), xOffset='Typ:N', tooltip=['Miesiąc', 'Typ', 'Kwota']).configure_view(stroke='transparent')
        st.altair_chart(c, use_container_width=True)

        st.divider()
        st.subheader("📊 Raport Miesięczny")
        months_options = [d['Miesiąc'] for d in chart_data]
        if months_options:
            curr_month_str = f"{MIESIACE_PL[today.month]} {today.year}"
            def_idx = months_options.index(curr_month_str) if curr_month_str in months_options else 0
            sel_month_report = st.selectbox("Wybierz miesiąc do analizy:", months_options, index=def_idx)
            target_report_date = None
            m_ptr = start_year
            while m_ptr <= end_year:
                if f"{MIESIACE_PL[m_ptr.month]} {m_ptr.year}" == sel_month_report:
                    target_report_date = m_ptr
                    break
                m_ptr += relativedelta(months=1)
                
            if target_report_date:
                r_start = target_report_date.replace(day=1)
                r_end = r_start + relativedelta(months=1) - timedelta(days=1)
                
                # Use get_predicted_lessons for Plan report
                lessons_report = get_predicted_lessons(df, r_start, r_end)
                
                plan_total, plan_monthly, plan_single, plan_tuition, plan_travel = 0, 0, 0, 0, 0
                student_plan_total, student_plan_travel = {}, {}
                
                for l in lessons_report:
                    amt = float(l['Stawka'])
                    sid = l['Uczen_ID']
                    s_row = df[df['ID'] == sid].iloc[0]
                    mode = s_row.get('Tryb_platnosci', 'Co zajęcia')
                    travel_unit_cost = float(s_row.get('Dojazd', 0))
                    curr_travel = travel_unit_cost
                    curr_tuition = amt - travel_unit_cost
                    if curr_tuition < 0: curr_travel = amt; curr_tuition = 0
                    plan_total += amt; plan_travel += curr_travel; plan_tuition += curr_tuition
                    lesson_type = l.get('Typ', 'Stała')
                    if mode == 'Miesięcznie' and lesson_type != 'Dodatkowa': plan_monthly += amt
                    else: plan_single += amt
                    student_plan_total[sid] = student_plan_total.get(sid, 0) + amt
                    student_plan_travel[sid] = student_plan_travel.get(sid, 0) + curr_travel
                
                st.markdown("#### 🔵 PLAN (Przewidywane)")
                c1, c2, c3, c4 = st.columns(4)
                c1.metric("Suma", f"{plan_total:.2f} zł")
                c2.metric("Edukacja", f"{plan_tuition:.2f} zł")
                c3.metric("Dojazdy", f"{plan_travel:.2f} zł")
                c4.caption(f"Abonamenty: {plan_monthly:.2f}\nPojedyncze: {plan_single:.2f}")

                real_total, real_monthly, real_single, real_tuition, real_travel = 0, 0, 0, 0, 0
                month_prefix = target_report_date.strftime("%Y-%m")
                if not df_settlements.empty:
                    df_settlements['OkresStr'] = df_settlements['Okres'].astype(str)
                    real_recs = df_settlements[df_settlements['OkresStr'].str.startswith(month_prefix)]
                    for _, row in real_recs.iterrows():
                        paid = float(row['Wplacono'])
                        if paid > 0:
                            sid = row['Uczen_ID']
                            real_total += paid
                            s_row = df[df['ID'] == sid]
                            if not s_row.empty:
                                s_row = s_row.iloc[0]
                                mode = s_row.get('Tryb_platnosci', 'Co zajęcia')
                                is_extra = len(str(row['Okres'])) > 7
                                if mode == 'Miesięcznie' and not is_extra: real_monthly += paid
                                else: real_single += paid
                                p_tot = student_plan_total.get(sid, 0)
                                p_trav = student_plan_travel.get(sid, 0)
                                if p_tot > 0:
                                    ratio = min(paid / p_tot, 1.0)
                                    est_travel = p_trav * ratio
                                    real_travel += est_travel
                                    real_tuition += (paid - est_travel)
                                else: real_tuition += paid

                st.markdown("#### 🟢 RZECZYWISTOŚĆ (Wpłacone)")
                r1, r2, r3, r4 = st.columns(4)
                r1.metric("Suma", f"{real_total:.2f} zł", delta=f"{real_total - plan_total:.2f} zł")
                r2.metric("Edukacja", f"{real_tuition:.2f} zł")
                r3.metric("Dojazdy", f"{real_travel:.2f} zł")
                r4.caption(f"Abonamenty: {real_monthly:.2f}\nPojedyncze: {real_single:.2f}")
        else: st.info("Brak danych.")

        st.divider()
        st.subheader("📊 Raport Kwartalny")
        quarter_options = []
        q_ptr = start_year
        while q_ptr <= end_year:
            curr_q = (q_ptr.month - 1) // 3 + 1
            label_q = f"Q{curr_q} {q_ptr.year}"
            if not quarter_options or quarter_options[-1]['Label'] != label_q:
                q_start_month = (curr_q - 1) * 3 + 1
                q_start_date = date(q_ptr.year, q_start_month, 1)
                q_end_date = q_start_date + relativedelta(months=3) - timedelta(days=1)
                quarter_options.append({'Label': label_q, 'Start': q_start_date, 'End': q_end_date})
            q_ptr += relativedelta(months=1)
        q_labels = [q['Label'] for q in quarter_options]
        if q_labels:
            curr_q_label = f"Q{(today.month - 1) // 3 + 1} {today.year}"
            def_q_idx = q_labels.index(curr_q_label) if curr_q_label in q_labels else 0
            sel_q_label = st.selectbox("Wybierz kwartał:", q_labels, index=def_q_idx)
            sel_q_data = next(q for q in quarter_options if q['Label'] == sel_q_label)
            if sel_q_data:
                q_start, q_end = sel_q_data['Start'], sel_q_data['End']
                
                # Use get_predicted_lessons for Plan report
                q_lessons_report = get_predicted_lessons(df, q_start, q_end)
                
                q_plan_total, q_plan_monthly, q_plan_single, q_plan_tuition, q_plan_travel = 0, 0, 0, 0, 0
                q_student_plan_total, q_student_plan_travel = {}, {}
                for l in q_lessons_report:
                    amt = float(l['Stawka'])
                    sid = l['Uczen_ID']
                    s_row = df[df['ID'] == sid].iloc[0]
                    mode = s_row.get('Tryb_platnosci', 'Co zajęcia')
                    travel_unit_cost = float(s_row.get('Dojazd', 0))
                    curr_travel = travel_unit_cost
                    curr_tuition = amt - travel_unit_cost
                    if curr_tuition < 0: curr_travel = amt; curr_tuition = 0
                    q_plan_total += amt; q_plan_travel += curr_travel; q_plan_tuition += curr_tuition
                    lesson_type = l.get('Typ', 'Stała')
                    if mode == 'Miesięcznie' and lesson_type != 'Dodatkowa': q_plan_monthly += amt
                    else: q_plan_single += amt
                    q_student_plan_total[sid] = q_student_plan_total.get(sid, 0) + amt
                    q_student_plan_travel[sid] = q_student_plan_travel.get(sid, 0) + curr_travel

                st.markdown("#### 🔵 PLAN KWARTALNY (Przewidywane)")
                qc1, qc2, qc3, qc4 = st.columns(4)
                qc1.metric("Suma", f"{q_plan_total:.2f} zł")
                qc2.metric("Edukacja", f"{q_plan_tuition:.2f} zł")
                qc3.metric("Dojazdy", f"{q_plan_travel:.2f} zł")
                qc4.caption(f"Abonamenty: {q_plan_monthly:.2f}\nPojedyncze: {q_plan_single:.2f}")
                
                q_real_total, q_real_monthly, q_real_single, q_real_tuition, q_real_travel = 0, 0, 0, 0, 0
                if not df_settlements.empty:
                    q_months_prefixes = []
                    iter_m = q_start
                    while iter_m <= q_end:
                        q_months_prefixes.append(iter_m.strftime("%Y-%m"))
                        iter_m += relativedelta(months=1)
                    df_settlements['OkresStr'] = df_settlements['Okres'].astype(str)
                    q_real_recs = df_settlements[df_settlements['OkresStr'].str.slice(0, 7).isin(q_months_prefixes)]
                    for _, row in q_real_recs.iterrows():
                        paid = float(row['Wplacono'])
                        if paid > 0:
                            sid = row['Uczen_ID']
                            q_real_total += paid
                            s_row = df[df['ID'] == sid]
                            if not s_row.empty:
                                s_row = s_row.iloc[0]
                                mode = s_row.get('Tryb_platnosci', 'Co zajęcia')
                                is_extra = len(str(row['Okres'])) > 7
                                if mode == 'Miesięcznie' and not is_extra: q_real_monthly += paid
                                else: q_real_single += paid
                                p_tot = q_student_plan_total.get(sid, 0)
                                p_trav = q_student_plan_travel.get(sid, 0)
                                if p_tot > 0:
                                    ratio = min(paid / p_tot, 1.0)
                                    est_travel = p_trav * ratio
                                    q_real_travel += est_travel
                                    q_real_tuition += (paid - est_travel)
                                else: q_real_tuition += paid

                st.markdown("#### 🟢 RZECZYWISTOŚĆ KWARTALNA (Wpłacone)")
                qr1, qr2, qr3, qr4 = st.columns(4)
                qr1.metric("Suma", f"{q_real_total:.2f} zł", delta=f"{q_real_total - q_plan_total:.2f} zł")
                qr2.metric("Edukacja", f"{q_real_tuition:.2f} zł")
                qr3.metric("Dojazdy", f"{q_real_travel:.2f} zł")
                qr4.caption(f"Abonamenty: {q_real_monthly:.2f}\nPojedyncze: {q_real_single:.2f}")
        else: st.info("Brak danych.")

elif menu == "➕ Dodaj Ucznia":
    st.header("Dodaj nowego ucznia")
    use_t2 = st.checkbox("Dodaj Termin 2")
    with st.form("add"):
        c1, c2 = st.columns(2)
        imie, nazwisko = c1.text_input("Imię"), c2.text_input("Nazwisko")
        c_cont1, c_cont2 = st.columns(2)
        nr_tel, adres = c_cont1.text_input("Numer telefonu"), c_cont2.text_input("Adres")
        c_sch1, c_sch2, c_sch3 = st.columns(3)
        szkola, klasa, poziom = c_sch1.selectbox("Szkoła", ["Podstawowa", "Liceum", "Technikum"]), c_sch2.text_input("Klasa"), c_sch3.selectbox("Poziom", ["Podstawowy", "Rozszerzony"])
        st.markdown("---")
        c3, c4, c5 = st.columns(3)
        data_rozp, data_zak = c3.date_input("Start", date.today()), c4.date_input("Koniec", date(2026, 6, 26))
        tryb = c5.selectbox("Tryb płatności", ["Co zajęcia", "Miesięcznie"])
        st.markdown("---"); st.caption("Terminy zajęć")
        col_t1, col_t2 = st.columns(2)
        with col_t1:
            d1 = st.selectbox("Dzień 1", list(DNI_MAPA.keys()), key="d1")
            g1 = st.time_input("Godz 1", time(16,0), key="g1")
            len1 = st.number_input("Czas 1 (h)", 1.0, 3.0, 1.0, 0.5, key="l1")
        with col_t2:
            d2 = st.selectbox("Dzień 2", list(DNI_MAPA.keys()), key="d2", disabled=not use_t2)
            g2 = st.time_input("Godz 2", time(16,0), key="g2", disabled=not use_t2)
            len2 = st.number_input("Czas 2 (h)", 1.0, 3.0, 1.0, 0.5, key="l2", disabled=not use_t2)
        c8, c9 = st.columns(2)
        stawka, dojazd = c8.number_input("Stawka za godzinę", value=50), c9.number_input("Dojazd (zł)", value=0)
        if st.form_submit_button("Zapisz"):
            if not imie or not nazwisko: st.error("Imię i Nazwisko są wymagane!")
            else:
                days_str, times_str, lens_str = d1, str(g1), str(len1)
                if use_t2: days_str += f";{d2}"; times_str += f";{g2}"; lens_str += f";{len2}"
                new_id = 1 if df.empty else df['ID'].max() + 1
                new_row = {'ID': new_id, 'Imie': imie, 'Nazwisko': nazwisko, 'Dzien_tyg': days_str, 'Godzina': times_str, 'Data_rozp': data_rozp, 'Data_zak': data_zak, 'Stawka': stawka, 'Dojazd': dojazd, 'H_w_tygodniu': lens_str, 'Nieobecnosci': 0, 'Tryb_platnosci': tryb, 'Odrabiania': 0, 'Do_odrobienia_umowione': 0, 'Do_odrobienia_nieumowione': 0, 'Szkola': szkola, 'Klasa': klasa, 'Poziom': poziom, 'Nr_tel': nr_tel, 'Adres': adres}
                df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
                save_data(df); st.success("Dodano!"); 
                new_sch_rows = []
                terms = parse_student_terms(pd.Series(new_row))
                for t in terms:
                    new_sch_rows.append({'Uczen_ID': new_id, 'Dzien_tyg': t['day_name'], 'Godzina': t['time_str'], 'Czas_trwania': t['duration'], 'Data_od': data_rozp, 'Data_do': data_zak, 'Stawka': stawka})
                if new_sch_rows:
                    df_schedule_new = pd.DataFrame(new_sch_rows)
                    df_sch_curr = load_schedule()
                    df_sch_final = pd.concat([df_sch_curr, df_schedule_new], ignore_index=True)
                    save_schedule(df_sch_final)
                st.rerun()

elif menu == "📋 Baza Danych":
    st.header("Podgląd i edycja plików CSV")
    edited = st.data_editor(df, num_rows="dynamic", key="edit_db_students")
    if st.button("Zapisz Uczniów"): save_data(edited); st.success("Zapisano!")
    st.divider()
    c1, c2 = st.columns(2)
    with c1:
        st.caption("Odwołane")
        if not df_cancellations.empty:
            if st.button("Zapisz Odwołane"): save_cancellations(st.data_editor(df_cancellations, num_rows="dynamic"))
    with c2:
        st.caption("Dodatkowe")
        if not df_extra.empty:
            if st.button("Zapisz Dodatkowe"): save_extra(st.data_editor(df_extra, num_rows="dynamic"))