import streamlit as st
import pandas as pd
import os
import altair as alt
from datetime import datetime, timedelta, date, time
from dateutil.relativedelta import relativedelta
from streamlit_calendar import calendar

# --- KONFIGURACJA STRONY ---
st.set_page_config(page_title="Menedżer Korepetycji", layout="wide", page_icon="📚")

# --- PLIKI DANYCH ---
FILE_DB = 'uczniowie.csv'
FILE_SETTLEMENTS = 'rozliczenia.csv'
FILE_CANCELLATIONS = 'odwolane.csv'
FILE_EXTRA = 'dodatkowe.csv'

# Definicja kolumn
COLUMNS = [
    'ID', 'Imie', 'Nazwisko', 'H_w_tygodniu', 'Stawka', 
    'Nieobecnosci', 'Odrabiania', 'Do_odrobienia_umowione', 'Do_odrobienia_nieumowione',
    'Szkola', 'Klasa', 'Poziom', 'Nr_tel', 
    'Data_rozp', 'Data_zak', 'Dzien_tyg', 'Godzina', 'Adres',
    'Tryb_platnosci'
]
COLUMNS_SETTLEMENTS = ['Uczen_ID', 'Okres', 'Kwota', 'Zaplacono']
COLUMNS_CANCELLATIONS = ['Uczen_ID', 'Data']
# Dodajemy kolumnę 'Typ' żeby wiedzieć czy to odrabianie przy usuwaniu
COLUMNS_EXTRA = ['Uczen_ID', 'Data', 'Godzina', 'Stawka', 'Typ']

# Mapowanie dni tygodnia
DNI_MAPA = {
    "Poniedziałek": 0, "Wtorek": 1, "Środa": 2, "Czwartek": 3, 
    "Piątek": 4, "Sobota": 5, "Niedziela": 6
}
MIESIACE_PL = {
    1: 'Styczeń', 2: 'Luty', 3: 'Marzec', 4: 'Kwiecień', 5: 'Maj', 6: 'Czerwiec',
    7: 'Lipiec', 8: 'Sierpień', 9: 'Wrzesień', 10: 'Październik', 11: 'Listopad', 12: 'Grudzień'
}

# --- FUNKCJE ŁADOWANIA DANYCH ---
def load_data():
    if not os.path.exists(FILE_DB): return pd.DataFrame(columns=COLUMNS)
    try:
        df = pd.read_csv(FILE_DB)
        if 'Tryb_platnosci' not in df.columns: df['Tryb_platnosci'] = 'Co zajęcia'
        df['Stawka'] = pd.to_numeric(df['Stawka'], errors='coerce').fillna(0)
        df['Odrabiania'] = pd.to_numeric(df['Odrabiania'], errors='coerce').fillna(0)
        # Konwersja NaN na puste stringi dla pól tekstowych żeby nie wyświetlało "nan"
        text_cols = ['Szkola', 'Klasa', 'Poziom', 'Nr_tel', 'Adres']
        for col in text_cols:
            if col in df.columns:
                df[col] = df[col].fillna("-")
        return df
    except: return pd.DataFrame(columns=COLUMNS)

def save_data(df): df.to_csv(FILE_DB, index=False)

def load_settlements():
    if not os.path.exists(FILE_SETTLEMENTS): return pd.DataFrame(columns=COLUMNS_SETTLEMENTS)
    try:
        df = pd.read_csv(FILE_SETTLEMENTS)
        if 'Miesiac' in df.columns and 'Okres' not in df.columns: df.rename(columns={'Miesiac': 'Okres'}, inplace=True)
        return df
    except: return pd.DataFrame(columns=COLUMNS_SETTLEMENTS)

def save_settlements(df): df.to_csv(FILE_SETTLEMENTS, index=False)

def load_cancellations():
    if not os.path.exists(FILE_CANCELLATIONS): return pd.DataFrame(columns=COLUMNS_CANCELLATIONS)
    return pd.read_csv(FILE_CANCELLATIONS)

def save_cancellations(df): df.to_csv(FILE_CANCELLATIONS, index=False)

def load_extra():
    if not os.path.exists(FILE_EXTRA): return pd.DataFrame(columns=COLUMNS_EXTRA)
    try:
        df = pd.read_csv(FILE_EXTRA)
        # Migracja dla starych plików bez kolumny Typ
        if 'Typ' not in df.columns:
            df['Typ'] = 'Dodatkowa'
        return df
    except: return pd.DataFrame(columns=COLUMNS_EXTRA)

def save_extra(df): df.to_csv(FILE_EXTRA, index=False)

# --- GŁÓWNA LOGIKA KALENDARZA I FINANSÓW ---

def get_lessons_in_period(df_students, start_date, end_date):
    lessons = []
    df_cancel = load_cancellations()
    df_extra = load_extra()
    
    cancelled_set = set()
    if not df_cancel.empty:
        for _, row in df_cancel.iterrows():
            cancelled_set.add((row['Uczen_ID'], str(row['Data'])))

    # 1. Generowanie lekcji stałych
    current_day = start_date
    while current_day <= end_date:
        weekday_num = current_day.weekday()
        for _, row in df_students.iterrows():
            uczen_dzien = row['Dzien_tyg']
            if uczen_dzien in DNI_MAPA and DNI_MAPA[uczen_dzien] == weekday_num:
                try:
                    c_start = pd.to_datetime(row['Data_rozp']).date()
                    c_end = pd.to_datetime(row['Data_zak']).date()
                    if c_start <= current_day <= c_end:
                        if (row['ID'], str(current_day)) not in cancelled_set:
                            lessons.append({
                                'Data': current_day,
                                'Uczen_ID': row['ID'],
                                'Stawka': row['Stawka'],
                                'Godzina': row['Godzina'],
                                'Imie': row['Imie'],
                                'Nazwisko': row['Nazwisko'],
                                'Typ': 'Stała'
                            })
                except: pass
        current_day += timedelta(days=1)

    # 2. Dodawanie lekcji dodatkowych
    if not df_extra.empty:
        for _, row in df_extra.iterrows():
            try:
                l_date = pd.to_datetime(row['Data']).date()
                if start_date <= l_date <= end_date:
                    student = df_students[df_students['ID'] == row['Uczen_ID']]
                    if not student.empty:
                        s_row = student.iloc[0]
                        # Rozróżnienie koloru w zależności od typu
                        typ_lekcji = row.get('Typ', 'Dodatkowa')
                        lessons.append({
                            'Data': l_date,
                            'Uczen_ID': row['Uczen_ID'],
                            'Stawka': row['Stawka'],
                            'Godzina': row['Godzina'],
                            'Imie': s_row['Imie'],
                            'Nazwisko': s_row['Nazwisko'],
                            'Typ': typ_lekcji
                        })
            except: pass
            
    return lessons

def calculate_income(df_students, start_date, end_date):
    lessons = get_lessons_in_period(df_students, start_date, end_date)
    return sum(l['Stawka'] for l in lessons)

def generate_calendar_events(df_students):
    today = date.today()
    end_date = today + timedelta(days=60)
    start_date = today - timedelta(days=7)
    
    lessons = get_lessons_in_period(df_students, start_date, end_date)
    events = []
    
    for l in lessons:
        try:
            godzina_str = str(l['Godzina'])
            if len(godzina_str.split(':')) == 2: godzina_str += ":00"
            start_time = datetime.combine(l['Data'], datetime.strptime(godzina_str, "%H:%M:%S").time())
            end_time = start_time + timedelta(hours=1)
            
            # Kolory: Stała=Niebieski, Dodatkowa=Zielony, Odrabianie=Pomarańczowy
            color = "#3788d8"
            if l['Typ'] == 'Dodatkowa': color = "#28a745"
            elif l['Typ'] == 'Odrabianie': color = "#fd7e14"
            elif l['Typ'] == 'Przełożona': color = "#6f42c1" # Fioletowy dla przełożonych
            
            events.append({
                "title": f"{l['Imie']} {l['Nazwisko']}",
                "start": start_time.isoformat(),
                "end": end_time.isoformat(),
                "backgroundColor": color,
                "borderColor": color,
                # --- WAŻNE: Dodajemy metadane do kafelka, aby obsłużyć kliknięcie ---
                "extendedProps": {
                    "Uczen_ID": l['Uczen_ID'],
                    "Typ": l['Typ'],
                    "Data": l['Data'].strftime("%Y-%m-%d"),
                    "Godzina": str(l['Godzina']),
                    "Stawka": l['Stawka'],
                    "Imie": l['Imie'],
                    "Nazwisko": l['Nazwisko']
                }
            })
        except: pass
    return events

# --- START APLIKACJI ---
df = load_data()
df_settlements = load_settlements()
df_cancellations = load_cancellations()
df_extra = load_extra()

with st.sidebar:
    st.title("📚 Korepetycje")
    menu = st.radio("Menu", ["📅 Kalendarz", "👤 Szczegóły Ucznia", "💰 Finanse (Wykres)", "➕ Dodaj Ucznia", "📋 Baza Danych"])

# --- ZAKŁADKA KALENDARZ ---
if menu == "📅 Kalendarz":
    st.header("Grafik Zajęć")
    
    # Zarządzanie terminami (Odwoływanie / Dodawanie) - Ukrywamy w expanderze, bo teraz mamy klikanie
    with st.expander("🛠️ Ręczne dodawanie / odwoływanie (Opcje zaawansowane)"):
        tab1, tab2 = st.tabs(["❌ Odwołaj lekcję", "➕ Dodaj lekcję (Odrabianie)"])
        
        student_opts = {f"{r['Imie']} {r['Nazwisko']}": r['ID'] for i, r in df.iterrows()}
        
        with tab1:
            st.caption("Odwołanie lekcji spowoduje usunięcie jej z kalendarza i wyliczeń finansowych.")
            c1, c2 = st.columns(2)
            s_name = c1.selectbox("Kto odwołuje?", list(student_opts.keys()), key="cancel_who")
            s_id = student_opts[s_name]
            cancel_date = c2.date_input("Który dzień?", date.today(), key="cancel_date")
            
            if st.button("Zatwierdź odwołanie"):
                new_cancel = pd.DataFrame([{'Uczen_ID': s_id, 'Data': cancel_date}])
                df_cancellations = pd.concat([df_cancellations, new_cancel], ignore_index=True)
                save_cancellations(df_cancellations)
                st.success(f"Odwołano zajęcia dla {s_name} w dniu {cancel_date}")
                st.rerun()

        with tab2:
            st.caption("Dodaj lekcję w innym terminie (Odrabianie lub Dodatkowa płatna).")
            c1, c2, c3, c4 = st.columns(4)
            e_name = c1.selectbox("Kto?", list(student_opts.keys()), key="extra_who")
            e_id = student_opts[e_name]
            e_date = c2.date_input("Kiedy?", date.today(), key="extra_date")
            e_time = c3.time_input("O której?", time(17,0), key="extra_time")
            
            def_rate = df[df['ID'] == e_id].iloc[0]['Stawka']
            e_rate = c4.number_input("Stawka", value=float(def_rate), key="extra_rate")
            
            typ_lekcji_ui = st.radio("Typ lekcji:", ["Odrabianie (Zwiększ licznik)", "Dodatkowa (Ekstra płatna)"], horizontal=True)
            
            if st.button("Dodaj lekcję"):
                typ_save = "Odrabianie" if "Odrabianie" in typ_lekcji_ui else "Dodatkowa"
                new_extra = pd.DataFrame([{
                    'Uczen_ID': e_id, 'Data': e_date, 'Godzina': e_time, 'Stawka': e_rate, 'Typ': typ_save
                }])
                df_extra = pd.concat([df_extra, new_extra], ignore_index=True)
                save_extra(df_extra)
                
                if typ_save == "Odrabianie":
                    idx = df.index[df['ID'] == e_id].tolist()
                    if idx:
                        curr_odr = df.at[idx[0], 'Odrabiania']
                        df.at[idx[0], 'Odrabiania'] = curr_odr + 1
                        save_data(df)
                        st.success("Dodano lekcję i zwiększono licznik odrabiania!")
                else:
                    st.success("Dodano dodatkową lekcję!")
                st.rerun()

    # --- KALENDARZ Z OBSŁUGĄ KLIKNIĘĆ ---
    calendar_options = {
        "editable": "true", "locale": "pl", "firstDay": 1,
        "headerToolbar": {"left": "prev,next today", "center": "title", "right": "dayGridMonth,timeGridWeek"},
        "buttonText": {"today": "Dziś", "month": "Miesiąc", "week": "Tydzień", "day": "Dzień"},
        "slotMinTime": "08:00:00", "slotMaxTime": "22:00:00", "allDaySlot": False,
        "eventTimeFormat": {"hour": "2-digit", "minute": "2-digit", "hour12": False}
    }
    
    events = generate_calendar_events(df)
    
    # Przechwytujemy stan kalendarza (kliknięcia)
    cal_state = calendar(events=events, options=calendar_options)
    
    # --- OBSŁUGA KLIKNIĘCIA W KAFELEK ---
    if cal_state.get("eventClick"):
        event_info = cal_state["eventClick"]["event"]
        props = event_info["extendedProps"]
        
        # Wyświetlamy panel zarządzania pod kalendarzem
        st.divider()
        st.subheader("⚡ Zarządzanie wybranym spotkaniem")
        
        col_sel1, col_sel2 = st.columns([1, 2])
        
        with col_sel1:
            st.info(f"**Uczeń:** {props['Imie']} {props['Nazwisko']}\n\n"
                    f"📅 **Data:** {props['Data']}\n\n"
                    f"🕒 **Godzina:** {props['Godzina']}\n\n"
                    f"🏷️ **Typ:** {props['Typ']}")
        
        with col_sel2:
            tab_move, tab_del = st.tabs(["📅 Przełóż Termin", "🗑️ Usuń / Odwołaj"])
            
            # --- ZAKŁADKA PRZEKŁADANIA ---
            with tab_move:
                st.write("Wybierz nowy termin dla tego spotkania:")
                c_new1, c_new2 = st.columns(2)
                new_date = c_new1.date_input("Nowa data", pd.to_datetime(props['Data']).date())
                
                # Parsowanie starej godziny
                old_time_obj = datetime.strptime(props['Godzina'][:5], "%H:%M").time()
                new_time = c_new2.time_input("Nowa godzina", old_time_obj)
                
                if st.button("Zatwierdź zmianę terminu", type="primary"):
                    # 1. Jeśli to lekcja STAŁA: Odwołujemy starą, dodajemy nową
                    if props['Typ'] == 'Stała':
                        # Odwołanie starej
                        new_cancel = pd.DataFrame([{'Uczen_ID': props['Uczen_ID'], 'Data': props['Data']}])
                        df_cancellations = pd.concat([df_cancellations, new_cancel], ignore_index=True)
                        save_cancellations(df_cancellations)
                        
                        # Dodanie nowej (jako Przełożona)
                        new_extra = pd.DataFrame([{
                            'Uczen_ID': props['Uczen_ID'], 'Data': new_date, 'Godzina': new_time, 
                            'Stawka': props['Stawka'], 'Typ': 'Przełożona'
                        }])
                        df_extra = pd.concat([df_extra, new_extra], ignore_index=True)
                        save_extra(df_extra)
                        
                    # 2. Jeśli to lekcja DODATKOWA/ODRABIANA/PRZEŁOŻONA: Edytujemy wpis
                    else:
                        # Szukamy tego wpisu w df_extra
                        mask = (df_extra['Uczen_ID'] == props['Uczen_ID']) & \
                               (df_extra['Data'] == props['Data']) & \
                               (df_extra['Godzina'].astype(str).str.contains(str(props['Godzina'])[:5]))
                        
                        if mask.any():
                            idx = df_extra[mask].index[0]
                            df_extra.at[idx, 'Data'] = new_date
                            df_extra.at[idx, 'Godzina'] = new_time
                            save_extra(df_extra)
                        else:
                            st.error("Nie znaleziono wpisu w bazie do edycji.")
                            
                    st.success("Termin został zmieniony!")
                    st.rerun()

            # --- ZAKŁADKA USUWANIA ---
            with tab_del:
                st.warning("Ta operacja jest nieodwracalna.")
                
                if props['Typ'] == 'Stała':
                    if st.button("❌ Odwołaj zajęcia (Tylko te jedne)"):
                        new_cancel = pd.DataFrame([{'Uczen_ID': props['Uczen_ID'], 'Data': props['Data']}])
                        df_cancellations = pd.concat([df_cancellations, new_cancel], ignore_index=True)
                        save_cancellations(df_cancellations)
                        st.success("Zajęcia odwołane.")
                        st.rerun()
                else:
                    if st.button("🗑️ Usuń całkowicie z kalendarza"):
                        # Szukamy i usuwamy
                        mask = (df_extra['Uczen_ID'] == props['Uczen_ID']) & \
                               (df_extra['Data'] == props['Data']) & \
                               (df_extra['Godzina'].astype(str).str.contains(str(props['Godzina'])[:5]))
                        
                        if mask.any():
                            idx = df_extra[mask].index[0]
                            # Logika cofania licznika jeśli to Odrabianie
                            if props['Typ'] == 'Odrabianie':
                                s_idx = df.index[df['ID'] == props['Uczen_ID']].tolist()
                                if s_idx:
                                    curr = df.at[s_idx[0], 'Odrabiania']
                                    if curr > 0:
                                        df.at[s_idx[0], 'Odrabiania'] = curr - 1
                                        save_data(df)
                                        st.toast("Cofnięto licznik odrabiania.")
                            
                            df_extra = df_extra.drop(idx).reset_index(drop=True)
                            save_extra(df_extra)
                            st.success("Usunięto.")
                            st.rerun()
                        else:
                            st.error("Nie znaleziono wpisu.")

# --- ZAKŁADKA SZCZEGÓŁY UCZNIA ---
elif menu == "👤 Szczegóły Ucznia":
    st.header("Karta Ucznia")
    if df.empty:
        st.warning("Brak uczniów w bazie. Dodaj kogoś najpierw!")
    else:
        # Wybór ucznia
        student_options = {f"{r['Imie']} {r['Nazwisko']}": r['ID'] for i, r in df.iterrows()}
        selected_student_name = st.selectbox("Wybierz ucznia:", list(student_options.keys()))
        selected_id = student_options[selected_student_name]
        
        # Pobieranie danych ucznia
        student_row = df[df['ID'] == selected_id].iloc[0]
        tryb = student_row.get('Tryb_platnosci', 'Co zajęcia')
        
        # --- SEKCJA INFORMACYJNA (NOWOŚĆ) ---
        st.markdown("---")
        col_info1, col_info2, col_info3 = st.columns(3)
        
        with col_info1:
            st.markdown("##### 📞 Kontakt i Szkoła")
            st.write(f"**Telefon:** {student_row['Nr_tel']}")
            st.write(f"**Adres:** {student_row['Adres']}")
            st.write(f"**Szkoła:** {student_row['Szkola']}")
            st.write(f"**Klasa:** {student_row['Klasa']} | **Poziom:** {student_row['Poziom']}")

        with col_info2:
            st.markdown("##### 📚 Warunki Kursu")
            st.write(f"**Termin:** {student_row['Dzien_tyg']} {student_row['Godzina']}")
            st.write(f"**Stawka:** {student_row['Stawka']} zł")
            st.write(f"**Okres:** {student_row['Data_rozp']} ➡ {student_row['Data_zak']}")
            st.write(f"**Płatność:** {tryb}")
            
        with col_info3:
            st.markdown("##### 📊 Status")
            st.write(f"**Liczba godzin/tydz:** {student_row['H_w_tygodniu']}")
            st.metric("Licznik odrabiania", int(student_row['Odrabiania']))
            # Można tu też wyświetlić nieobecności jeśli będziemy je zliczać w przyszłości
            # st.metric("Nieobecności", int(student_row['Nieobecnosci']))

        st.markdown("---")
        
        # --- SEKCJA ROZLICZEŃ (TABELA) ---
        st.subheader("💳 Rozliczenia i Płatności")
        
        start_date = pd.to_datetime(student_row['Data_rozp']).date()
        end_date = pd.to_datetime(student_row['Data_zak']).date()
        max_date = date.today() + relativedelta(months=6) 
        effective_end = min(end_date, max_date)
        
        saved_for_student = df_settlements[df_settlements['Uczen_ID'] == selected_id]
        if not saved_for_student.empty:
            saved_for_student = saved_for_student.set_index('Okres')
        
        table_data = []

        if tryb == "Miesięcznie":
            curr = start_date.replace(day=1)
            while curr <= effective_end:
                m_str = curr.strftime("%Y-%m")
                # Oblicz zakres miesiąca
                y, m_num = curr.year, curr.month
                m_start = date(y, m_num, 1)
                m_end = m_start + relativedelta(months=1) - timedelta(days=1)
                
                # Oblicz kwotę z uwzględnieniem odwołań i dodatkowych
                calc_amount = calculate_income(df[df['ID'] == selected_id], m_start, m_end)
                
                # Sprawdź zapisane
                is_paid = False
                final_amount = float(calc_amount)
                
                if not saved_for_student.empty and m_str in saved_for_student.index:
                    record = saved_for_student.loc[m_str]
                    if isinstance(record, pd.DataFrame): 
                        record = record.iloc[0] # Bierzemy pierwszy wiersz jeśli duplikat
                    final_amount = float(record['Kwota'])
                    is_paid = bool(record['Zaplacono'])
                
                table_data.append({
                    "ID Okresu": m_str,
                    "Termin": f"{MIESIACE_PL.get(m_num)} {y}",
                    "Kwota": final_amount,
                    "Zapłacono": is_paid
                })
                curr += relativedelta(months=1)

        else: # Tryb "Co zajęcia"
            all_lessons = get_lessons_in_period(df[df['ID'] == selected_id], start_date, effective_end)
            all_lessons.sort(key=lambda x: x['Data'])
            
            for l in all_lessons:
                d_str = l['Data'].strftime("%Y-%m-%d")
                label = f"{l['Data'].day} {MIESIACE_PL.get(l['Data'].month)} {l['Data'].year}"
                if l['Typ'] != 'Stała': label += f" ({l['Typ']})"
                
                final_amount = float(l['Stawka'])
                is_paid = False
                
                if not saved_for_student.empty and d_str in saved_for_student.index:
                    record = saved_for_student.loc[d_str]
                    if isinstance(record, pd.DataFrame): 
                        record = record.iloc[0]
                    final_amount = float(record['Kwota'])
                    is_paid = bool(record['Zaplacono'])
                    
                table_data.append({
                    "ID Okresu": d_str,
                    "Termin": label,
                    "Kwota": final_amount,
                    "Zapłacono": is_paid
                })

        # Wyświetlanie i zapisywanie tabeli
        if not table_data:
            st.info("Brak zajęć w tym okresie.")
        else:
            df_disp = pd.DataFrame(table_data)
            edited = st.data_editor(
                df_disp,
                column_config={
                    "ID Okresu": None,
                    "Termin": st.column_config.TextColumn(disabled=True),
                    "Kwota": st.column_config.NumberColumn(format="%.2f zł"),
                    "Zapłacono": st.column_config.CheckboxColumn()
                },
                hide_index=True, use_container_width=True, key=f"edit_{selected_id}"
            )
            
            if st.button("💾 Zapisz stan płatności", type="primary"):
                other = df_settlements[df_settlements['Uczen_ID'] != selected_id]
                new_rows = []
                for _, r in edited.iterrows():
                    new_rows.append({
                        'Uczen_ID': selected_id, 'Okres': r['ID Okresu'],
                        'Kwota': r['Kwota'], 'Zaplacono': r['Zapłacono']
                    })
                
                updated = pd.concat([other, pd.DataFrame(new_rows)], ignore_index=True)
                save_settlements(updated)
                df_settlements = updated
                st.success("Zapisano!")

# --- ZAKŁADKA FINANSE ---
elif menu == "💰 Finanse (Wykres)":
    st.header("Analiza Finansowa")
    if df.empty: st.info("Brak danych.")
    else:
        today = date.today()
        start_school_year = date(today.year if today.month >= 9 else today.year - 1, 9, 1)
        end_school_year = date(today.year + 1 if today.month >= 9 else today.year, 6, 30)
        
        income_year_total = calculate_income(df, start_school_year, end_school_year)
        paid_total = df_settlements[df_settlements['Zaplacono'] == True]['Kwota'].sum()
        
        c1, c2 = st.columns(2)
        c1.metric("Przewidywany Przychód (Rok)", f"{income_year_total} PLN")
        c2.metric("Zaksięgowane Wpłaty", f"{paid_total} PLN")
        
        st.divider()
        monthly_data = []
        curr = start_school_year
        while curr <= end_school_year:
            next_m = curr + relativedelta(months=1)
            e_m = next_m - timedelta(days=1)
            val = calculate_income(df, curr, e_m)
            label = f"{MIESIACE_PL.get(curr.month)} {curr.year}"
            monthly_data.append({"Miesiąc": label, "Przychód": val})
            curr = next_m
            
        st.altair_chart(alt.Chart(pd.DataFrame(monthly_data)).mark_bar().encode(
            x=alt.X('Miesiąc', sort=None), y='Przychód', tooltip=['Miesiąc', 'Przychód']
        ), use_container_width=True)

# --- DODAJ UCZNIA ---
elif menu == "➕ Dodaj Ucznia":
    st.header("Dodaj nowego ucznia")
    with st.form("add"):
        c1, c2 = st.columns(2)
        imie = c1.text_input("Imię")
        nazwisko = c2.text_input("Nazwisko")
        c3, c4 = st.columns(2)
        dzien = c3.selectbox("Dzień tygodnia", list(DNI_MAPA.keys()))
        godzina = c4.time_input("Godzina", time(16, 0))
        c5, c6, c7 = st.columns(3)
        data_rozp = c5.date_input("Start", date.today())
        data_zak = c6.date_input("Koniec", date(2026, 6, 26))
        tryb = c7.selectbox("Tryb płatności", ["Co zajęcia", "Miesięcznie"])
        stawka = st.number_input("Stawka", value=50)
        
        if st.form_submit_button("Zapisz"):
            new_id = 1 if df.empty else df['ID'].max() + 1
            new_row = {
                'ID': new_id, 'Imie': imie, 'Nazwisko': nazwisko,
                'Dzien_tyg': dzien, 'Godzina': godzina,
                'Data_rozp': data_rozp, 'Data_zak': data_zak, 'Stawka': stawka,
                'H_w_tygodniu': 1, 'Nieobecnosci': 0, 'Tryb_platnosci': tryb
            }
            for col in COLUMNS: 
                if col not in new_row: new_row[col] = ""
            df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
            save_data(df)
            st.success("Dodano!")
            st.rerun()

# --- BAZA DANYCH ---
elif menu == "📋 Baza Danych":
    st.header("Podgląd i edycja plików CSV")
    
    st.subheader("1. Uczniowie (uczniowie.csv)")
    edited = st.data_editor(df, num_rows="dynamic", key="edit_db_students")
    if st.button("Zapisz Uczniów"):
        save_data(edited)
        st.success("Zapisano!")

    st.subheader("2. Odwołane lekcje (odwolane.csv)")
    if not df_cancellations.empty:
        edited_cancel = st.data_editor(df_cancellations, num_rows="dynamic", key="edit_db_cancel")
        if st.button("Zapisz Odwołania"):
            save_cancellations(edited_cancel)
            st.success("Zapisano!")
    else:
        st.info("Brak odwołanych lekcji.")
        
    st.subheader("3. Dodatkowe lekcje (dodatkowe.csv)")
    if not df_extra.empty:
        edited_extra_db = st.data_editor(df_extra, num_rows="dynamic", key="edit_db_extra")
        if st.button("Zapisz Dodatkowe"):
            save_extra(edited_extra_db)
            st.success("Zapisano!")
    else:
        st.info("Brak dodatkowych lekcji.")