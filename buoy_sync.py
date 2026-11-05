import os, glob, requests, copernicusmarine, shutil, signal, time
import xarray as xr, pandas as pd
from supabase import create_client
from datetime import datetime, timedelta, timezone

supabase = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])
is_probe_run = os.environ.get("PROBE_MODE", "false").lower() == "true"

def clean_val(val):
    return round(float(val), 2) if pd.notna(val) else None

class TimeoutException(Exception): pass
def timeout_handler(signum, frame): raise TimeoutException()
signal.signal(signal.SIGALRM, timeout_handler)

print(f"--- Starte sequenziellen Sync ---")
print(f"Modus: {'PROBE-RUN (Wöchentlich, Inaktive Bojen)' if is_probe_run else 'LIVE-SYNC (30-Min, Aktive Bojen)'}")

# Dynamische Datenbank-Abfrage je nach Modus
if is_probe_run:
    buoys = supabase.table("buoys").select("*").eq("is_active", False).eq("admin_disabled", False).execute().data
else:
    buoys = supabase.table("buoys").select("*").eq("is_active", True).execute().data

if not buoys:
    print("Keine relevanten Bojen für diesen Durchlauf gefunden.")
    exit()

now_utc_str = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
# Das 7-Tage-Fenster bleibt bestehen, um echte Verzögerungen von Copernicus abzufedern
time_threshold = datetime.now() - timedelta(days=7)
time_threshold_str = time_threshold.strftime("%Y-%m-%dT%H:%M")

for b in buoys:
    station_id = b["station_id"]
    buoy_uuid = b.get("id") or b.get("uuid")
    lat, lng = b["lat"], b["lon"]
    
    print(f"\n--- Verarbeite {station_id} ---")
    out_dir = f"./temp_{station_id}"
    shutil.rmtree(out_dir, ignore_errors=True)
    os.makedirs(out_dir, exist_ok=True)

    url_ecmwf = f"https://marine-api.open-meteo.com/v1/marine?latitude={lat}&longitude={lng}&hourly=wave_height,wave_direction,wave_period,wave_peak_period&models=ecmwf_wam&past_days=7&forecast_days=3"
    url_gfs = f"https://marine-api.open-meteo.com/v1/marine?latitude={lat}&longitude={lng}&hourly=wave_height,wave_direction,wave_period&models=ncep_gfswave025&past_days=7&forecast_days=3"

    try:
        ecmwf_data = requests.get(url_ecmwf).json().get("hourly", {})
        gfs_data = requests.get(url_gfs).json().get("hourly", {})
    except Exception as e:
        print(f"OM API Fehler: {e}")
        continue

    # Scope-Reset für jede Boje
    merged_data = {}
    if ecmwf_data and gfs_data:
        for i, t_str in enumerate(ecmwf_data.get("time", [])):
            if t_str < time_threshold_str: continue
            t_iso = t_str + ":00Z"
            merged_data[t_iso] = {
                "buoy_uuid": buoy_uuid, "timestamp": t_iso, "created_at": now_utc_str,
                "ecmwf_height": clean_val(ecmwf_data["wave_height"][i]), "ecmwf_period": clean_val(ecmwf_data["wave_period"][i]),
                "ecmwf_peak_period": clean_val(ecmwf_data["wave_peak_period"][i]), "ecmwf_dir": clean_val(ecmwf_data["wave_direction"][i]),
                "gfs_height": clean_val(gfs_data["wave_height"][i]), "gfs_period": clean_val(gfs_data["wave_period"][i]), "gfs_dir": clean_val(gfs_data["wave_direction"][i]),
                "buoy_height": None, "buoy_period": None, "buoy_dir": None, "diff_height": None, "diff_period": None, "diff_dir": None
            }

    has_copernicus_data = False
    # 1. Timeout auf 45 Sekunden reduzieren! 
    # (30 Bojen x 45s = max 22.5 Minuten -> sicher unter dem 55-Min-Limit)
    signal.alarm(45) 
    try:
        # 2. regex statt filter nutzen, um Full-Scans des Katalogs zu verhindern
        copernicusmarine.get(
            dataset_id="cmems_obs-ins_glo_phybgcwav_mynrt_na_irr", 
            regex=rf".*{station_id}.*\.nc$", 
            no_directories=True, 
            output_directory=out_dir
        )
    except TimeoutException:
        print("Timeout: Copernicus reagiert nicht in 45s.")
    except Exception as e:
        print(f"Fehler beim Download: {e}")
    finally:
        signal.alarm(0)

    nc_files = glob.glob(f"{out_dir}/*{station_id}*.nc")
    if nc_files:
        dfs = []
        for f in nc_files:
            try:
                with xr.open_dataset(f) as ds:
                    if "VHM0" in ds.variables: dfs.append(ds.to_dataframe().reset_index())
            except: pass

        if dfs:
            df = pd.concat(dfs, ignore_index=True)
            time_col = 'TIME' if 'TIME' in df.columns else 'time'
            df[time_col] = pd.to_datetime(df[time_col]).dt.tz_localize(None)

            for col in ["VHM0", "VTPK", "VMDR"]:
                if col not in df.columns: df[col] = pd.NA

            df_valid = df[(df[time_col] >= time_threshold) & (df["VHM0"].notna())]
            if not df_valid.empty:
                has_copernicus_data = True
                for _, row in df_valid.iterrows():
                    t_exact = row[time_col]
                    t_iso = t_exact.strftime("%Y-%m-%dT%H:%M:%S") + "Z"
                    b_height, b_period, b_dir = clean_val(row.get("VHM0")), clean_val(row.get("VTPK")), clean_val(row.get("VMDR"))

                    if t_iso not in merged_data:
                        merged_data[t_iso] = {"buoy_uuid": buoy_uuid, "timestamp": t_iso, "created_at": now_utc_str,
                                              "ecmwf_height": None, "ecmwf_period": None, "ecmwf_peak_period": None, "ecmwf_dir": None,
                                              "gfs_height": None, "gfs_period": None, "gfs_dir": None,
                                              "buoy_height": b_height, "buoy_period": b_period, "buoy_dir": b_dir,
                                              "diff_height": None, "diff_period": None, "diff_dir": None}
                    else:
                        merged_data[t_iso].update({"buoy_height": b_height, "buoy_period": b_period, "buoy_dir": b_dir})

                    nearest_hour = t_exact.round("h").strftime("%Y-%m-%dT%H:%M:%S") + "Z"
                    if nearest_hour in merged_data:
                        f_ref = merged_data[nearest_hour]
                        if b_height is not None and f_ref["ecmwf_height"] is not None: merged_data[t_iso]["diff_height"] = round(b_height - f_ref["ecmwf_height"], 2)
                        if b_period is not None and f_ref["ecmwf_peak_period"] is not None: merged_data[t_iso]["diff_period"] = round(b_period - f_ref["ecmwf_peak_period"], 2)
                        if b_dir is not None and f_ref["ecmwf_dir"] is not None: merged_data[t_iso]["diff_dir"] = round(b_dir - f_ref["ecmwf_dir"], 2)

    shutil.rmtree(out_dir, ignore_errors=True)

    updates = list(merged_data.values())
    if updates:
        if not has_copernicus_data:
            print(f"⚠️ Kein Copernicus-Update. Speichere NUR den Open-Meteo Forecast.")
            # Keys entfernen, damit die DB beim Upsert alte Messwerte nicht mit None überschreibt
            for row in updates:
                for key in ["buoy_height", "buoy_period", "buoy_dir", "diff_height", "diff_period", "diff_dir"]:
                    row.pop(key, None)

        supabase.table("buoy_wave_data").upsert(updates, on_conflict="buoy_uuid,timestamp").execute()
        print(f" -> {len(updates)} Zeilen erfolgreich aktualisiert.")
            
        # Auto-Reaktivierung natürlich nur, wenn wir ECHTE Bojen-Daten haben
        if has_copernicus_data and is_probe_run:
            supabase.table("buoys").update({"is_active": True}).eq("id", buoy_uuid).execute()
            print(f"🎉 Boje {station_id} liefert wieder Daten und wurde automatisch reaktiviert!")

    # Isolierter Cleanup 
    supabase.table("buoy_wave_data").delete().eq("buoy_uuid", buoy_uuid).lt("timestamp", time_threshold_str).execute()
    
    time.sleep(2)
