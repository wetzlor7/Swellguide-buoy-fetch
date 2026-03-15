import os
import glob
import requests
import copernicusmarine
import xarray as xr
import pandas as pd
from supabase import create_client
from datetime import datetime, timedelta, timezone

supabase = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])

def clean_val(val):
    return round(float(val), 2) if pd.notna(val) else None

print("--- Starte 144h Sync (High-Res & Resilient) ---")

buoys = supabase.table("buoys").select("*").eq("is_active", True).execute().data
if not buoys:
    print("Keine aktiven Bojen gefunden.")
    exit()

out_dir = "./temp_buoy_data"
os.makedirs(out_dir, exist_ok=True)

updates = []
now_utc = datetime.now(timezone.utc)
now_utc_str = now_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

time_threshold = now_utc.replace(tzinfo=None) - timedelta(hours=72)
time_threshold_str = time_threshold.strftime("%Y-%m-%dT%H:%M")

for b in buoys:
    station_id = b["station_id"]
    buoy_uuid = b.get("id") or b.get("uuid") 
    lat, lng = b["lat"], b["lon"]
    print(f"\nVerarbeite Boje: {station_id}...")

    # 1. Forecasts laden (ECMWF & GFS)
    url_ecmwf = f"https://marine-api.open-meteo.com/v1/marine?latitude={lat}&longitude={lng}&hourly=wave_height,wave_direction,wave_period,wave_peak_period&models=ecmwf_wam&past_days=3&forecast_days=3"
    url_gfs = f"https://marine-api.open-meteo.com/v1/marine?latitude={lat}&longitude={lng}&hourly=wave_height,wave_direction,wave_period&models=ncep_gfswave025&past_days=3&forecast_days=3"
    
    ecmwf_data = requests.get(url_ecmwf).json().get("hourly", {})
    gfs_data = requests.get(url_gfs).json().get("hourly", {})
    
    if not ecmwf_data or not gfs_data:
        continue

    merged_data = {}
    
    # Forecast-Zeilen anlegen 
    times = ecmwf_data.get("time", [])
    for i, t_str in enumerate(times):
        if t_str < time_threshold_str:
            continue
            
        t_iso = t_str + ":00Z"
        merged_data[t_iso] = {
            "buoy_uuid": buoy_uuid,
            "timestamp": t_iso, 
            "created_at": now_utc_str, 
            "ecmwf_height": clean_val(ecmwf_data["wave_height"][i]),
            "ecmwf_period": clean_val(ecmwf_data["wave_period"][i]),
            "ecmwf_peak_period": clean_val(ecmwf_data["wave_peak_period"][i]),
            "ecmwf_dir": clean_val(ecmwf_data["wave_direction"][i]),
            "gfs_height": clean_val(gfs_data["wave_height"][i]),
            "gfs_period": clean_val(gfs_data["wave_period"][i]),
            "gfs_dir": clean_val(gfs_data["wave_direction"][i]),
            "buoy_height": None, "buoy_period": None, "buoy_dir": None,
            "diff_height": None, "diff_period": None, "diff_dir": None
        }

    # 2. Bojen-Daten laden
    copernicusmarine.get(
        dataset_id="cmems_obs-ins_glo_phybgcwav_mynrt_na_irr",
        filter=f"*latest*{station_id}*.nc",
        no_directories=True,
        output_directory=out_dir
    )

    nc_files = glob.glob(f"{out_dir}/*{station_id}*.nc")
    if nc_files:
        dfs = []
        # NEU: Wir prüfen JEDE Datei und behalten nur die mit Wellen-Daten
        for f in nc_files:
            try:
                ds = xr.open_dataset(f)
                if "VHM0" in ds.variables:
                    dfs.append(ds.to_dataframe().reset_index())
                ds.close()
            except Exception as e:
                print(f"Fehler beim Lesen von {f}: {e}")
            finally:
                if os.path.exists(f):
                    os.remove(f) 
                
        if not dfs:
            print(f"  -> Keine VHM0-Daten für {station_id} in diesem Zeitraum gefunden.")
            continue
            
        df = pd.concat(dfs, ignore_index=True)
        
        time_col = 'TIME' if 'TIME' in df.columns else 'time'
        df[time_col] = pd.to_datetime(df[time_col]).dt.tz_localize(None)
        
        for col in ["VHM0", "VTPK", "VMDR"]:
            if col not in df.columns:
                df[col] = pd.NA
                
        df_waves = df[(df[time_col] >= time_threshold) & (df["VHM0"].notna())].copy()
        
        for _, row in df_waves.iterrows():
            t_exact = row[time_col]
            t_iso = t_exact.strftime("%Y-%m-%dT%H:%M:%S") + "Z"
            
            b_height = clean_val(row.get("VHM0"))
            b_period = clean_val(row.get("VTPK"))
            b_dir = clean_val(row.get("VMDR"))

            if t_iso not in merged_data:
                merged_data[t_iso] = {
                    "buoy_uuid": buoy_uuid,
                    "timestamp": t_iso, 
                    "created_at": now_utc_str,
                    "ecmwf_height": None, "ecmwf_period": None, "ecmwf_peak_period": None, "ecmwf_dir": None,
                    "gfs_height": None, "gfs_period": None, "gfs_dir": None,
                    "buoy_height": b_height, "buoy_period": b_period, "buoy_dir": b_dir,
                    "diff_height": None, "diff_period": None, "diff_dir": None
                }
            else:
                merged_data[t_iso]["buoy_height"] = b_height
                merged_data[t_iso]["buoy_period"] = b_period
                merged_data[t_iso]["buoy_dir"] = b_dir

            # Diffs berechnen 
            nearest_hour = t_exact.round("h").strftime("%Y-%m-%dT%H:%M:%S") + "Z"
            if nearest_hour in merged_data:
                f_ref = merged_data[nearest_hour]
                if b_height is not None and f_ref["ecmwf_height"] is not None:
                    merged_data[t_iso]["diff_height"] = round(b_height - f_ref["ecmwf_height"], 2)
                if b_period is not None and f_ref["ecmwf_peak_period"] is not None:
                    merged_data[t_iso]["diff_period"] = round(b_period - f_ref["ecmwf_peak_period"], 2)
                if b_dir is not None and f_ref["ecmwf_dir"] is not None:
                    merged_data[t_iso]["diff_dir"] = round(b_dir - f_ref["ecmwf_dir"], 2)
    
    updates.extend(list(merged_data.values()))

if updates:
    supabase.table("buoy_wave_data").upsert(updates, on_conflict="buoy_uuid,timestamp").execute()
    print(f"\nErfolgreich {len(updates)} Zeilen aktualisiert!")

# Löscht alle Daten, die älter als 72 Stunden sind
supabase.table("buoy_wave_data").delete().lt("timestamp", time_threshold_str).execute()