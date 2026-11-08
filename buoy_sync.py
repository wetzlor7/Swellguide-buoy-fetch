import os
import glob
import shutil
import requests
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

import pandas as pd
import xarray as xr
import copernicusmarine
from supabase import create_client

# --- Konfiguration ---
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    print("Fehler: SUPABASE_URL oder SUPABASE_SERVICE_ROLE_KEY nicht gesetzt.")
    exit(1)

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

PAST_DAYS = 3
FORECAST_DAYS = 4
MAX_WORKERS = 4  # 4 parallele Threads sind optimal für die 2 vCPUs von GitHub Actions
is_probe_run = os.environ.get("PROBE") == "1"

# Thread-Lock: Verhindert den Segfault beim C-Zugriff auf NetCDF4 / HDF5
netcdf_lock = threading.Lock()

def clean_val(val):
    if val is None or pd.isna(val):
        return None
    try:
        if hasattr(val, "item"):
            val = val.item()
        elif hasattr(val, "__iter__") and not isinstance(val, (str, bytes)):
            val = list(val)[0] if len(val) > 0 else None
            if val is not None and hasattr(val, "item"):
                val = val.item()
        v_float = float(val)
        return round(v_float, 2) if not pd.isna(v_float) else None
    except Exception:
        return None

def process_single_buoy(buoy):
    station_id = buoy.get("station_id")
    buoy_uuid = buoy.get("id")
    name = buoy.get("name", station_id)
    lat = buoy.get("latitude")
    lng = buoy.get("longitude")

    if not station_id or not lat or not lng:
        return f"[{name}] Übersprungen (ungültige Koordinaten / ID)."

    out_dir = f"./temp_{station_id}"
    shutil.rmtree(out_dir, ignore_errors=True)

    now_utc = datetime.now(timezone.utc)
    now_utc_str = now_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
    time_threshold_iso = (now_utc - timedelta(days=PAST_DAYS)).strftime("%Y-%m-%dT%H:%M:%SZ")
    time_threshold_naive = pd.to_datetime(time_threshold_iso).tz_localize(None)

    # 1. Download von Copernicus
    try:
        copernicusmarine.get(
            dataset_id="cmems_obs-ins_glo_phybgcwav_mynrt_na_irr",
            dataset_part="latest",
            filter=f"*{station_id}*.nc",
            no_directories=True,
            output_directory=out_dir,
            overwrite=True
        )
    except Exception as e:
        shutil.rmtree(out_dir, ignore_errors=True)
        return f"[{name}] Download-Fehler: {e}"

    nc_files = glob.glob(f"{out_dir}/*{station_id}*.nc")
    if not nc_files:
        shutil.rmtree(out_dir, ignore_errors=True)
        return f"[{name}] Keine 'latest'-Dateien gefunden."

    obs_rows = []
    # 2. NetCDF-Dateien thread-safe auslesen (inkl. CANDHIS/Cerema VAVH-Unterstützung)
    for f in nc_files:
        try:
            df = None
            with netcdf_lock:
                with xr.open_dataset(f) as ds:
                    # Wellenhöhe: VHM0 (Standard) oder VAVH (Frankreich / Cerema wie Anglet)
                    h_var = "VHM0" if "VHM0" in ds.variables else ("VAVH" if "VAVH" in ds.variables else None)
                    p_var = "VTPK" if "VTPK" in ds.variables else ("VTZA" if "VTZA" in ds.variables else ("VTM02" if "VTM02" in ds.variables else None))
                    d_var = "VMDR" if "VMDR" in ds.variables else ("VPED" if "VPED" in ds.variables else None)

                    if h_var:
                        wave_vars = [v for v in [h_var, p_var, d_var] if v in ds.variables]
                        df = ds[wave_vars].to_dataframe().reset_index()

            if df is None or df.empty:
                continue

            time_col = "TIME" if "TIME" in df.columns else "time"
            df[time_col] = pd.to_datetime(df[time_col])
            if df[time_col].dt.tz is not None:
                df[time_col] = df[time_col].dt.tz_localize(None)

            mask = (df[time_col] >= time_threshold_naive) & (df[h_var].notna())
            df_valid = df[mask].copy()

            if not df_valid.empty:
                df_valid = df_valid.drop_duplicates(subset=[time_col], keep="first")
                for _, row in df_valid.iterrows():
                    cleaned_h = clean_val(row.get(h_var))
                    if cleaned_h is not None:
                        t_iso = row[time_col].strftime("%Y-%m-%dT%H:%M:%SZ")
                        obs_rows.append({
                            "timestamp": t_iso,
                            "buoy_height": cleaned_h,
                            "buoy_period": clean_val(row.get(p_var)) if p_var and p_var in row else None,
                            "buoy_dir": clean_val(row.get(d_var)) if d_var and d_var in row else None,
                        })
        except Exception:
            pass

    shutil.rmtree(out_dir, ignore_errors=True)

    if not obs_rows:
        return f"[{name}] Keine gültigen Messwerte im 3-Tage-Fenster."

    # 3. Open-Meteo Forecast abrufen
    url_ecmwf = f"https://marine-api.open-meteo.com/v1/marine?latitude={lat}&longitude={lng}&hourly=wave_height,wave_direction,wave_period,wave_peak_period&models=ecmwf_wam&past_days={PAST_DAYS}&forecast_days={FORECAST_DAYS}"
    url_gfs = f"https://marine-api.open-meteo.com/v1/marine?latitude={lat}&longitude={lng}&hourly=wave_height,wave_direction,wave_period&models=ncep_gfswave025&past_days={PAST_DAYS}&forecast_days={FORECAST_DAYS}"

    try:
        r_ecmwf = requests.get(url_ecmwf, timeout=15)
        ecmwf_data = r_ecmwf.json().get("hourly", {}) if r_ecmwf.status_code == 200 else {}
    except Exception:
        ecmwf_data = {}

    try:
        r_gfs = requests.get(url_gfs, timeout=15)
        gfs_data = r_gfs.json().get("hourly", {}) if r_gfs.status_code == 200 else {}
    except Exception:
        gfs_data = {}

    # 4. Daten zusammenführen
    merged_data = {}
    time_series = ecmwf_data.get("time", []) or gfs_data.get("time", [])

    for i, t_str in enumerate(time_series):
        t_iso = t_str + ":00Z"
        merged_data[t_iso] = {
            "buoy_uuid": buoy_uuid,
            "timestamp": t_iso,
            "created_at": now_utc_str,
            "ecmwf_height": clean_val(ecmwf_data.get("wave_height", [None])[i]) if i < len(ecmwf_data.get("wave_height", [])) else None,
            "ecmwf_period": clean_val(ecmwf_data.get("wave_period", [None])[i]) if i < len(ecmwf_data.get("wave_period", [])) else None,
            "ecmwf_peak_period": clean_val(ecmwf_data.get("wave_peak_period", [None])[i]) if i < len(ecmwf_data.get("wave_peak_period", [])) else None,
            "ecmwf_dir": clean_val(ecmwf_data.get("wave_direction", [None])[i]) if i < len(ecmwf_data.get("wave_direction", [])) else None,
            "gfs_height": clean_val(gfs_data.get("wave_height", [None])[i]) if i < len(gfs_data.get("wave_height", [])) else None,
            "gfs_period": clean_val(gfs_data.get("wave_period", [None])[i]) if i < len(gfs_data.get("wave_period", [])) else None,
            "gfs_dir": clean_val(gfs_data.get("wave_direction", [None])[i]) if i < len(gfs_data.get("wave_direction", [])) else None,
            "buoy_height": None,
            "buoy_period": None,
            "buoy_dir": None,
            "diff_height": None,
            "diff_period": None,
            "diff_dir": None,
        }

    for obs in obs_rows:
        t_iso = obs["timestamp"]
        b_h, b_p, b_d = obs["buoy_height"], obs["buoy_period"], obs["buoy_dir"]
        if t_iso not in merged_data:
            merged_data[t_iso] = {
                "buoy_uuid": buoy_uuid, "timestamp": t_iso, "created_at": now_utc_str,
                "ecmwf_height": None, "ecmwf_period": None, "ecmwf_peak_period": None, "ecmwf_dir": None,
                "gfs_height": None, "gfs_period": None, "gfs_dir": None,
                "buoy_height": b_h, "buoy_period": b_p, "buoy_dir": b_d,
                "diff_height": None, "diff_period": None, "diff_dir": None,
            }
        else:
            merged_data[t_iso].update({"buoy_height": b_h, "buoy_period": b_p, "buoy_dir": b_d})

        nearest_hour = pd.Timestamp(t_iso).round("h").strftime("%Y-%m-%dT%H:%M:%SZ")
        if nearest_hour in merged_data:
            ref = merged_data[nearest_hour]
            if b_h is not None and ref["ecmwf_height"] is not None:
                merged_data[t_iso]["diff_height"] = round(b_h - ref["ecmwf_height"], 2)
            if b_p is not None and ref["ecmwf_peak_period"] is not None:
                merged_data[t_iso]["diff_period"] = round(b_p - ref["ecmwf_peak_period"], 2)
            if b_d is not None and ref["ecmwf_dir"] is not None:
                merged_data[t_iso]["diff_dir"] = round(b_d - ref["ecmwf_dir"], 2)

    # 5. Speichern in Supabase
    updates = list(merged_data.values())
    if updates:
        supabase.table("buoy_wave_data").upsert(updates, on_conflict="buoy_uuid,timestamp").execute()
        if is_probe_run:
            supabase.table("buoys").update({"is_active": True}).eq("id", buoy_uuid).execute()

    supabase.table("buoy_wave_data").delete().eq("buoy_uuid", buoy_uuid).lt("timestamp", time_threshold_iso).execute()
    return f"✓ [{name}] {len(obs_rows)} Messwerte gespeichert."

print("--- Starte parallelen Copernicus Buoy Sync ---")
query = supabase.table("buoys").select("*").ilike("provider", "%Copernicus%")
if is_probe_run:
    buoys = query.eq("is_active", False).eq("admin_disabled", False).execute().data
else:
    buoys = query.eq("is_active", True).execute().data

if not buoys:
    print("Keine Bojen gefunden.")
    exit(0)

print(f"Verarbeite {len(buoys)} Bojen mit {MAX_WORKERS} Threads parallel...")

with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    futures = {executor.submit(process_single_buoy, b): b for b in buoys}
    for future in as_completed(futures):
        print(future.result())

print("\n--- Sync erfolgreich beendet ---")
