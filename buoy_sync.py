import os, glob, requests, copernicusmarine, shutil
import xarray as xr, pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from supabase import create_client
from datetime import datetime, timedelta, timezone

supabase = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])
is_probe_run = os.environ.get("PROBE_MODE", "false").lower() == "true"

PAST_DAYS = 3
FORECAST_DAYS = 4
MAX_WORKERS = 5  # 5 Bojen parallel laden -> drückt die Zeit von 25 auf ~4 Minuten

def clean_val(val):
    if val is None:
        return None
    try:
        if hasattr(val, "__iter__") and not isinstance(val, (str, bytes)):
            val = val[0]
        if pd.isna(val):
            return None
        f = float(val)
        return round(f, 2) if not pd.isna(f) else None
    except Exception:
        return None

now_utc = datetime.now(timezone.utc)
now_utc_str = now_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
time_threshold_dt = now_utc - timedelta(days=PAST_DAYS)
time_threshold_str = time_threshold_dt.strftime("%Y-%m-%dT%H:%M")
time_threshold_naive = time_threshold_dt.replace(tzinfo=None)

def process_single_buoy(b):
    station_id = str(b["station_id"]).strip()
    buoy_uuid = b.get("id") or b.get("uuid")
    lat, lng = b["lat"], b["lon"]
    name = b.get("name", station_id)

    out_dir = f"./temp_{station_id}"
    shutil.rmtree(out_dir, ignore_errors=True)
    os.makedirs(out_dir, exist_ok=True)

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
    for f in nc_files:
        try:
            with xr.open_dataset(f) as ds:
                if "VHM0" not in ds.variables:
                    continue
                wave_vars = [v for v in ["VHM0", "VTPK", "VMDR"] if v in ds.variables]
                df = ds[wave_vars].to_dataframe().reset_index()

                time_col = "TIME" if "TIME" in df.columns else "time"
                df[time_col] = pd.to_datetime(df[time_col])
                if df[time_col].dt.tz is not None:
                    df[time_col] = df[time_col].dt.tz_localize(None)

                mask = (df[time_col] >= time_threshold_naive) & (df["VHM0"].notna())
                df_valid = df[mask].copy()

                if not df_valid.empty:
                    df_valid = df_valid.drop_duplicates(subset=[time_col], keep="first")
                    for _, row in df_valid.iterrows():
                        cleaned_h = clean_val(row.get("VHM0"))
                        if cleaned_h is not None:
                            t_iso = row[time_col].strftime("%Y-%m-%dT%H:%M:%SZ")
                            obs_rows.append({
                                "timestamp": t_iso,
                                "buoy_height": cleaned_h,
                                "buoy_period": clean_val(row.get("VTPK")) if "VTPK" in row else None,
                                "buoy_dir": clean_val(row.get("VMDR")) if "VMDR" in row else None,
                            })
        except Exception as e:
            pass

    shutil.rmtree(out_dir, ignore_errors=True)

    if not obs_rows:
        return f"[{name}] Keine gültigen Messwerte im 3-Tage-Fenster."

    # Open-Meteo
    url_ecmwf = f"https://marine-api.open-meteo.com/v1/marine?latitude={lat}&longitude={lng}&hourly=wave_height,wave_direction,wave_period,wave_peak_period&models=ecmwf_wam&past_days={PAST_DAYS}&forecast_days={FORECAST_DAYS}"
    url_gfs = f"https://marine-api.open-meteo.com/v1/marine?latitude={lat}&longitude={lng}&hourly=wave_height,wave_direction,wave_period&models=ncep_gfswave025&past_days={PAST_DAYS}&forecast_days={FORECAST_DAYS}"

    ecmwf_data, gfs_data = {}, {}
    try:
        r_ec = requests.get(url_ecmwf, timeout=10)
        if r_ec.ok: ecmwf_data = r_ec.json().get("hourly", {})
        r_gf = requests.get(url_gfs, timeout=10)
        if r_gf.ok: gfs_data = r_gf.json().get("hourly", {})
    except Exception:
        pass

    merged_data = {}
    if ecmwf_data and gfs_data:
        for i, t_str in enumerate(ecmwf_data.get("time", [])):
            if t_str < time_threshold_str:
                continue
            t_iso = t_str + ":00Z"
            merged_data[t_iso] = {
                "buoy_uuid": buoy_uuid,
                "timestamp": t_iso,
                "created_at": now_utc_str,
                "ecmwf_height": clean_val(ecmwf_data.get("wave_height", [None])[i]),
                "ecmwf_period": clean_val(ecmwf_data.get("wave_period", [None])[i]),
                "ecmwf_peak_period": clean_val(ecmwf_data.get("wave_peak_period", [None])[i]),
                "ecmwf_dir": clean_val(ecmwf_data.get("wave_direction", [None])[i]),
                "gfs_height": clean_val(gfs_data.get("wave_height", [None])[i]),
                "gfs_period": clean_val(gfs_data.get("wave_period", [None])[i]),
                "gfs_dir": clean_val(gfs_data.get("wave_direction", [None])[i]),
                "buoy_height": None, "buoy_period": None, "buoy_dir": None,
                "diff_height": None, "diff_period": None, "diff_dir": None,
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

    updates = list(merged_data.values())
    if updates:
        supabase.table("buoy_wave_data").upsert(updates, on_conflict="buoy_uuid,timestamp").execute()
        if is_probe_run:
            supabase.table("buoys").update({"is_active": True}).eq("id", buoy_uuid).execute()

    supabase.table("buoy_wave_data").delete().eq("buoy_uuid", buoy_uuid).lt("timestamp", time_threshold_str + ":00Z").execute()
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
