import os, glob, requests, copernicusmarine, shutil, time
import xarray as xr, pandas as pd
from supabase import create_client
from datetime import datetime, timedelta, timezone

supabase = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])
is_probe_run = os.environ.get("PROBE_MODE", "false").lower() == "true"

PAST_DAYS = 3
FORECAST_DAYS = 4

def clean_val(val):
    if val is None:
        return None
    try:
        # Falls ein Array/Tuple durchrutscht, ersten Wert nehmen
        if hasattr(val, "__iter__") and not isinstance(val, (str, bytes)):
            val = val[0]
        if pd.isna(val):
            return None
        f = float(val)
        return round(f, 2) if not pd.isna(f) else None
    except Exception:
        return None

print("--- Starte Copernicus Buoy Sync ---")
print(f"Modus: {'PROBE-RUN (Inaktiv)' if is_probe_run else 'LIVE-SYNC (Aktiv)'}")

if is_probe_run:
    buoys = (
        supabase.table("buoys")
        .select("*")
        .ilike("provider", "%Copernicus%")
        .eq("is_active", False)
        .eq("admin_disabled", False)
        .execute()
        .data
    )
else:
    buoys = (
        supabase.table("buoys")
        .select("*")
        .ilike("provider", "%Copernicus%")
        .eq("is_active", True)
        .execute()
        .data
    )

if not buoys:
    print("Keine relevanten Copernicus-Bojen gefunden.")
    exit(0)

now_utc = datetime.now(timezone.utc)
now_utc_str = now_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

time_threshold_dt = now_utc - timedelta(days=PAST_DAYS)
time_threshold_iso = time_threshold_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
time_threshold_str = time_threshold_dt.strftime("%Y-%m-%dT%H:%M")

print(f"Verarbeite {len(buoys)} Bojen (Historie ab {time_threshold_iso}, Forecast bis +{FORECAST_DAYS-1} Tage)...\n")

for b in buoys:
    station_id = str(b["station_id"]).strip()
    buoy_uuid = b.get("id") or b.get("uuid")
    lat, lng = b["lat"], b["lon"]

    print(f"--- Boje {b.get('name', station_id)} ({station_id}) ---")
    out_dir = f"./temp_{station_id}"
    shutil.rmtree(out_dir, ignore_errors=True)
    os.makedirs(out_dir, exist_ok=True)

    # 1. Copernicus download
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
        print(f"  ❌ Download-Fehler: {e}")
        shutil.rmtree(out_dir, ignore_errors=True)
        continue

    nc_files = glob.glob(f"{out_dir}/*{station_id}*.nc")
    if not nc_files:
        print("  ⚠️ Keine 'latest'-Dateien gefunden.")
        shutil.rmtree(out_dir, ignore_errors=True)
        continue

    # 2. NetCDF auslesen über schlankes to_dataframe (extrem robust gegen DEPTH/Dimensionen)
    obs_rows = []
    time_threshold_naive = time_threshold_dt.replace(tzinfo=None)

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
                    # Mehrere Sensortiefen am selben Zeitstempel auf 1 Messwert reduzieren
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
            print(f"  Fehler beim Lesen von {f}: {e}")

    shutil.rmtree(out_dir, ignore_errors=True)

    if not obs_rows:
        print("  ⚠️ Keine gültigen Wellendaten (VHM0) in den letzten 3 Tagen.")
        continue

    print(f"  ✓ {len(obs_rows)} Messwerte gefunden. Lade Open-Meteo...")

    # 3. Open-Meteo Forecast abrufen
    url_ecmwf = f"https://marine-api.open-meteo.com/v1/marine?latitude={lat}&longitude={lng}&hourly=wave_height,wave_direction,wave_period,wave_peak_period&models=ecmwf_wam&past_days={PAST_DAYS}&forecast_days={FORECAST_DAYS}"
    url_gfs = f"https://marine-api.open-meteo.com/v1/marine?latitude={lat}&longitude={lng}&hourly=wave_height,wave_direction,wave_period&models=ncep_gfswave025&past_days={PAST_DAYS}&forecast_days={FORECAST_DAYS}"

    ecmwf_data, gfs_data = {}, {}
    try:
        r_ecmwf = requests.get(url_ecmwf, timeout=10)
        if r_ecmwf.ok:
            ecmwf_data = r_ecmwf.json().get("hourly", {})
        r_gfs = requests.get(url_gfs, timeout=10)
        if r_gfs.ok:
            gfs_data = r_gfs.json().get("hourly", {})
    except Exception as e:
        print(f"  ⚠️ Open-Meteo Fehler: {e}")

    # 4. Forecast-Grid aufbauen
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
                "buoy_height": None,
                "buoy_period": None,
                "buoy_dir": None,
                "diff_height": None,
                "diff_period": None,
                "diff_dir": None,
            }

    # 5. Messwerte mergen & Diffs berechnen
    for obs in obs_rows:
        t_iso = obs["timestamp"]
        b_h = obs["buoy_height"]
        b_p = obs["buoy_period"]
        b_d = obs["buoy_dir"]

        if t_iso not in merged_data:
            merged_data[t_iso] = {
                "buoy_uuid": buoy_uuid,
                "timestamp": t_iso,
                "created_at": now_utc_str,
                "ecmwf_height": None,
                "ecmwf_period": None,
                "ecmwf_peak_period": None,
                "ecmwf_dir": None,
                "gfs_height": None,
                "gfs_period": None,
                "gfs_dir": None,
                "buoy_height": b_h,
                "buoy_period": b_p,
                "buoy_dir": b_d,
                "diff_height": None,
                "diff_period": None,
                "diff_dir": None,
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

    # 6. Upsert in DB & Cleanup
    updates = list(merged_data.values())
    if updates:
        supabase.table("buoy_wave_data").upsert(updates, on_conflict="buoy_uuid,timestamp").execute()
        print(f"  ✓ {len(updates)} Zeilen gespeichert.")

        if is_probe_run:
            supabase.table("buoys").update({"is_active": True}).eq("id", buoy_uuid).execute()
            print(f"  🎉 Boje {station_id} reaktiviert!")

    supabase.table("buoy_wave_data").delete().eq("buoy_uuid", buoy_uuid).lt("timestamp", time_threshold_iso).execute()

print("\n--- Sync erfolgreich abgeschlossen ---")
