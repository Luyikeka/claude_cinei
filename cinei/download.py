"""
Data download utilities for CINEI.
Supports CEDS gridded emission data from PNNL DataHub.
"""

import os
import hashlib
import tarfile
import requests
from pathlib import Path


# ── Data registry ─────────────────────────────────────────────────────────────
CEDS_REGISTRY = {
    "url": (
        "https://g-83fdd0.1beed.03c0.data.globus.org/cartuids/"
        "13796_addda3a315c4154d9d9044771ea80aa3.tar"
    ),
    "doi": "https://doi.org/10.25584/PNNLDataHub/1779095",
    "version": "v_2021_04_21",
    "description": "CEDS v_2021_04_21 gridded 0.5° monthly NetCDF",
    "species": ["SO2", "NOx", "CO", "BC", "OC", "NH3", "NMVOC",
                "CO2", "CH4", "N2O", "PM2.5", "PM10"],
}

# ── Species name variants (case-insensitive mapping) ──────────────────────────
# Maps any user input → list of possible filename fragments to match
SPECIES_VARIANTS = {
    "NOX":   ["NOx", "nox", "NOX"],
    "SO2":   ["SO2", "so2"],
    "CO":    ["_CO_", "_co_"],       # avoid matching CO2
    "CO2":   ["CO2", "co2"],
    "BC":    ["BC", "bc"],
    "OC":    ["OC", "oc"],
    "NH3":   ["NH3", "nh3"],
    "NMVOC": ["NMVOC", "nmvoc", "VOC", "voc"],
    "CH4":   ["CH4", "ch4"],
    "N2O":   ["N2O", "n2o"],
    "PM25":  ["PM2.5", "pm2.5", "PM25", "pm25"],
    "PM10":  ["PM10", "pm10"],
}


# ── Main public functions ──────────────────────────────────────────────────────

def download_ceds(save_dir, species=None, keep_tar=False):
    """
    Download CEDS v_2021_04_21 gridded emission data from PNNL DataHub.

    Parameters
    ----------
    save_dir : str
        Directory to save downloaded and extracted files.
    species : list of str, optional
        Species to extract. Case-insensitive.
        e.g. ['CO', 'NOx'] or ['co', 'nox'] or ['CO', 'NOX']
        If None, all species are extracted.
        Available: SO2, NOx, CO, BC, OC, NH3, NMVOC, CO2, CH4, N2O, PM2.5, PM10
    keep_tar : bool, optional
        If True, keep the .tar file after extraction. Default False.

    Returns
    -------
    list of str
        Paths to extracted NetCDF files.

    Examples
    --------
    >>> import cinei
    >>> files = cinei.download_ceds(
    ...     save_dir='/work/bb1554/data/CEDS',
    ...     species=['CO', 'NOx']   # case-insensitive: 'co','nox' also works
    ... )
    """
    save_dir = Path(save_dir)
    save_dir.mkdir(parents=True, exist_ok=True)

    tar_path = save_dir / "CEDS_v_2021_04_21.tar"

    # ── Validate species input ─────────────────────────────────────────
    if species is not None:
        species = _normalize_species(species)

    print(f"[CINEI] CEDS v_2021_04_21 Download")
    print(f"[CINEI] Source  : {CEDS_REGISTRY['doi']}")
    print(f"[CINEI] Save to : {save_dir}")
    print(f"[CINEI] Species : {species if species else 'ALL'}")
    print()

    # ── Step 1: Download tar (with resume) ────────────────────────────
    _download_with_resume(CEDS_REGISTRY["url"], tar_path)

    # ── Step 2: Extract selected species ──────────────────────────────
    extracted = _extract_species(tar_path, save_dir, species)

    # ── Step 3: Cleanup tar ───────────────────────────────────────────
    if not keep_tar and tar_path.exists():
        os.remove(tar_path)
        print(f"[CINEI] Removed tar file: {tar_path.name}")

    print(f"\n[CINEI] ✅ Done! {len(extracted)} file(s) saved to {save_dir}")
    for f in extracted:
        print(f"         {Path(f).name}")
    return extracted


def list_ceds_species():
    """Print available CEDS species and their accepted name variants."""
    print("[CINEI] Available CEDS species (case-insensitive):")
    for canonical, variants in SPECIES_VARIANTS.items():
        clean = [v.strip('_') for v in variants]
        print(f"  {canonical:<8} →  accepted input: {clean}")


# ── Species normalization ──────────────────────────────────────────────────────

def _normalize_species(species_list):
    """
    Normalize user-provided species names to canonical uppercase keys.
    Raises ValueError for unrecognized species.

    Examples
    --------
    ['co', 'NOx', 'SO2'] → ['CO', 'NOX', 'SO2']
    """
    normalized = []
    unrecognized = []

    for sp in species_list:
        sp_upper = sp.upper().strip()
        if sp_upper in SPECIES_VARIANTS:
            normalized.append(sp_upper)
        else:
            # Try fuzzy match: e.g. 'nox' → 'NOX'
            matched = None
            for key, variants in SPECIES_VARIANTS.items():
                if sp_upper in [v.strip('_').upper() for v in variants]:
                    matched = key
                    break
            if matched:
                normalized.append(matched)
            else:
                unrecognized.append(sp)

    if unrecognized:
        raise ValueError(
            f"[CINEI] Unrecognized species: {unrecognized}\n"
            f"        Available: {list(SPECIES_VARIANTS.keys())}\n"
            f"        Call cinei.list_ceds_species() to see all options."
        )

    return normalized


# ── Internal helpers ───────────────────────────────────────────────────────────

def _extract_species(tar_path, out_dir, species=None):
    """Extract NetCDF files for selected species from CEDS tar archive."""
    tar_path = Path(tar_path)
    out_dir  = Path(out_dir)
    extracted = []

    print(f"\n[CINEI] Opening archive: {tar_path.name}")

    with tarfile.open(tar_path, "r:*") as tar:
        members = tar.getmembers()
        nc_members = [m for m in members if m.name.endswith(".nc")]

        # ── Filter by species variants ─────────────────────────────────
        if species:
            filtered = []
            for m in nc_members:
                fname = m.name  # preserve original case for matching
                for sp in species:
                    variants = SPECIES_VARIANTS.get(sp, [sp])
                    if any(v in fname for v in variants):
                        filtered.append(m)
                        break
            nc_members = filtered

        if not nc_members:
            raise ValueError(
                f"[CINEI] No matching NetCDF files found for species: {species}\n"
                f"        Call cinei.list_ceds_species() to see all options."
            )

        print(f"[CINEI] Extracting {len(nc_members)} NetCDF file(s)...")

        try:
            from tqdm import tqdm
            iterator = tqdm(nc_members, desc="Extracting", ncols=80)
        except ImportError:
            iterator = nc_members

        for member in iterator:
            member.name = Path(member.name).name
            tar.extract(member, path=out_dir)
            out_path = out_dir / member.name
            md5 = _md5(out_path)
            print(f"[CINEI]   ✅ {member.name}  md5:{md5[:8]}...")
            extracted.append(str(out_path))

    return extracted


def _download_with_resume(url, dest_path):
    """Download a file with resume support and tqdm progress bar."""
    try:
        from tqdm import tqdm
        use_tqdm = True
    except ImportError:
        use_tqdm = False
        print("[CINEI] Tip: pip install tqdm for a progress bar")

    dest_path = Path(dest_path)
    existing_size = dest_path.stat().st_size if dest_path.exists() else 0

    head = requests.head(url, allow_redirects=True, timeout=30)
    total_size = int(head.headers.get("content-length", 0))

    if existing_size == total_size and total_size > 0:
        print(f"[CINEI] Already downloaded: {dest_path.name} "
              f"({_human_size(total_size)})")
        return

    if existing_size > 0:
        print(f"[CINEI] Resuming from {_human_size(existing_size)} "
              f"/ {_human_size(total_size)}")
    else:
        print(f"[CINEI] Downloading {dest_path.name} "
              f"({_human_size(total_size)})...")

    headers = {"Range": f"bytes={existing_size}-"} if existing_size > 0 else {}
    mode = "ab" if existing_size > 0 else "wb"

    response = requests.get(url, headers=headers, stream=True,
                            allow_redirects=True, timeout=60)
    response.raise_for_status()

    chunk_size = 1024 * 1024

    if use_tqdm:
        progress = tqdm(total=total_size, initial=existing_size,
                        unit="B", unit_scale=True, unit_divisor=1024,
                        desc=dest_path.name, ncols=80)

    with open(dest_path, mode) as f:
        for chunk in response.iter_content(chunk_size=chunk_size):
            if chunk:
                f.write(chunk)
                if use_tqdm:
                    progress.update(len(chunk))

    if use_tqdm:
        progress.close()
    print(f"[CINEI] Download complete: {dest_path.name}")


def _md5(filepath, chunk_size=1024 * 1024):
    """Compute MD5 checksum of a file."""
    h = hashlib.md5()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()


def _human_size(num_bytes):
    """Convert bytes to human-readable string."""
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if num_bytes < 1024:
            return f"{num_bytes:.1f} {unit}"
        num_bytes /= 1024
    return f"{num_bytes:.1f} PB"


# ── MEIC Registry ─────────────────────────────────────────────────────────────
MEIC_REGISTRY = {
    "doi": "https://doi.org/10.5281/zenodo.15039737",
    "zenodo_id": "15039737",
    "version": "MEIC v1.4",
    "description": "MEIC 2017 speciated NetCDF sample data (Jan & Jul)",
    "citation": (
        "Zhang, Y.: Data used in manuscript 'Towards an integrated inventory "
        "of anthropogenic emissions for China', "
        "https://doi.org/10.5281/zenodo.15039737, 2025."
    ),
    "files": {
        "jan": {
            "name": "MEIC201701_SPECIATED_NETCDF.zip",
            "url":  "https://zenodo.org/records/15039737/files/MEIC201701_SPECIATED_NETCDF.zip?download=1",
            "md5":  "eeabbd6001f8b0b20f94a4906e915a79",
            "size": "10.1 MB",
            "month": "January 2017",
        },
        "jul": {
            "name": "MEIC201707_SPECIATED_NETCDF.zip",
            "url":  "https://zenodo.org/records/15039737/files/MEIC201707_SPECIATED_NETCDF.zip?download=1",
            "md5":  "444f8de588525780ed1758e6b3a38fa0",
            "size": "31.8 MB",
            "month": "July 2017",
        },
        "sectoral": {
            "name": "MEIC1.4_sectoral_emission (2008-2020).zip",
            "url":  "https://zenodo.org/records/15039737/files/MEIC1.4_sectoral_emission%20(2008-2020).zip?download=1",
            "md5":  "6b6030c0ee8efc29fdf54cbdc21a7f20",
            "size": "7.5 kB",
            "month": "sectoral totals 2008-2020",
        },
    },
    "species": ["SO2", "NOx", "CO", "BC", "OC", "NH3", "NMVOC", "PM2.5", "PM10"],
    "full_data_url": "http://meicmodel.org.cn/?page_id=1772&lang=en",
}


# ── MEIC public functions ──────────────────────────────────────────────────────

def download_meic_sample(save_dir, months=None, extract=True, keep_zip=False):
    """
    Download MEIC v1.4 sample data (2017) from Zenodo.

    This provides two sample months (January and July 2017) in speciated
    NetCDF format, suitable for testing CINEI workflows.
    For the full multi-year MEIC dataset, use get_meic_info().

    Parameters
    ----------
    save_dir : str
        Directory to save downloaded files.
    months : list of str, optional
        Which months to download. Options: ['jan', 'jul', 'sectoral']
        Default: ['jan', 'jul'] (both sample months)
    extract : bool, optional
        If True, automatically unzip downloaded files. Default True.
    keep_zip : bool, optional
        If True, keep .zip files after extraction. Default False.

    Returns
    -------
    list of str
        Paths to downloaded (and extracted) files.

    Examples
    --------
    >>> import cinei
    >>> # Download both sample months
    >>> cinei.download_meic_sample(save_dir='/work/bb1554/data/MEIC')

    >>> # Download only January
    >>> cinei.download_meic_sample(
    ...     save_dir='/work/bb1554/data/MEIC',
    ...     months=['jan']
    ... )

    >>> # Download sectoral totals only
    >>> cinei.download_meic_sample(
    ...     save_dir='/work/bb1554/data/MEIC',
    ...     months=['sectoral']
    ... )
    """
    save_dir = Path(save_dir)
    save_dir.mkdir(parents=True, exist_ok=True)

    # ── Default months ─────────────────────────────────────────────────
    if months is None:
        months = ["jan", "jul"]

    # ── Validate months ────────────────────────────────────────────────
    valid = list(MEIC_REGISTRY["files"].keys())
    invalid = [m for m in months if m.lower() not in valid]
    if invalid:
        raise ValueError(
            f"[CINEI] Unrecognized month keys: {invalid}\n"
            f"        Available: {valid}"
        )

    print(f"[CINEI] MEIC v1.4 Sample Data Download")
    print(f"[CINEI] Source  : {MEIC_REGISTRY['doi']}")
    print(f"[CINEI] Save to : {save_dir}")
    print(f"[CINEI] Months  : {months}")
    print(f"[CINEI] Species : {MEIC_REGISTRY['species']}")
    print()

    downloaded = []
    for key in months:
        info = MEIC_REGISTRY["files"][key.lower()]
        zip_path = save_dir / info["name"]

        print(f"[CINEI] → {info['month']}  ({info['size']})")

        # ── Download ───────────────────────────────────────────────────
        _download_with_resume(info["url"], zip_path)

        # ── MD5 check ─────────────────────────────────────────────────
        actual_md5 = _md5(zip_path)
        if actual_md5 == info["md5"]:
            print(f"[CINEI]   ✅ MD5 verified")
        else:
            print(f"[CINEI]   ⚠️  MD5 mismatch! File may be corrupted.")
            print(f"[CINEI]      Expected : {info['md5']}")
            print(f"[CINEI]      Got      : {actual_md5}")

        # ── Extract ────────────────────────────────────────────────────
        if extract:
            out_subdir = save_dir / info["name"].replace(".zip", "")
            out_subdir.mkdir(exist_ok=True)
            import zipfile
            with zipfile.ZipFile(zip_path, "r") as z:
                z.extractall(out_subdir)
            print(f"[CINEI]   📂 Extracted to: {out_subdir}")
            downloaded.append(str(out_subdir))

            if not keep_zip:
                os.remove(zip_path)
        else:
            downloaded.append(str(zip_path))

    print(f"\n[CINEI] ✅ Done! Files saved to {save_dir}")
    print(f"\n[CINEI] Citation:")
    print(f"  {MEIC_REGISTRY['citation']}")
    return downloaded


def get_meic_info():
    """
    Print information and instructions for downloading the full MEIC dataset.

    The full MEIC dataset requires registration at the official website.
    This function prints step-by-step instructions.

    Examples
    --------
    >>> import cinei
    >>> cinei.get_meic_info()
    """
    print("=" * 65)
    print("  MEIC — Multi-resolution Emission Inventory for China")
    print("=" * 65)
    print()
    print("  Sample data (2017 Jan & Jul) — publicly available:")
    print(f"  {MEIC_REGISTRY['doi']}")
    print()
    print("  Full dataset — registration required:")
    print(f"  {MEIC_REGISTRY['full_data_url']}")
    print()
    print("  Steps to download full MEIC data:")
    print("  1. Visit the URL above")
    print("  2. Register for an account")
    print("  3. Select species: SO2, NOx, CO, BC, OC, NH3, NMVOC, PM2.5, PM10")
    print("  4. Select sectors: agriculture, industry, power, residential,")
    print("                     transportation")
    print("  5. Download monthly NetCDF files for your target year")
    print("  6. Place files in your MEIC directory, e.g.:")
    print("       /work/bb1554/data/MEIC/2017/")
    print()
    print("  Expected filename pattern after download:")
    print("    *_{month}_*_{species}.nc")
    print("    e.g. agr_Jan_2017_SO2.nc, ind_Jan_2017_NOx.nc")
    print()
    print("  Species available: " + ", ".join(MEIC_REGISTRY["species"]))
    print()
    print("  Citation:")
    print(f"    {MEIC_REGISTRY['citation']}")
    print("=" * 65)
