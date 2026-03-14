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


# ── MEIC filename species mapping ─────────────────────────────────────────────
# Maps canonical species name → exact string used in MEIC filenames
MEIC_SPECIES_FILENAME = {
    "NOX":   "NOx",
    "SO2":   "SO2",
    "CO":    "CO",
    "BC":    "BC",
    "OC":    "OC",
    "NH3":   "NH3",
    "PM25":  "pm25",
    "PM10":  "pm10",
    # VOC speciated files use SAPRC99 or MOZART mechanism — not included here
}

MEIC_SECTORS = ["agriculture", "industry", "power", "residential", "transportation"]

MEIC_MONTHS = {
    1: "01",  2: "02",  3: "03",  4: "04",
    5: "05",  6: "06",  7: "07",  8: "08",
    9: "09", 10: "10", 11: "11", 12: "12",
}


def list_meic_filenames(year, species=None, months=None, sectors=None):
    """
    List expected MEIC filenames for given year, species, months, sectors.

    Useful to verify your downloaded MEIC files match the expected naming.

    Parameters
    ----------
    year : int or str
        Target year, e.g. 2017
    species : list of str, optional
        Species list, e.g. ['NOx', 'SO2']. Default: all species.
        Case-insensitive. Available: NOx, SO2, CO, BC, OC, NH3, PM2.5, PM10
    months : list of int, optional
        Month numbers 1-12. Default: all 12 months.
        e.g. [1, 7] for January and July only.
    sectors : list of str, optional
        Sector names. Default: all 5 sectors.
        Available: agriculture, industry, power, residential, transportation

    Returns
    -------
    list of str
        Expected MEIC filenames.

    Examples
    --------
    >>> import cinei
    >>> # List all expected files for 2017 NOx, January only
    >>> cinei.list_meic_filenames(2017, species=['NOx'], months=[1])
    ['2017_01_agriculture_NOx.nc',
     '2017_01_industry_NOx.nc',
     '2017_01_power_NOx.nc',
     '2017_01_residential_NOx.nc',
     '2017_01_transportation_NOx.nc']
    """
    if species is None:
        sp_keys = list(MEIC_SPECIES_FILENAME.keys())
    else:
        sp_keys = _normalize_meic_species(species)

    if months is None:
        months = list(range(1, 13))

    if sectors is None:
        sectors = MEIC_SECTORS

    # Validate months
    invalid_months = [m for m in months if m not in range(1, 13)]
    if invalid_months:
        raise ValueError(
            f"[CINEI] Invalid month numbers: {invalid_months}\n"
            f"        Expected integers 1-12."
        )

    # Validate sectors
    invalid_sectors = [s for s in sectors if s not in MEIC_SECTORS]
    if invalid_sectors:
        raise ValueError(
            f"[CINEI] Invalid sectors: {invalid_sectors}\n"
            f"        Available: {MEIC_SECTORS}"
        )

    filenames = []
    for sp_key in sp_keys:
        sp_str = MEIC_SPECIES_FILENAME[sp_key]
        for mon in sorted(months):
            mon_str = MEIC_MONTHS[mon]
            for sector in sectors:
                filenames.append(f"{year}_{mon_str}_{sector}_{sp_str}.nc")

    return filenames


def check_meic_files(meic_dir, year, species=None, months=None, sectors=None):
    """
    Check which expected MEIC files are present or missing in a directory.

    Parameters
    ----------
    meic_dir : str
        Path to directory containing MEIC NetCDF files.
    year : int or str
        Target year, e.g. 2017
    species : list of str, optional
        Species to check. Default: all species.
    months : list of int, optional
        Months 1-12 to check. Default: all 12 months.
    sectors : list of str, optional
        Sectors to check. Default: all 5 sectors.

    Returns
    -------
    dict with keys 'found', 'missing'
        Each value is a list of filenames.

    Examples
    --------
    >>> import cinei
    >>> result = cinei.check_meic_files(
    ...     meic_dir='/work/bb1554/data/MEIC/2017',
    ...     year=2017,
    ...     species=['NOx', 'SO2'],
    ...     months=[1, 7]
    ... )
    >>> print(result['missing'])
    """
    meic_dir = Path(meic_dir)
    if not meic_dir.exists():
        raise FileNotFoundError(
            f"[CINEI] MEIC directory not found: {meic_dir}"
        )

    expected = list_meic_filenames(year, species, months, sectors)
    found    = [f for f in expected if (meic_dir / f).exists()]
    missing  = [f for f in expected if not (meic_dir / f).exists()]

    print(f"[CINEI] MEIC file check: {meic_dir}")
    print(f"[CINEI] Year    : {year}")
    print(f"[CINEI] Species : {species if species else 'ALL'}")
    print(f"[CINEI] Months  : {months if months else 'ALL (1-12)'}")
    print(f"[CINEI] Sectors : {sectors if sectors else 'ALL'}")
    print()
    print(f"[CINEI] ✅ Found  : {len(found):>3} / {len(expected)} files")
    print(f"[CINEI] ❌ Missing: {len(missing):>3} / {len(expected)} files")

    if missing:
        print(f"\n[CINEI] Missing files:")
        for f in missing:
            print(f"          {f}")

    return {"found": found, "missing": missing}


def _normalize_meic_species(species_list):
    """Normalize user species input to canonical MEIC keys."""
    # Reuse CEDS normalization but map to MEIC keys
    alias = {
        "NOX": "NOX", "NOX": "NOX", "NOx": "NOX",
        "SO2": "SO2",
        "CO":  "CO",
        "BC":  "BC",
        "OC":  "OC",
        "NH3": "NH3",
        "PM2.5": "PM25", "PM25": "PM25", "pm25": "PM25", "pm2.5": "PM25",
        "PM10":  "PM10", "pm10": "PM10",
    }
    normalized = []
    unrecognized = []
    for sp in species_list:
        key = alias.get(sp.strip(), alias.get(sp.strip().upper()))
        if key and key in MEIC_SPECIES_FILENAME:
            normalized.append(key)
        else:
            unrecognized.append(sp)
    if unrecognized:
        raise ValueError(
            f"[CINEI] Unrecognized MEIC species: {unrecognized}\n"
            f"        Available: {list(MEIC_SPECIES_FILENAME.keys())}"
        )
    return normalized


# ── HTAP Registry ─────────────────────────────────────────────────────────────
HTAP_REGISTRY = {
    "doi":         "https://doi.org/10.5281/zenodo.7516361",
    "zenodo_id":   "7516361",
    "version":     "HTAP v3",
    "description": "HTAP v3 emission mosaic, 2000-2018, monthly, 0.1°/0.5°",
    "citation": (
        "Crippa, M. et al.: HTAP_v3 emission mosaic, "
        "https://doi.org/10.5281/zenodo.7516361, 2023."
    ),
    "resolutions": ["01x01", "05x05"],
    "types":       ["emissions", "fluxes"],
    "species":     ["BC", "CO", "NH3", "NMVOC", "NOx", "OC", "PM10", "PM2.5", "SO2"],
    # MD5 checksums from Zenodo
    "files": {
        # 0.1° emissions
        ("01x01", "emissions", "BC"):    {"md5": "0c4fa152b6f98d36628fb0f3c30bcf56", "size": "8.3 GB"},
        ("01x01", "emissions", "CO"):    {"md5": "5a6defc1d94418515d327eb0380d205a", "size": "7.8 GB"},
        ("01x01", "emissions", "NH3"):   {"md5": "6ce68610f6d8c1ef965feeae231b35f5", "size": "10.1 GB"},
        ("01x01", "emissions", "NMVOC"): {"md5": "93a103ff0bdafb3e7eb7f6babee43893", "size": "13.0 GB"},
        ("01x01", "emissions", "NOx"):   {"md5": "ab5c26c62d44602dc31b1d23e662a28b", "size": "10.3 GB"},
        ("01x01", "emissions", "OC"):    {"md5": "47c72ca7861fcdf1f6254ac7264f207b", "size": "8.5 GB"},
        ("01x01", "emissions", "PM10"):  {"md5": "6e6e8122154d4b360f4c8c84be90e316", "size": "10.9 GB"},
        ("01x01", "emissions", "PM2.5"): {"md5": "e87807a5be65ae516335573e713a84bc", "size": "10.9 GB"},
        ("01x01", "emissions", "SO2"):   {"md5": "6eae0ad4f5855caeb75bcd70be72a329", "size": "7.7 GB"},
        # 0.1° fluxes
        ("01x01", "fluxes", "BC"):    {"md5": "f14029934cceb0f4f7e36e76ce6b4bf6", "size": "9.0 GB"},
        ("01x01", "fluxes", "CO"):    {"md5": "ab4555583c19b89b168382f333d6c0ee", "size": "8.4 GB"},
        ("01x01", "fluxes", "NH3"):   {"md5": "d0f037e2348a2c9868a81806e7e96ab2", "size": "10.9 GB"},
        ("01x01", "fluxes", "NMVOC"): {"md5": "35c60639a4a4ea05f883a8312a1033c3", "size": "13.9 GB"},
        ("01x01", "fluxes", "NOx"):   {"md5": "2716e831648553a4d01a9e8890ef781a", "size": "11.0 GB"},
        ("01x01", "fluxes", "OC"):    {"md5": "b1efc20d00c91b8fd7cdda12f84de4c7", "size": "9.2 GB"},
        ("01x01", "fluxes", "PM10"):  {"md5": "f5f2e47a616dced4451798633c3385bb", "size": "11.6 GB"},
        ("01x01", "fluxes", "PM2.5"): {"md5": "e562373e2e80ed0ecda742832c7bc3b1", "size": "11.6 GB"},
        ("01x01", "fluxes", "SO2"):   {"md5": "76a985fd05fb4a5ffb73cf187192d1cf", "size": "8.4 GB"},
        # 0.5° emissions
        ("05x05", "emissions", "BC"):    {"md5": "f45164efab4c8088974cde92b6f444ac", "size": "608 MB"},
        ("05x05", "emissions", "CO"):    {"md5": "faaf48dde4edac0a214a0bfcf597811d", "size": "561 MB"},
        ("05x05", "emissions", "NH3"):   {"md5": "122120b7799084e00b346fa6bfe5941f", "size": "689 MB"},
        ("05x05", "emissions", "NMVOC"): {"md5": "66b03d57e9ddbe5226a9912072e2c626", "size": "839 MB"},
        ("05x05", "emissions", "NOx"):   {"md5": "2243aae1710ad4fb2b9a457d250ac524", "size": "681 MB"},
        ("05x05", "emissions", "OC"):    {"md5": "4fefd0a54b048b7f66b3a0558f3237d0", "size": "616 MB"},
        ("05x05", "emissions", "PM10"):  {"md5": "b04afd121ea623dcd7ae86209fd3220c", "size": "744 MB"},
        ("05x05", "emissions", "PM2.5"): {"md5": "66f8454eb79fb4056157976d0ab1e415", "size": "744 MB"},
        ("05x05", "emissions", "SO2"):   {"md5": "d9c23549191f6bc559bc39886f890537", "size": "556 MB"},
        # 0.5° fluxes
        ("05x05", "fluxes", "BC"):    {"md5": "108d559ef838e237ce82c560b9aebe43", "size": "611 MB"},
        ("05x05", "fluxes", "CO"):    {"md5": "551dd28dfdc634cbc29022cef507399e", "size": "563 MB"},
        ("05x05", "fluxes", "NH3"):   {"md5": "46fd6667e5e879685af97ea97184dbcf", "size": "692 MB"},
        ("05x05", "fluxes", "NMVOC"): {"md5": "69d99c9fb30066f4c1d1ed8cfa9d7273", "size": "842 MB"},
        ("05x05", "fluxes", "NOx"):   {"md5": "ad6f4ea3fb8ccfe7dbb61d49cff59baa", "size": "683 MB"},
        ("05x05", "fluxes", "OC"):    {"md5": "d8d8d010e8bf643e9a4df4281542b3d9", "size": "619 MB"},
        ("05x05", "fluxes", "PM10"):  {"md5": "f5f2e47a616dced4451798633c3385bb", "size": "747 MB"},
        ("05x05", "fluxes", "PM2.5"): {"md5": "5574c700d4601123e1c2c7dd1ce59550", "size": "746 MB"},
        ("05x05", "fluxes", "SO2"):   {"md5": "694f18fb209bbf28fc4dfa3fc833737a", "size": "559 MB"},
    },
}

# HTAP species name variants (user input → canonical filename)
HTAP_SPECIES_VARIANTS = {
    "BC":    ["BC", "bc"],
    "CO":    ["CO", "co"],
    "NH3":   ["NH3", "nh3"],
    "NMVOC": ["NMVOC", "nmvoc", "VOC", "voc"],
    "NOX":   ["NOx", "nox", "NOX"],
    "OC":    ["OC", "oc"],
    "PM10":  ["PM10", "pm10"],
    "PM25":  ["PM2.5", "pm2.5", "PM25", "pm25"],
    "SO2":   ["SO2", "so2"],
}

# Map canonical key → filename species string
HTAP_SPECIES_FILENAME = {
    "BC": "BC", "CO": "CO", "NH3": "NH3", "NMVOC": "NMVOC",
    "NOX": "NOx", "OC": "OC", "PM10": "PM10", "PM25": "PM2.5", "SO2": "SO2",
}


def download_htap(save_dir, species=None, resolution="05x05",
                  data_type="emissions", extract=True, keep_zip=False):
    """
    Download HTAP v3 gridded emission data from Zenodo.

    Coverage: 2000-2018, monthly, 9 species, 16 sectors.
    Each NetCDF file contains all 12 months and all sectors for one year.

    Parameters
    ----------
    save_dir : str
        Directory to save downloaded files.
    species : list of str, optional
        Species to download. Case-insensitive.
        e.g. ['NOx', 'SO2'] or ['nox', 'so2'] or ['NOX', 'PM2.5']
        Default: all 9 species.
        Available: BC, CO, NH3, NMVOC, NOx, OC, PM10, PM2.5, SO2
    resolution : str, optional
        Spatial resolution. Options:
        - '05x05' : 0.5° x 0.5° (~500-800 MB per species) [default]
        - '01x01' : 0.1° x 0.1° (~8-13 GB per species)
    data_type : str, optional
        Data type. Options:
        - 'emissions' : Mg/month  [default]
        - 'fluxes'    : kg/m2/s
    extract : bool, optional
        If True, automatically unzip after download. Default True.
    keep_zip : bool, optional
        If True, keep .zip files after extraction. Default False.

    Returns
    -------
    list of str
        Paths to downloaded (and extracted) files/directories.

    Examples
    --------
    >>> import cinei
    >>> # Download NOx and SO2 at 0.5° resolution (recommended)
    >>> cinei.download_htap(
    ...     save_dir='/work/bb1554/data/HTAP',
    ...     species=['NOx', 'SO2'],
    ...     resolution='05x05'
    ... )

    >>> # Download all species at 0.1° (warning: very large ~90 GB)
    >>> cinei.download_htap(
    ...     save_dir='/work/bb1554/data/HTAP',
    ...     resolution='01x01'
    ... )
    """
    save_dir = Path(save_dir)
    save_dir.mkdir(parents=True, exist_ok=True)

    # ── Validate resolution ────────────────────────────────────────────
    if resolution not in HTAP_REGISTRY["resolutions"]:
        raise ValueError(
            f"[CINEI] Invalid resolution: '{resolution}'\n"
            f"        Available: {HTAP_REGISTRY['resolutions']}\n"
            f"        Tip: use '05x05' (0.5°) to save disk space."
        )

    # ── Validate data_type ─────────────────────────────────────────────
    if data_type not in HTAP_REGISTRY["types"]:
        raise ValueError(
            f"[CINEI] Invalid data_type: '{data_type}'\n"
            f"        Available: {HTAP_REGISTRY['types']}"
        )

    # ── Normalize species ──────────────────────────────────────────────
    if species is None:
        sp_keys = list(HTAP_SPECIES_FILENAME.keys())
    else:
        sp_keys = _normalize_htap_species(species)

    # ── Estimate total size ────────────────────────────────────────────
    total_info = [
        HTAP_REGISTRY["files"][(resolution, data_type, HTAP_SPECIES_FILENAME[k])]
        for k in sp_keys
    ]

    print(f"[CINEI] HTAP v3 Download")
    print(f"[CINEI] Source     : {HTAP_REGISTRY['doi']}")
    print(f"[CINEI] Save to    : {save_dir}")
    print(f"[CINEI] Resolution : {resolution.replace('x', '° x ')}°")
    print(f"[CINEI] Data type  : {data_type} "
          f"({'Mg/month' if data_type == 'emissions' else 'kg/m²/s'})")
    print(f"[CINEI] Species    : {[HTAP_SPECIES_FILENAME[k] for k in sp_keys]}")
    print(f"[CINEI] File sizes : "
          f"{', '.join(i['size'] for i in total_info)}")
    print()

    downloaded = []
    for sp_key in sp_keys:
        sp_str  = HTAP_SPECIES_FILENAME[sp_key]
        fname   = f"gridmaps_{resolution}_{data_type}_{sp_str}.zip"
        url     = (f"https://zenodo.org/records/7516361/files/"
                   f"{fname}?download=1")
        zip_path = save_dir / fname
        info    = HTAP_REGISTRY["files"][(resolution, data_type, sp_str)]

        print(f"[CINEI] → {sp_str}  ({info['size']})")

        # ── Download with resume ───────────────────────────────────────
        _download_with_resume(url, zip_path)

        # ── MD5 check ─────────────────────────────────────────────────
        actual_md5 = _md5(zip_path)
        if actual_md5 == info["md5"]:
            print(f"[CINEI]   ✅ MD5 verified")
        else:
            print(f"[CINEI]   ⚠️  MD5 mismatch!")
            print(f"[CINEI]      Expected : {info['md5']}")
            print(f"[CINEI]      Got      : {actual_md5}")

        # ── Extract ────────────────────────────────────────────────────
        if extract:
            import zipfile
            out_subdir = save_dir / fname.replace(".zip", "")
            out_subdir.mkdir(exist_ok=True)
            print(f"[CINEI]   📂 Extracting to: {out_subdir.name}/")
            with zipfile.ZipFile(zip_path, "r") as z:
                z.extractall(out_subdir)
            if not keep_zip:
                os.remove(zip_path)
            downloaded.append(str(out_subdir))
        else:
            downloaded.append(str(zip_path))

    print(f"\n[CINEI] ✅ Done! {len(downloaded)} species downloaded.")
    print(f"\n[CINEI] Citation:")
    print(f"  {HTAP_REGISTRY['citation']}")
    return downloaded


def list_htap_files(resolution="05x05", data_type="emissions", species=None):
    """
    List available HTAP v3 files with sizes.

    Parameters
    ----------
    resolution : str
        '05x05' or '01x01'
    data_type : str
        'emissions' or 'fluxes'
    species : list of str, optional
        Filter by species. Default: all.

    Examples
    --------
    >>> import cinei
    >>> cinei.list_htap_files(resolution='05x05', data_type='emissions')
    """
    if species is None:
        sp_keys = list(HTAP_SPECIES_FILENAME.keys())
    else:
        sp_keys = _normalize_htap_species(species)

    res_label = resolution.replace("x", "° x ") + "°"
    print(f"[CINEI] HTAP v3 — {res_label}  {data_type}")
    print(f"[CINEI] {'Species':<10} {'Filename':<45} {'Size':>10}")
    print(f"[CINEI] {'-'*65}")
    for sp_key in sp_keys:
        sp_str = HTAP_SPECIES_FILENAME[sp_key]
        fname  = f"gridmaps_{resolution}_{data_type}_{sp_str}.zip"
        info   = HTAP_REGISTRY["files"][(resolution, data_type, sp_str)]
        print(f"[CINEI] {sp_str:<10} {fname:<45} {info['size']:>10}")


def _normalize_htap_species(species_list):
    """Normalize user species input to canonical HTAP keys."""
    normalized = []
    unrecognized = []
    for sp in species_list:
        sp_str = sp.strip()
        matched = None
        for key, variants in HTAP_SPECIES_VARIANTS.items():
            if sp_str in variants or sp_str.upper() == key:
                matched = key
                break
        if matched:
            normalized.append(matched)
        else:
            unrecognized.append(sp)
    if unrecognized:
        raise ValueError(
            f"[CINEI] Unrecognized HTAP species: {unrecognized}\n"
            f"        Available: {HTAP_REGISTRY['species']}"
        )
    return normalized


# ── EDGAR Registry ────────────────────────────────────────────────────────────
EDGAR_REGISTRY = {
    "base_url": (
        "https://jeodpp.jrc.ec.europa.eu/ftp/jrc-opendata/EDGAR/datasets"
    ),
    "dataset":  "v81_FT2022_AP_new",
    "version":  "v8.1",
    "doi":      "https://edgar.jrc.ec.europa.eu/dataset_ap81",
    "coverage": "1970-2022",
    "resolution": "0.1° x 0.1°",
    "description": (
        "EDGAR v8.1 global air pollutant emissions, "
        "monthly NetCDF, 0.1°, 1970-2022"
    ),
    "citation": (
        "Crippa, M. et al.: EDGAR v8.1 Global Air Pollutant Emissions, "
        "European Commission, Joint Research Centre (JRC), "
        "https://edgar.jrc.ec.europa.eu/dataset_ap81, 2024."
    ),
    # Exact species strings used in EDGAR filenames
    "species_filename": {
        "BC":    "BC",
        "CO":    "CO",
        "NH3":   "NH3",
        "NMVOC": "NMVOC",
        "NOX":   "NOx",
        "OC":    "OC",
        "PM10":  "PM10",
        "PM25":  "PM2.5",
        "SO2":   "SO2",
    },
    # data_type → subfolder and file suffix
    "types": {
        "fluxes":    {"folder": "flx_nc", "suffix": "flx_nc"},   # kg/m2/s
        "emissions": {"folder": "emi_nc", "suffix": "emi_nc"},   # Mg/month
    },
}

# EDGAR species variants (case-insensitive user input → canonical key)
EDGAR_SPECIES_VARIANTS = {
    "BC":    ["BC", "bc"],
    "CO":    ["CO", "co"],
    "NH3":   ["NH3", "nh3"],
    "NMVOC": ["NMVOC", "nmvoc", "VOC", "voc"],
    "NOX":   ["NOx", "nox", "NOX"],
    "OC":    ["OC", "oc"],
    "PM10":  ["PM10", "pm10"],
    "PM25":  ["PM2.5", "pm2.5", "PM25", "pm25"],
    "SO2":   ["SO2", "so2"],
}


def download_edgar(save_dir, species=None, years=None,
                   data_type="fluxes", extract=True, keep_zip=False):
    """
    Download EDGAR v8.1 gridded air pollutant emission data from JRC FTP.

    Coverage: 1970-2022, monthly, 0.1° x 0.1° resolution, 9 species.
    Each NetCDF file contains one year with 12 months and all sectors.

    Parameters
    ----------
    save_dir : str
        Directory to save downloaded files.
    species : list of str, optional
        Species to download. Case-insensitive.
        e.g. ['NOx', 'SO2'] or ['nox', 'so2'] or ['PM2.5']
        Default: all 9 species.
        Available: BC, CO, NH3, NMVOC, NOx, OC, PM10, PM2.5, SO2
    years : list of int, optional
        Years to download. Range: 1970-2022.
        e.g. [2015, 2016, 2017] or list(range(2010, 2018))
        Default: [2017]  (single year)
    data_type : str, optional
        Data type. Options:
        - 'fluxes'    : kg/m2/s  [default]
        - 'emissions' : Mg/month
    extract : bool, optional
        If True, automatically unzip after download. Default True.
    keep_zip : bool, optional
        If True, keep .zip files after extraction. Default False.

    Returns
    -------
    list of str
        Paths to downloaded (and extracted) files.

    Examples
    --------
    >>> import cinei
    >>> # Download NOx and SO2 for 2017
    >>> cinei.download_edgar(
    ...     save_dir='/work/bb1554/data/EDGAR',
    ...     species=['NOx', 'SO2'],
    ...     years=[2017]
    ... )

    >>> # Download all species for 2015-2017
    >>> cinei.download_edgar(
    ...     save_dir='/work/bb1554/data/EDGAR',
    ...     years=list(range(2015, 2018))
    ... )
    """
    save_dir = Path(save_dir)
    save_dir.mkdir(parents=True, exist_ok=True)

    # ── Defaults ──────────────────────────────────────────────────────
    if years is None:
        years = [2017]
    if species is None:
        sp_keys = list(EDGAR_REGISTRY["species_filename"].keys())
    else:
        sp_keys = _normalize_edgar_species(species)

    # ── Validate data_type ─────────────────────────────────────────────
    if data_type not in EDGAR_REGISTRY["types"]:
        raise ValueError(
            f"[CINEI] Invalid data_type: '{data_type}'\n"
            f"        Available: {list(EDGAR_REGISTRY['types'].keys())}"
        )

    # ── Validate years ─────────────────────────────────────────────────
    invalid_years = [y for y in years if not (1970 <= y <= 2022)]
    if invalid_years:
        raise ValueError(
            f"[CINEI] Invalid years: {invalid_years}\n"
            f"        EDGAR v8.1 coverage: 1970-2022"
        )

    type_info = EDGAR_REGISTRY["types"][data_type]
    unit = "kg/m²/s" if data_type == "fluxes" else "Mg/month"

    print(f"[CINEI] EDGAR v8.1 Download")
    print(f"[CINEI] Source     : {EDGAR_REGISTRY['doi']}")
    print(f"[CINEI] Save to    : {save_dir}")
    print(f"[CINEI] Resolution : {EDGAR_REGISTRY['resolution']}")
    print(f"[CINEI] Data type  : {data_type} ({unit})")
    print(f"[CINEI] Species    : "
          f"{[EDGAR_REGISTRY['species_filename'][k] for k in sp_keys]}")
    print(f"[CINEI] Years      : {years}")
    print(f"[CINEI] Files      : {len(sp_keys) * len(years)} total")
    print()

    downloaded = []
    for sp_key in sp_keys:
        sp_str = EDGAR_REGISTRY["species_filename"][sp_key]
        for year in sorted(years):
            fname = (
                f"v8.1_FT2022_AP_{sp_str}_{year}"
                f"_TOTALS_{type_info['suffix']}.zip"
            )
            url = (
                f"{EDGAR_REGISTRY['base_url']}/"
                f"{EDGAR_REGISTRY['dataset']}/"
                f"{sp_str}/TOTALS/{type_info['folder']}/{fname}"
            )
            zip_path = save_dir / fname

            print(f"[CINEI] → {sp_str}  {year}")

            # ── Download with resume ───────────────────────────────────
            _download_with_resume(url, zip_path)

            # ── Extract ────────────────────────────────────────────────
            if extract:
                import zipfile
                out_subdir = save_dir / fname.replace(".zip", "")
                out_subdir.mkdir(exist_ok=True)
                print(f"[CINEI]   📂 Extracting to: {out_subdir.name}/")
                with zipfile.ZipFile(zip_path, "r") as z:
                    z.extractall(out_subdir)
                if not keep_zip:
                    os.remove(zip_path)
                downloaded.append(str(out_subdir))
            else:
                downloaded.append(str(zip_path))

    print(f"\n[CINEI] ✅ Done! {len(downloaded)} file(s) downloaded.")
    print(f"\n[CINEI] Citation:")
    print(f"  {EDGAR_REGISTRY['citation']}")
    return downloaded


def list_edgar_species():
    """Print available EDGAR v8.1 species."""
    print("[CINEI] Available EDGAR v8.1 species (case-insensitive):")
    print(f"  {'Input':<10} → {'Filename':<10}  Coverage")
    print(f"  {'-'*45}")
    for key, fname in EDGAR_REGISTRY["species_filename"].items():
        variants = EDGAR_SPECIES_VARIANTS.get(key, [])
        print(f"  {fname:<10}   also accepted: "
              f"{[v for v in variants if v != fname]}")
    print(f"\n  Year coverage : {EDGAR_REGISTRY['coverage']}")
    print(f"  Resolution    : {EDGAR_REGISTRY['resolution']}")


def _normalize_edgar_species(species_list):
    """Normalize user species input to canonical EDGAR keys."""
    normalized = []
    unrecognized = []
    for sp in species_list:
        sp_str = sp.strip()
        matched = None
        for key, variants in EDGAR_SPECIES_VARIANTS.items():
            if sp_str in variants or sp_str.upper() == key:
                matched = key
                break
        if matched:
            normalized.append(matched)
        else:
            unrecognized.append(sp)
    if unrecognized:
        raise ValueError(
            f"[CINEI] Unrecognized EDGAR species: {unrecognized}\n"
            f"        Available: {list(EDGAR_REGISTRY['species_filename'].keys())}\n"
            f"        Call cinei.list_edgar_species() to see all options."
        )
    return normalized
