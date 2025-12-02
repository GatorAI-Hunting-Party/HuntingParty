from __future__ import annotations

from importlib import import_module
from pathlib import Path
import tempfile
from typing import Any, Iterable, Optional

import numpy as np
import pandas as pd


def _import_teammate_gwr():
    candidates = [
        "GWR_Map_Creation.gwr_pipeline",
        "GWR_Map_Creation.gwr_creator",
        "app.gwr.teammate.gwr_pipeline",
        "app.gwr.teammate.gwr_creator",
    ]
    for name in candidates:
        try:
            return import_module(name)
        except Exception:
            continue
    return None


def haversine_miles(lat1: Iterable[float], lon1: Iterable[float], lat2: float, lon2: float) -> np.ndarray:
    lat1_r = np.radians(lat1)
    lon1_r = np.radians(lon1)
    lat2_r = np.radians(lat2)
    lon2_r = np.radians(lon2)
    dlat = lat2_r - lat1_r
    dlon = lon2_r - lon1_r
    a = np.sin(dlat / 2) ** 2 + np.cos(lat1_r) * np.cos(lat2_r) * np.sin(dlon / 2) ** 2
    return 3958.8 * 2 * np.arcsin(np.sqrt(a))


def kernel_weights(dist_mi: np.ndarray, bandwidth_mi: float) -> np.ndarray:
    dist_sq = np.square(dist_mi)
    return np.exp(-(dist_sq) / (bandwidth_mi ** 2 + 1e-9))


def local_linear(y: np.ndarray, X: np.ndarray, w: np.ndarray) -> np.ndarray:
    W = np.diag(w)
    XtWX = X.T @ W @ X
    XtWy = X.T @ W @ y
    try:
        beta = np.linalg.solve(XtWX, XtWy)
    except np.linalg.LinAlgError:
        beta = np.linalg.pinv(XtWX) @ XtWy
    return beta


def gwr_surface(
    df: pd.DataFrame,
    metric_col: str,
    lat_col: str,
    lon_col: str,
    bandwidth_mi: float = 8.0,
    grid_step_mi: float = 2.0,
    extra_cols: Optional[list[str]] = None,
) -> pd.DataFrame:
    df = df.copy()
    df[metric_col] = pd.to_numeric(df.get(metric_col), errors="coerce")
    df[lat_col] = pd.to_numeric(df.get(lat_col), errors="coerce")
    df[lon_col] = pd.to_numeric(df.get(lon_col), errors="coerce")
    df = df.dropna(subset=[metric_col, lat_col, lon_col])
    if df.empty:
        return pd.DataFrame(columns=["lat", "lon", "pred", "n_used"])

    X_base_cols = [c for c in (extra_cols or []) if c in df.columns]
    lat_min, lat_max = df[lat_col].min(), df[lat_col].max()
    lon_min, lon_max = df[lon_col].min(), df[lon_col].max()

    dlat = grid_step_mi / 69.0
    dlon = grid_step_mi / 54.6
    lat_grid = np.arange(lat_min, lat_max + dlat, dlat)
    lon_grid = np.arange(lon_min, lon_max + dlon, dlon)
    points = np.array([(la, lo) for la in lat_grid for lo in lon_grid])

    y = df[metric_col].astype(float).values
    if X_base_cols:
        X_base = np.column_stack([np.ones(len(df))] + [df[c].astype(float).values for c in X_base_cols])
    else:
        X_base = np.ones((len(df), 1))

    lat_arr = df[lat_col].values
    lon_arr = df[lon_col].values
    preds = []
    counts = []
    for (la, lo) in points:
        dist = haversine_miles(lat_arr, lon_arr, la, lo)
        w = kernel_weights(dist, bandwidth_mi)
        if np.sum(w > 1e-6) < 5:
            preds.append(np.nan)
            counts.append(int(np.sum(w > 1e-6)))
            continue
        beta = local_linear(y, X_base, w)
        intercept_vec = np.array([1.0] + [0.0] * len(X_base_cols))
        pred = float(intercept_vec @ beta)
        preds.append(pred)
        counts.append(int(np.sum(w > 1e-3)))

    return pd.DataFrame(
        {
            "lat": points[:, 0],
            "lon": points[:, 1],
            "pred": preds,
            "n_used": counts,
        }
    )


def _teammate_surface_from_df(mod: Any, df: pd.DataFrame, metric_col: str, lat_col: str, lon_col: str, **kwargs) -> pd.DataFrame:
    if hasattr(mod, "build_surface"):
        try:
            params = {"metric_col": metric_col, "lat_col": lat_col, "lon_col": lon_col}
            for key in ("bandwidth_mi", "grid_step_mi", "bandwidth"):
                if key in kwargs:
                    params[key] = kwargs[key]
            return mod.build_surface(df, **params)  # type: ignore[attr-defined]
        except Exception:
            return pd.DataFrame()

    if hasattr(mod, "run_gwr_model"):
        surface = _run_teammate_model(mod, df, metric_col, lat_col, lon_col, **kwargs)
        if surface is not None:
            return surface

    if hasattr(mod, "build_gwr_figure"):
        surface = _run_teammate_map(mod, df, metric_col, lat_col, lon_col, **kwargs)
        if surface is not None:
            return surface

    return pd.DataFrame()


def _run_teammate_model(mod: Any, df: pd.DataFrame, metric_col: str, lat_col: str, lon_col: str, **kwargs) -> Optional[pd.DataFrame]:
    try:
        cleaned = df[[metric_col, lat_col, lon_col]].copy()
    except KeyError:
        return None
    cleaned[metric_col] = pd.to_numeric(cleaned[metric_col], errors="coerce")
    cleaned[lat_col] = pd.to_numeric(cleaned[lat_col], errors="coerce")
    cleaned[lon_col] = pd.to_numeric(cleaned[lon_col], errors="coerce")
    cleaned = cleaned.dropna(subset=[metric_col, lat_col, lon_col])
    if cleaned.empty:
        return None

    gwr_df = pd.DataFrame(
        {
            "cell_id": np.arange(len(cleaned)),
            "centroid_lon": cleaned[lon_col].astype(float).values,
            "centroid_lat": cleaned[lat_col].astype(float).values,
            metric_col: cleaned[metric_col].astype(float).values,
        }
    )

    try:
        preds, _bw = mod.run_gwr_model(gwr_df, metric_col, bandwidth=kwargs.get("bandwidth_mi"))  # type: ignore[attr-defined]
    except Exception:
        return None

    try:
        merged = gwr_df.merge(preds, on="cell_id", how="left")
        surface = pd.DataFrame(
            {
                "lat": merged["centroid_lat"],
                "lon": merged["centroid_lon"],
                "pred": merged.get("gwr_prediction"),
                "n_used": len(gwr_df),
            }
        )
        return surface.dropna(subset=["lat", "lon", "pred"])
    except Exception:
        return None


def _run_teammate_map(mod: Any, df: pd.DataFrame, metric_col: str, lat_col: str, lon_col: str, **kwargs) -> Optional[pd.DataFrame]:
    grid_path = None
    grid_dir = getattr(mod, "CACHED_GEODATA_DIR", None)
    if grid_dir:
        path = Path(grid_dir)
        if path.exists():
            parquet_files = sorted(path.glob("*.parquet"))
            if parquet_files:
                grid_path = parquet_files[0]
    if grid_path is None:
        return None

    tmp_path: Optional[Path] = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as tmp:
            df[[metric_col, lat_col, lon_col]].to_csv(tmp.name, index=False)
            tmp_path = Path(tmp.name)
    except Exception:
        return None

    try:
        fig, grid_gdf, _ = mod.build_gwr_figure(  # type: ignore[attr-defined]
            points=tmp_path,
            target_stat=metric_col,
            grid_path=grid_path,
            lon_col=lon_col,
            lat_col=lat_col,
            bandwidth=kwargs.get("bandwidth_mi"),
            title="GWR Surface",
        )
    except Exception:
        if tmp_path:
            try:
                tmp_path.unlink()
            except Exception:
                pass
        return None

    try:
        return pd.DataFrame(
            {
                "lat": grid_gdf["centroid_lat"],
                "lon": grid_gdf["centroid_lon"],
                "pred": grid_gdf.get(f"projected_{metric_col}", grid_gdf.get("gwr_prediction")),
                "n_used": len(df),
            }
        ).dropna(subset=["lat", "lon", "pred"])
    except Exception:
        return None
    finally:
        if tmp_path:
            try:
                tmp_path.unlink()
            except Exception:
                pass


def run_teammate_gwr(df: pd.DataFrame, **kwargs) -> pd.DataFrame:
    mod = _import_teammate_gwr()
    if mod is None:
        return pd.DataFrame()
    metric_col = kwargs.get("metric_col")
    lat_col = kwargs.get("lat_col")
    lon_col = kwargs.get("lon_col")
    if not metric_col or not lat_col or not lon_col:
        return pd.DataFrame()

    try:
        return _teammate_surface_from_df(mod, df, metric_col=metric_col, lat_col=lat_col, lon_col=lon_col, **kwargs)
    except Exception:
        return pd.DataFrame()
