from __future__ import annotations
from pathlib import Path
from typing import Optional, Union
import pickle

import geopandas as gpd
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio
from scipy.spatial import cKDTree
from shapely.geometry import Point
from sklearn.preprocessing import StandardScaler

from mgwr.sel_bw import Sel_BW
from mgwr.gwr import GWR

PROJECT_DIRECTORY = Path(__file__).parent.parent.resolve()
CACHED_GEODATA_DIR = PROJECT_DIRECTORY / "cached_geodata"

def load_points_table(points: Union[str, Path, pd.DataFrame], target_stat: str,
                      lon_col: str = "longitude", lat_col: str = "latitude") -> pd.DataFrame:
    if isinstance(points, (str, Path)):
        path = Path(points)
        if path.suffix.lower() in {".parquet", ".pq"}:
            table = pd.read_parquet(path)
        else:
            table = pd.read_csv(path)
    else:
        table = points.copy()

    if lon_col != "longitude" or lat_col != "latitude":
        table = table.rename(columns={lon_col: "longitude", lat_col: "latitude"})

    required = ["longitude", "latitude", target_stat]
    return table[required].dropna(subset=required)


def ensure_centroids(grid_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    if {"centroid_lon", "centroid_lat"}.issubset(grid_gdf.columns):
        return grid_gdf.to_crs("EPSG:4326")

    try:
        projected = grid_gdf.to_crs(grid_gdf.estimate_utm_crs())
    except Exception:
        projected = grid_gdf.to_crs("EPSG:3857")

    centroids = projected.geometry.centroid.to_crs("EPSG:4326")
    geographic = grid_gdf.to_crs("EPSG:4326").copy()
    geographic["centroid_lon"] = centroids.x
    geographic["centroid_lat"] = centroids.y
    return geographic


def load_grid_dataset(grid_path: Union[str, Path]) -> gpd.GeoDataFrame:
    grid = gpd.read_parquet(str(grid_path))
    return ensure_centroids(grid)


def aggregate_points_to_grid(points_df: pd.DataFrame, grid_gdf: gpd.GeoDataFrame,
                             target_stat: str) -> gpd.GeoDataFrame:
    points_gdf = gpd.GeoDataFrame(
        points_df,
        geometry=[Point(xy) for xy in zip(points_df["longitude"], points_df["latitude"])],
        crs="EPSG:4326",
    ).to_crs(grid_gdf.crs)

    join = gpd.sjoin(points_gdf, grid_gdf, how="inner", predicate="within")
    cell_values = join.groupby("cell_id")[target_stat].mean().reset_index()
    return grid_gdf.merge(cell_values, on="cell_id", how="left")


def prepare_gwr_table(grid_with_values: gpd.GeoDataFrame, target_stat: str) -> pd.DataFrame:
    required = ["cell_id", "centroid_lon", "centroid_lat", target_stat]
    subset = grid_with_values[required].copy()

    mask = (
        np.isfinite(subset["centroid_lon"].to_numpy(dtype=float))
        & np.isfinite(subset["centroid_lat"].to_numpy(dtype=float))
        & np.isfinite(subset[target_stat].to_numpy(dtype=float))
    )
    gwr_df = subset.loc[mask].copy()

    if len(gwr_df) < 5:
        raise ValueError("Need at least five valid grid cells to fit GWR.")

    try:
        gwr_df = gwr_df.drop(gwr_df[target_stat].idxmax())
    except Exception:
        pass

    return gwr_df


def run_gwr_model(gwr_df: pd.DataFrame, target_stat: str, bandwidth: Optional[float] = None,
                  kernel: str = "bisquare", fixed: bool = False) -> tuple[pd.DataFrame, float]:
    y = gwr_df[target_stat].to_numpy(dtype=float).reshape(-1, 1)
    coords = gwr_df[["centroid_lon", "centroid_lat"]].to_numpy(dtype=float)

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(coords)

    if bandwidth is None:
        selector = Sel_BW(coords, y, X_scaled, spherical=False, n_jobs=1)
        bandwidth = float(selector.search())

    model = GWR(coords, y, X_scaled, bandwidth, fixed=fixed, kernel=kernel, constant=True)
    results = model.fit()
    preds = model.predict(coords, X_scaled).predictions.ravel()

    output = gwr_df[["cell_id"]].copy()
    output["gwr_prediction"] = preds
    output["coeff_intercept"] = results.params[:, 0]
    output["coeff_lon"] = results.params[:, 1]
    output["coeff_lat"] = results.params[:, 2]
    return output, bandwidth


def merge_predictions(grid_gdf: gpd.GeoDataFrame, predictions: pd.DataFrame,
                      target_stat: str) -> tuple[gpd.GeoDataFrame, str]:
    projected_stat = f"projected_{target_stat}"
    merged = grid_gdf.merge(predictions, on="cell_id", how="left")
    merged[projected_stat] = merged["gwr_prediction"]
    return merged, projected_stat


def impute_missing_projection(grid_gdf: gpd.GeoDataFrame, projected_stat: str) -> gpd.GeoDataFrame:
    if not grid_gdf[projected_stat].isna().any():
        return grid_gdf

    known_mask = grid_gdf[projected_stat].notna()
    if not known_mask.any():
        return grid_gdf

    tree = cKDTree(grid_gdf.loc[known_mask, ["centroid_lon", "centroid_lat"]].to_numpy())
    unknown_idx = grid_gdf.index[~known_mask]
    unknown_coords = grid_gdf.loc[unknown_idx, ["centroid_lon", "centroid_lat"]].to_numpy()
    _, nearest = tree.query(unknown_coords, k=1)
    grid_gdf.loc[unknown_idx, projected_stat] = (
        grid_gdf.loc[known_mask].iloc[nearest][projected_stat].values
    )
    return grid_gdf


def build_map_figure(grid_gdf: gpd.GeoDataFrame, projected_stat: str,
                    center: Optional[dict[str, float]] = None, zoom: float = 6.0, title: Optional[str] = None,
                    user_frienly = False) -> go.Figure:
    if center is None:
        minx, miny, maxx, maxy = grid_gdf.total_bounds
        center = {"lat": (miny + maxy) / 2.0, "lon": (minx + maxx) / 2.0}

    value_min = float(grid_gdf[projected_stat].min())
    value_max = float(grid_gdf[projected_stat].quantile(0.95))

    if not user_frienly:
        fig = px.choropleth_map(
            grid_gdf,
            geojson=grid_gdf.__geo_interface__,
            locations="cell_id",
            featureidkey="properties.cell_id",
            color=projected_stat,
            color_continuous_scale=["white", "yellow", "orange", "red"],
            range_color=(value_min, value_max),
            map_style="carto-darkmatter",
            center=center,
            zoom=zoom,
            title=title,
        )
    else:
        template = '<span style="font-size:16px; font-weight:bold;">%{customdata:,.2f}</span><extra></extra>'

        fig = px.choropleth_map(
            grid_gdf,
            geojson =grid_gdf.__geo_interface__,
            locations ="cell_id",
            featureidkey ="properties.cell_id",
            color = projected_stat,
            custom_data = [projected_stat],
            hovertemplate = template,
            color_continuous_scale = ["white", "yellow", "orange", "red"],
            range_color =(value_min, value_max),
            map_style ="carto-positron",
            center =center,
            zoom =zoom,
            title =title.replace("_", " ").title(),
        )
    
    fig.update_traces(marker_line_width=0)
    fig.update_layout(margin={"r": 0, "t": 40, "l": 0, "b": 0})
    return fig


def build_gwr_figure(points: Union[str, Path, pd.DataFrame], target_stat: str,
                     grid_path: Union[str, Path], lon_col: str = "longitude",
                     lat_col: str = "latitude", bandwidth: Optional[float] = None,
                     center: Optional[dict[str, float]] = None, zoom: float = 6.0,
                     title: Optional[str] = None) -> tuple[go.Figure, gpd.GeoDataFrame, float]:
    
    points_df = load_points_table(points, target_stat, lon_col=lon_col, lat_col=lat_col)
    grid_gdf = load_grid_dataset(grid_path)
    grid_with_values = aggregate_points_to_grid(points_df, grid_gdf, target_stat)

    gwr_table = prepare_gwr_table(grid_with_values, target_stat)
    predictions, used_bandwidth = run_gwr_model(gwr_table, target_stat, bandwidth=bandwidth)

    projected_grid, projected_stat = merge_predictions(grid_with_values, predictions, target_stat)
    projected_grid = impute_missing_projection(projected_grid, projected_stat)

    fig = build_map_figure(
        projected_grid,
        projected_stat,
        center=center,
        zoom=zoom,
        title=title or f"GWR Projected {target_stat}"
    )

    return fig, projected_grid, used_bandwidth

def save_as_pickle(obj: object, file_path: Union[str, Path]) -> bool:
    successful = True

    try:
        with open(file_path, 'wb') as file:
            pickle.dump(obj, file)
    except Exception as e:
        successful = False
    
    return successful

def save_as_html(fig: go.Figure, file_path: Union[str, Path]) -> bool:
    successful = True

    try:
        fig.write_html(file_path)
    except Exception as e:
        successful = False
    
    return successful    


def get_new_york_html(square_side: int, target_stat) -> Optional[Path]:
    file = CACHED_GEODATA_DIR / f"new_york_{target_stat}_{square_side}.html"
    
    if not file.exists():
        # Implement generation future
        return None
    
    return file

def get_new_york_pkl(square_side: int, target_stat) -> Optional[Path]:
    file = CACHED_GEODATA_DIR / f"new_york_{target_stat}_{square_side}.pkl"
    
    if not file.exists():
        # Implement generation future
        return None
    
    return file

def main():
    pio.renderers.default = "browser"

    square_side = 200
    crexi_filtered_path = CACHED_GEODATA_DIR / "crexi_filtered.parquet"
    ny_grid_path = CACHED_GEODATA_DIR / Path(f"ny_grid{200}.parquet")

    target_stat = "asking_price"

    fig, _, _ = build_gwr_figure(
        points      = crexi_filtered_path,
        target_stat = target_stat,
        grid_path   = ny_grid_path,
        lon_col     = "longitude",
        lat_col     = "latitude",
        bandwidth   = None,
        zoom        = 6.5,
    )
    fig.show()


    filename = Path(f"new_york_{target_stat}_{square_side}")
    save_as_pickle(fig, file_path=filename.with_suffix('.pkl'))



if __name__ == "__main__":
    main()
