import xarray as xr
import numpy as np
from pathlib import Path
from scipy.signal import savgol_filter
from scipy.interpolate import RegularGridInterpolator
import matplotlib.pyplot as plt
import json
import os

class SSTProcessor:
    """Handles all SST data processing and image generation."""
    
    def __init__(self):
        with open('color_scale.json', 'r') as f:
            color_scale = json.load(f)
        self.colors = color_scale['colors']
        self.cmap = plt.cm.colors.LinearSegmentedColormap.from_list('custom_cmap', self.colors)
        self.cmap.set_bad(alpha=0)

    def load_sst_data(self, nc4_filepath: Path):
        """Load and convert SST data from NC4 file."""
        with xr.open_dataset(nc4_filepath) as ds:
            sst = ds.sst.squeeze().values
            lat = ds.lat.values
            lon = ds.lon.values
        
        sst_fahrenheit = (sst * 9/5) + 32
        
        print("Fahrenheit SST data stats:")
        print(f"Shape: {sst_fahrenheit.shape}")
        print(f"Min: {np.nanmin(sst_fahrenheit):.2f}, Max: {np.nanmax(sst_fahrenheit):.2f}")
        print(f"NaN count: {np.isnan(sst_fahrenheit).sum()}")
        
        return sst_fahrenheit, lat, lon

    def smooth_sst(self, sst: np.ndarray, window_length: int = 11, polyorder: int = 2) -> np.ndarray:
        """Apply Savitzky-Golay filter for smoothing."""
        smoothed = savgol_filter(sst, window_length=window_length, polyorder=polyorder, axis=0, mode='nearest')
        smoothed = savgol_filter(smoothed, window_length=window_length, polyorder=polyorder, axis=1, mode='nearest')
        return smoothed

    def interpolate_sst(self, sst: np.ndarray, scale_factor: int) -> np.ndarray:
        """Interpolate SST data to higher resolution."""
        original_grid = (np.arange(sst.shape[0]), np.arange(sst.shape[1]))
        interpolator = RegularGridInterpolator(original_grid, sst, bounds_error=False, fill_value=np.nan)
        
        new_y = np.linspace(0, sst.shape[0] - 1, sst.shape[0] * scale_factor)
        new_x = np.linspace(0, sst.shape[1] - 1, sst.shape[1] * scale_factor)
        new_grid = np.meshgrid(new_y, new_x, indexing='ij')
        
        return np.round(interpolator((new_grid[0], new_grid[1])), 8)

    def increase_resolution(self, sst: np.ndarray, lat: np.ndarray, lon: np.ndarray, scale_factor: int) -> np.ndarray:
        """Increase resolution with smoothing and interpolation."""
        smoothed_sst = self.smooth_sst(sst)
        return self.interpolate_sst(smoothed_sst, scale_factor)

    def save_sst_image(self, sst: np.ndarray, output_path: Path, zoom_level: int, vmin: float, vmax: float):
        """Save SST data as image."""
        fig, ax = plt.subplots(figsize=(10, 12))
        
        ax.imshow(sst, cmap=self.cmap, vmin=vmin, vmax=vmax,
                 extent=[0, sst.shape[1], 0, sst.shape[0]])
        
        ax.axis('off')
        plt.subplots_adjust(top=1, bottom=0, right=1, left=0, hspace=0, wspace=0)
        plt.margins(0,0)
        
        plt.savefig(output_path, dpi=300, bbox_inches='tight', pad_inches=0, transparent=True)
        plt.close(fig)
        print(f"Saved SST image for zoom level {zoom_level} to {output_path}")

    def process_zoom_levels(self, sst: np.ndarray, lat: np.ndarray, lon: np.ndarray, output_dir: Path):
        """Process and save all zoom levels."""
        valid_data = sst[~np.isnan(sst)]
        vmin, vmax = np.percentile(valid_data, [2, 98])
        print(f"Global color scale range: {vmin:.2f}°F to {vmax:.2f}°F")

        zoom_levels = [5, 8, 10]
        for zoom in zoom_levels:
            print(f"\nProcessing zoom level {zoom}")
            if zoom == 5:
                output_sst = sst
            elif zoom == 8:
                output_sst = self.increase_resolution(sst, lat, lon, scale_factor=20)
            elif zoom == 10:
                output_sst = self.increase_resolution(sst, lat, lon, scale_factor=30)
            
            output_path = output_dir / f'sst_zoom_{zoom}.png'
            self.save_sst_image(output_sst, output_path, zoom, vmin, vmax)
