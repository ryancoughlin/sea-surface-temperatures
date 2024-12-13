#!/usr/bin/env python3

import os
import json
import shutil
import logging
import argparse
import subprocess
from pathlib import Path
from typing import Dict, List

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class TileDeployer:
    """Handles deployment of vector tiles to remote server."""
    
    def __init__(self, vector_tiles_dir: str = "layers"):
        """Initialize the tile deployer.
        
        Args:
            vector_tiles_dir: Directory containing vector tiles (default: layers)
        """
        self.vector_tiles_dir = Path(vector_tiles_dir)
        self.droplet_name = "dockeronubuntu2204-s-1vcpu-1gb-nyc3-01"
        self.ssh_key_path = "~/.ssh/digitalocean"
        self.remote_project = "/sea-surface-temperatures"
        self.remote_tiles_dir = f"{self.remote_project}/layers"

    def _get_rsync_command(self, source: str, dest: str) -> list[str]:
        """Get rsync command with doctl SSH configuration."""
        return [
            "rsync",
            "-avz",
            "-e",
            f"doctl compute ssh {self.droplet_name} --ssh-key-path {self.ssh_key_path} --ssh-command",
            source,
            f"root@localhost:{dest}"
        ]

    def deploy_layer(self, layer_name: str) -> None:
        """Deploy a single layer to the remote server."""
        layer_dir = self.vector_tiles_dir / layer_name
        if not layer_dir.exists():
            raise ValueError(f"Layer directory not found: {layer_dir}")

        # Create remote directory
        remote_dir = f"{self.remote_tiles_dir}/{layer_name}"
        subprocess.run([
            "doctl", "compute", "ssh",
            self.droplet_name,
            "--ssh-key-path", self.ssh_key_path,
            "--ssh-command", f"mkdir -p {remote_dir}/tiles"
        ], check=True)

        # Deploy tiles
        tiles_dir = layer_dir / "tiles/static"
        if tiles_dir.exists():
            cmd = self._get_rsync_command(
                f"{tiles_dir}/",
                f"{remote_dir}/tiles/static/"
            )
            subprocess.run(cmd, check=True)
            logger.info(f"Deployed tiles for layer: {layer_name}")

    def deploy(self, layer_name: str = None) -> None:
        """Deploy vector tiles to remote server."""
        try:
            # Create base directory
            subprocess.run([
                "doctl", "compute", "ssh",
                self.droplet_name,
                "--ssh-key-path", self.ssh_key_path,
                "--ssh-command", f"mkdir -p {self.remote_tiles_dir}"
            ], check=True)

            if layer_name:
                # Deploy specific layer
                self.deploy_layer(layer_name)
            else:
                # Deploy all layers
                for layer_dir in self.vector_tiles_dir.iterdir():
                    if layer_dir.is_dir() and layer_dir.name != "__pycache__":
                        self.deploy_layer(layer_dir.name)
            
            logger.info("Deployment completed successfully")
        except Exception as e:
            logger.error(f"Deployment failed: {e}")
            raise

def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Deploy vector tiles to remote server"
    )
    parser.add_argument(
        "--tiles-dir",
        default="layers",
        help="Directory containing vector tiles (default: layers)"
    )
    parser.add_argument(
        "--layer",
        help="Specific layer to deploy (deploys all layers if not specified)"
    )
    return parser.parse_args()

def main() -> None:
    """Main entry point."""
    try:
        args = parse_args()
        deployer = TileDeployer(args.tiles_dir)
        deployer.deploy(args.layer)
    except Exception as e:
        logger.error(f"Error: {e}")
        raise SystemExit(1)

if __name__ == "__main__":
    main()
