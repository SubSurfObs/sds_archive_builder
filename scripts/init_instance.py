"""
Scaffold a new instance directory from the config templates.

Usage:
    python scripts/init_instance.py ~/instances/victoria
    sds-init ~/instances/victoria
"""

import shutil
import sys
from pathlib import Path

import click


PACKAGE_ROOT = Path(__file__).resolve().parent.parent
TEMPLATES_DIR = PACKAGE_ROOT / "config" / "templates"


@click.command()
@click.argument("instance_dir", type=click.Path())
@click.option("--force", is_flag=True, help="Overwrite existing files.")
def main(instance_dir: str, force: bool):
    """Scaffold a new archive instance directory from templates."""
    dest = Path(instance_dir).resolve()

    if dest.exists() and any(dest.iterdir()) and not force:
        click.echo(f"Directory {dest} already exists and is non-empty.")
        click.echo("Use --force to overwrite.")
        sys.exit(1)

    dest.mkdir(parents=True, exist_ok=True)
    (dest / "networks").mkdir(exist_ok=True)
    (dest / "logs").mkdir(exist_ok=True)

    # Copy archive.yaml template
    src_archive = TEMPLATES_DIR / "archive.yaml"
    dst_archive = dest / "archive.yaml"
    if not dst_archive.exists() or force:
        shutil.copy2(src_archive, dst_archive)
        click.echo(f"  Created {dst_archive}")

    # Copy network YAML templates
    networks_src = TEMPLATES_DIR / "networks"
    for yaml_file in networks_src.glob("*.yaml"):
        dst = dest / "networks" / yaml_file.name
        if not dst.exists() or force:
            shutil.copy2(yaml_file, dst)
            click.echo(f"  Created {dst}")

    # Copy generic network template
    net_template_src = TEMPLATES_DIR / "network.yaml"
    net_template_dst = dest / "networks" / "_template.yaml"
    if not net_template_dst.exists() or force:
        shutil.copy2(net_template_src, net_template_dst)
        click.echo(f"  Created {net_template_dst}")

    click.echo(f"\nInstance scaffolded at: {dest}")
    click.echo("\nNext steps:")
    click.echo(f"  1. Edit {dest}/archive.yaml  — set sds_root, local_staging, geo_bounds")
    click.echo(f"  2. Edit network files in {dest}/networks/")
    click.echo(f"  3. Run: sds-inventory --instance {dest}")


if __name__ == "__main__":
    main()
