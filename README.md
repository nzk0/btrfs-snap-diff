# btrfs-snap-diff

A fast BTRFS snapshot comparison tool that shows file and package changes between snapshots.

## Features

- Color-coded output for better readability
- Tree view visualization of file changes
- Package change detection (Arch-based systems)
- Parallel scanning for performance
- Smart time-based snapshot resolution
- Works with both Snapper and Timeshift

## Installation

```bash
# Clone the repository
git clone https://github.com/nzk0/btrfs-snap-diff.git
cd btrfs-snap-diff

# Build with Zig
zig build-exe btrfs-snap-diff.zig

# Optional: Install to system
sudo cp btrfs-snap-diff /usr/local/bin/
```

## Usage

```bash
# Compare two snapshots
btrfs-snap-diff today yesterday
btrfs-snap-diff 2d today

# Compare latest two snapshots
btrfs-snap-diff latest

# List available snapshots
btrfs-snap-diff list
btrfs-snap-diff list --all

# Use specific snapshot numbers
btrfs-snap-diff #265 #270

# With time specifications
btrfs-snap-diff yesterday:14:00 latest
```

## Time References

- **Relative**: `today`, `yesterday`, `latest`, `previous`, `oldest`
- **Days ago**: `2d`, `3d`, `1w` (days/weeks before today)
- **With time**: `today:14:30`, `yesterday:09:15`
- **Snapshot numbers**: `#265` or just `265`

## Requirements

- Zig compiler (0.11.0 or later)
- BTRFS filesystem with snapshots
- Root privileges (auto-elevates with sudo)
- Snapper or Timeshift (optional, for metadata)

## Options

- `--no-color` - Disable colored output

## What It Scans

- `/etc` - Configuration files
- `/usr/bin` - Binary executables
- `/usr/lib` - Libraries
- `/var/lib/pacman/local` - Package database (Arch Linux)