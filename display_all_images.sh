#!/bin/bash

# Display all JPEG files in preview_projects directory using imgcat

# Option 1: Display all images (simple)
# find preview_projects -name "*.jpg" -type f | xargs imgcat

# Option 2: Display images grouped by project with labels
for project_dir in output/*/; do
    if [ -d "$project_dir" ]; then
        project_name=$(basename "$project_dir")
        echo "=== Project: $project_name ==="

        # Display all JPEGs in this project
        for img in "$project_dir"*.jpg; do
            if [ -f "$img" ]; then
                echo "$(basename "$img")"
                /Users/pauloliver/.iterm2/imgcat "$img"
            fi
        done
        echo ""
    fi
done

# Option 3: Display a grid of images (if you want to see many at once)
# find preview_projects -name "*.jpg" -type f | head -100 | xargs imgcat --width 200
