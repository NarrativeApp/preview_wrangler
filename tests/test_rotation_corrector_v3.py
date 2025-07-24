"""Tests for V3 rotation corrector."""

import gzip
import json
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

import pytest
from PIL import Image

from preview_wrangler.rotation_corrector_v3 import V3RotationCorrector


class TestV3RotationCorrector:
    """Test cases for V3RotationCorrector."""

    def test_rotation_to_degrees(self):
        """Test rotation value to degrees conversion."""
        corrector = V3RotationCorrector(Path("/tmp"))
        
        assert corrector._rotation_to_degrees(None) == 0
        assert corrector._rotation_to_degrees("None") == 0
        assert corrector._rotation_to_degrees("CW90") == 270
        assert corrector._rotation_to_degrees("CW180") == 180
        assert corrector._rotation_to_degrees("CW270") == 90
        assert corrector._rotation_to_degrees("unknown") == 0

    def test_parse_v3_file(self):
        """Test parsing v3.gz file for rotation data."""
        with tempfile.TemporaryDirectory() as temp_dir:
            corrector = V3RotationCorrector(Path(temp_dir))
            
            # Create test v3.gz file
            test_data = {
                "images": [
                    {
                        "id": "test-image-1",
                        "meta": {"rotation": "CW90"}
                    },
                    {
                        "id": "test-image-2", 
                        "meta": {"rotation": "None"}
                    }
                ]
            }
            
            v3_file = Path(temp_dir) / "test.v3.gz"
            with gzip.open(v3_file, 'wt') as f:
                json.dump(test_data, f)
            
            rotation_data = corrector._parse_v3_file(v3_file)
            
            assert rotation_data == {
                "test-image-1": "CW90",
                "test-image-2": "None"
            }

    def test_get_image_path(self):
        """Test getting image path from image ID."""
        with tempfile.TemporaryDirectory() as temp_dir:
            corrector = V3RotationCorrector(Path(temp_dir))
            project_dir = Path(temp_dir)
            
            # Create test JPEG file
            test_image = project_dir / "test-image-1.jpg"
            test_image.touch()
            
            # Should find the image
            found_path = corrector._get_image_path(project_dir, "test-image-1")
            assert found_path == test_image
            
            # Should return None for non-existent image
            not_found = corrector._get_image_path(project_dir, "non-existent")
            assert not_found is None

    def test_get_cache_key(self):
        """Test cache key generation."""
        with tempfile.TemporaryDirectory() as temp_dir:
            corrector = V3RotationCorrector(Path(temp_dir))
            
            test_file = Path(temp_dir) / "test.jpg"
            test_file.write_text("test content")
            
            cache_key = corrector._get_cache_key(test_file)
            
            assert cache_key.startswith("v3_rotation_")
            assert "test.jpg" in cache_key

    @patch('preview_wrangler.rotation_corrector_v3.Image')
    def test_apply_rotation_no_rotation(self, mock_image):
        """Test applying rotation when no rotation is needed."""
        with tempfile.TemporaryDirectory() as temp_dir:
            corrector = V3RotationCorrector(Path(temp_dir))
            
            input_path = Path(temp_dir) / "input.jpg"
            output_path = Path(temp_dir) / "output.jpg"
            
            # Create a dummy input file
            input_path.write_text("dummy image content")
            
            # Test no rotation needed
            result = corrector._apply_rotation(input_path, output_path, 0)
            assert result is False
            mock_image.assert_not_called()

    def test_get_output_path(self):
        """Test output path generation."""
        with tempfile.TemporaryDirectory() as temp_dir:
            input_dir = Path(temp_dir) / "input"
            output_dir = Path(temp_dir) / "output"
            
            # Test with output directory
            corrector = V3RotationCorrector(input_dir, output_dir)
            
            input_path = input_dir / "project" / "image.jpg"
            output_path = corrector._get_output_path(input_path)
            
            expected = output_dir / "project" / "image.jpg"
            assert output_path == expected
            
            # Test in-place (no output directory)
            corrector_inplace = V3RotationCorrector(input_dir)
            output_path_inplace = corrector_inplace._get_output_path(input_path)
            assert output_path_inplace == input_path