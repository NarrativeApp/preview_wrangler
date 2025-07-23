"""
Rotation detector using pre-trained check-orientation model.

This module uses the check-orientation package which provides a pre-trained
model for detecting image rotations of 0, 90, 180, or 270 degrees.
"""

import logging
from pathlib import Path
from typing import List, Optional, Tuple
import numpy as np
from PIL import Image
import torch
import cv2
from tqdm import tqdm
from check_orientation.pre_trained_models import create_model
import albumentations as A
from albumentations.pytorch import ToTensorV2

logger = logging.getLogger(__name__)


class SimpleCheckOrientationDetector:
    """
    Simplified interface to check-orientation model.
    
    This uses the model directly without additional preprocessing.
    """
    
    def __init__(self):
        """Initialize the detector."""
        logger.info("Loading pre-trained model weights...")
        # Create model with pre-trained weights
        self.model = create_model("swsl_resnext50_32x4d")
        self.model.eval()
        
        # Move to appropriate device
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.model = self.model.to(self.device)
        
        # Standard ImageNet preprocessing
        self.transform = A.Compose([
            A.Resize(224, 224),
            A.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225]),
            ToTensorV2()
        ])
    
    def predict_rotation(self, image_path: Path) -> int:
        """Predict rotation angle for a single image."""
        try:
            # Load and preprocess image
            image = cv2.imread(str(image_path))
            image = cv2.cvtColor(image, cv2.COLOR_BGR2RGB)
            
            # Apply transforms
            transformed = self.transform(image=image)
            image_tensor = transformed["image"].unsqueeze(0).to(self.device)
            
            # Predict
            with torch.no_grad():
                output = self.model(image_tensor)
                _, predicted = torch.max(output, 1)
                
            # Map prediction to angle
            angle_map = {0: 0, 1: 90, 2: 180, 3: 270}
            return angle_map[predicted.item()]
            
        except Exception as e:
            logger.error(f"Error predicting rotation for {image_path}: {e}")
            return 0
    
    def correct_rotation(self, image_path: Path, output_path: Path,
                        predicted_angle: Optional[int] = None) -> int:
        """Correct image rotation."""
        if predicted_angle is None:
            predicted_angle = self.predict_rotation(image_path)
        
        if predicted_angle == 0:
            if image_path != output_path:
                import shutil
                output_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(image_path, output_path)
            return 0
        
        # Load and rotate image
        image = cv2.imread(str(image_path))
        
        if predicted_angle == 90:
            rotated = cv2.rotate(image, cv2.ROTATE_90_COUNTERCLOCKWISE)
        elif predicted_angle == 180:
            rotated = cv2.rotate(image, cv2.ROTATE_180)
        elif predicted_angle == 270:
            rotated = cv2.rotate(image, cv2.ROTATE_90_CLOCKWISE)
        
        output_path.parent.mkdir(parents=True, exist_ok=True)
        cv2.imwrite(str(output_path), rotated)
        
        logger.info(f"Corrected {image_path.name}: rotated {predicted_angle}°")
        return predicted_angle


class PretrainedRotationDetector(SimpleCheckOrientationDetector):
    """Alias for SimpleCheckOrientationDetector for backward compatibility."""
    pass