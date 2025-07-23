"""
Rotation detector using PyTorch and pre-trained models.

This module detects image rotation angles (0, 90, 180, 270 degrees)
and provides functionality to correct rotated images.
"""

import logging
from pathlib import Path
from typing import Tuple, Optional, List
import numpy as np
from PIL import Image
import torch
import torch.nn as nn
import torchvision.models as models
import torchvision.transforms as transforms
import cv2
from tqdm import tqdm

logger = logging.getLogger(__name__)


class RotationDetector:
    """Detects and corrects image rotation using a CNN model."""
    
    def __init__(self, model_path: Optional[Path] = None):
        """
        Initialize the rotation detector.
        
        Args:
            model_path: Path to pre-trained model weights (optional)
        """
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        logger.info(f"Using device: {self.device}")
        
        # Image preprocessing
        self.transform = transforms.Compose([
            transforms.Resize((224, 224)),
            transforms.ToTensor(),
            transforms.Normalize(mean=[0.485, 0.456, 0.406], 
                               std=[0.229, 0.224, 0.225])
        ])
        
        # Initialize model
        self.model = self._create_model()
        self.model.to(self.device)
        self.model.eval()
        
        # Load pre-trained weights if provided
        if model_path and model_path.exists():
            self._load_weights(model_path)
    
    def _create_model(self) -> nn.Module:
        """
        Create a ResNet-based model for rotation detection.
        
        Returns:
            PyTorch model for 4-class rotation classification
        """
        # Use ResNet50 as backbone
        model = models.resnet50(weights=models.ResNet50_Weights.IMAGENET1K_V1)
        
        # Replace final layer for 4-class classification (0, 90, 180, 270 degrees)
        num_features = model.fc.in_features
        model.fc = nn.Linear(num_features, 4)
        
        return model
    
    def _load_weights(self, model_path: Path):
        """Load pre-trained weights."""
        try:
            state_dict = torch.load(model_path, map_location=self.device)
            self.model.load_state_dict(state_dict)
            logger.info(f"Loaded model weights from {model_path}")
        except Exception as e:
            logger.error(f"Failed to load model weights: {e}")
            raise
    
    def predict_rotation(self, image_path: Path) -> int:
        """
        Predict the rotation angle of an image.
        
        Args:
            image_path: Path to the image file
            
        Returns:
            Predicted rotation angle (0, 90, 180, or 270)
        """
        try:
            # Load and preprocess image
            image = Image.open(image_path).convert('RGB')
            input_tensor = self.transform(image).unsqueeze(0).to(self.device)
            
            # Predict
            with torch.no_grad():
                outputs = self.model(input_tensor)
                _, predicted = torch.max(outputs, 1)
                
            # Convert class to angle
            angles = [0, 90, 180, 270]
            return angles[predicted.item()]
            
        except Exception as e:
            logger.error(f"Error predicting rotation for {image_path}: {e}")
            raise
    
    def correct_rotation(self, image_path: Path, output_path: Path, 
                        predicted_angle: Optional[int] = None) -> int:
        """
        Correct the rotation of an image.
        
        Args:
            image_path: Path to the input image
            output_path: Path to save the corrected image
            predicted_angle: Pre-computed rotation angle (optional)
            
        Returns:
            Applied rotation angle
        """
        try:
            # Predict rotation if not provided
            if predicted_angle is None:
                predicted_angle = self.predict_rotation(image_path)
            
            # Skip if no rotation needed
            if predicted_angle == 0:
                logger.debug(f"Image {image_path.name} is already correctly oriented")
                # Copy image to output path if different
                if image_path != output_path:
                    image = Image.open(image_path)
                    image.save(output_path)
                return 0
            
            # Load image with OpenCV for rotation
            image = cv2.imread(str(image_path))
            if image is None:
                raise ValueError(f"Failed to load image: {image_path}")
            
            # Get image dimensions
            height, width = image.shape[:2]
            
            # Calculate rotation matrix
            if predicted_angle == 90:
                # Rotate 90 degrees counter-clockwise to correct
                rotated = cv2.rotate(image, cv2.ROTATE_90_COUNTERCLOCKWISE)
            elif predicted_angle == 180:
                # Rotate 180 degrees
                rotated = cv2.rotate(image, cv2.ROTATE_180)
            elif predicted_angle == 270:
                # Rotate 90 degrees clockwise to correct
                rotated = cv2.rotate(image, cv2.ROTATE_90_CLOCKWISE)
            else:
                raise ValueError(f"Invalid rotation angle: {predicted_angle}")
            
            # Save corrected image
            output_path.parent.mkdir(parents=True, exist_ok=True)
            cv2.imwrite(str(output_path), rotated)
            
            logger.info(f"Corrected rotation for {image_path.name}: {predicted_angle}° -> 0°")
            return predicted_angle
            
        except Exception as e:
            logger.error(f"Error correcting rotation for {image_path}: {e}")
            raise
    
    def process_batch(self, image_paths: List[Path], batch_size: int = 32) -> List[int]:
        """
        Process multiple images in batches for efficiency.
        
        Args:
            image_paths: List of image paths
            batch_size: Number of images to process at once
            
        Returns:
            List of predicted rotation angles
        """
        predictions = []
        
        for i in tqdm(range(0, len(image_paths), batch_size), 
                     desc="Detecting rotations", unit="batch"):
            batch_paths = image_paths[i:i + batch_size]
            batch_tensors = []
            
            # Load and preprocess batch
            for path in batch_paths:
                try:
                    image = Image.open(path).convert('RGB')
                    tensor = self.transform(image).unsqueeze(0)
                    batch_tensors.append(tensor)
                except Exception as e:
                    logger.error(f"Error loading {path}: {e}")
                    predictions.append(0)  # Assume no rotation on error
                    continue
            
            if batch_tensors:
                # Stack tensors and predict
                batch_input = torch.cat(batch_tensors, dim=0).to(self.device)
                
                with torch.no_grad():
                    outputs = self.model(batch_input)
                    _, predicted = torch.max(outputs, 1)
                
                # Convert predictions to angles
                angles = [0, 90, 180, 270]
                for pred in predicted.cpu().numpy():
                    predictions.append(angles[pred])
        
        return predictions


class SimpleRotationDetector:
    """
    Simple rotation detector using image analysis heuristics.
    
    This is a fallback option that doesn't require pre-trained weights.
    It uses basic image features to detect likely rotation.
    """
    
    def __init__(self):
        """Initialize the simple rotation detector."""
        pass
    
    def predict_rotation(self, image_path: Path) -> int:
        """
        Predict rotation using simple heuristics.
        
        This is less accurate than a trained model but works without weights.
        
        Args:
            image_path: Path to the image file
            
        Returns:
            Predicted rotation angle (0, 90, 180, or 270)
        """
        try:
            # Load image
            image = cv2.imread(str(image_path))
            if image is None:
                raise ValueError(f"Failed to load image: {image_path}")
            
            height, width = image.shape[:2]
            
            # For portrait photos, if width > height, likely rotated 90 or 270
            # This is a very simple heuristic and won't be very accurate
            if width > height * 1.2:  # Significant aspect ratio difference
                # Could be 90 or 270, default to 90
                return 90
            
            # Default to no rotation
            # More sophisticated analysis would be needed for better accuracy
            return 0
            
        except Exception as e:
            logger.error(f"Error in simple rotation detection for {image_path}: {e}")
            return 0
    
    def correct_rotation(self, image_path: Path, output_path: Path,
                        predicted_angle: Optional[int] = None) -> int:
        """Correct rotation using the simple detector."""
        if predicted_angle is None:
            predicted_angle = self.predict_rotation(image_path)
        
        if predicted_angle == 0:
            # Copy image if paths are different
            if image_path != output_path:
                image = Image.open(image_path)
                image.save(output_path)
            return 0
        
        # Use OpenCV for rotation
        image = cv2.imread(str(image_path))
        
        if predicted_angle == 90:
            rotated = cv2.rotate(image, cv2.ROTATE_90_COUNTERCLOCKWISE)
        elif predicted_angle == 180:
            rotated = cv2.rotate(image, cv2.ROTATE_180)
        elif predicted_angle == 270:
            rotated = cv2.rotate(image, cv2.ROTATE_90_CLOCKWISE)
        else:
            raise ValueError(f"Invalid rotation angle: {predicted_angle}")
        
        output_path.parent.mkdir(parents=True, exist_ok=True)
        cv2.imwrite(str(output_path), rotated)
        
        return predicted_angle