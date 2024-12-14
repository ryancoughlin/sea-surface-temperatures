#!/usr/bin/env python3
import os
import shutil
from pathlib import Path

def create_directories():
    """Create new directory structure"""
    dirs = [
        'processors/visualization',
        'processors/data',
        'processors/orchestration'
    ]
    for dir_path in dirs:
        Path(dir_path).mkdir(parents=True, exist_ok=True)
        init_file = Path(dir_path) / '__init__.py'
        if not init_file.exists():
            init_file.touch()

def move_and_rename_files():
    """Move and rename files to new structure"""
    moves = [
        # Visualization files
        ('processors/base_processor.py', 'processors/visualization/base_visualizer.py'),
        ('processors/sst_processor.py', 'processors/visualization/sst_visualizer.py'),
        ('processors/chlorophyll.py', 'processors/visualization/chlorophyll_visualizer.py'),
        ('processors/waves_processor.py', 'processors/visualization/waves_visualizer.py'),
        ('processors/currents_processor.py', 'processors/visualization/currents_visualizer.py'),
        ('processors/processor_factory.py', 'processors/visualization/visualizer_factory.py'),
        
        # Data files
        ('processors/data_preprocessor.py', 'processors/data/data_preprocessor.py'),
        ('processors/metadata_assembler.py', 'processors/data/data_assembler.py'),
        
        # Orchestration files
        ('processors/processing_manager.py', 'processors/orchestration/processing_orchestrator.py')
    ]
    
    for src, dst in moves:
        if Path(src).exists():
            shutil.move(src, dst)
            print(f"Moved {src} to {dst}")

def update_imports():
    """Update import statements in all files"""
    replacements = {
        'from .base_processor': 'from .base_visualizer',
        'class BaseVisualizer': 'class BaseVisualizer',
        'class SSTProcessor': 'class SSTVisualizer',
        'class ChlorophyllProcessor': 'class ChlorophyllVisualizer',
        'class WavesProcessor': 'class WavesVisualizer',
        'class CurrentsProcessor': 'class CurrentsVisualizer',
        'class ProcessorFactory': 'class VisualizerFactory',
        'class ProcessingManager': 'class ProcessingOrchestrator',
        'class DataAssembler': 'class DataAssembler',
        'from processors.processor_factory': 'from processors.visualization.visualizer_factory',
        'from processors.data_preprocessor': 'from processors.data.data_preprocessor',
        'from processors.metadata_assembler': 'from processors.data.data_assembler',
        'BaseVisualizer': 'BaseVisualizer',
        'SSTProcessor': 'SSTVisualizer',
        'ChlorophyllProcessor': 'ChlorophyllVisualizer',
        'WavesProcessor': 'WavesVisualizer',
        'CurrentsProcessor': 'CurrentsVisualizer',
        'ProcessorFactory': 'VisualizerFactory',
        'ProcessingManager': 'ProcessingOrchestrator',
        'DataAssembler': 'DataAssembler'
    }
    
    dirs_to_process = [
        'processors/visualization',
        'processors/data',
        'processors/orchestration'
    ]
    
    for dir_path in dirs_to_process:
        for file_path in Path(dir_path).glob('*.py'):
            with open(file_path, 'r') as f:
                content = f.read()
            
            for old, new in replacements.items():
                content = content.replace(old, new)
            
            with open(file_path, 'w') as f:
                f.write(content)
            print(f"Updated imports in {file_path}")

def main():
    """Main execution function"""
    print("Starting reorganization...")
    
    # Create new directory structure
    print("\nCreating directories...")
    create_directories()
    
    # Move and rename files
    print("\nMoving and renaming files...")
    move_and_rename_files()
    
    # Update imports
    print("\nUpdating imports...")
    update_imports()
    
    print("\nReorganization complete!")

if __name__ == "__main__":
    main() 