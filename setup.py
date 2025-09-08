#!/usr/bin/env python3
"""
Setup script for the migration environment
"""

import os
import subprocess
import sys
from pathlib import Path


def run_command(command, description):
    """Run a shell command with error handling"""
    print(f"\n{description}...")
    try:
        result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
        print(f"✓ {description} completed successfully")
        if result.stdout:
            print(result.stdout)
        return True
    except subprocess.CalledProcessError as e:
        print(f"✗ {description} failed: {e}")
        if e.stdout:
            print("STDOUT:", e.stdout)
        if e.stderr:
            print("STDERR:", e.stderr)
        return False


def check_python_version():
    """Check if Python version is compatible"""
    version = sys.version_info
    if version.major < 3 or (version.major == 3 and version.minor < 8):
        print("✗ Python 3.8+ is required")
        return False
    print(f"✓ Python {version.major}.{version.minor}.{version.micro} is compatible")
    return True


def setup_virtual_environment():
    """Setup virtual environment for the migration"""
    venv_path = Path("./venv")
    
    if venv_path.exists():
        print("✓ Virtual environment already exists")
        return True
    
    if not run_command("python3 -m venv venv", "Creating virtual environment"):
        return False
    
    return True


def install_dependencies():
    """Install required Python packages"""
    activate_script = "./venv/bin/activate" if os.name != 'nt' else "./venv/Scripts/activate"
    
    commands = [
        f"source {activate_script} && pip install --upgrade pip",
        f"source {activate_script} && pip install -r requirements.txt"
    ]
    
    for cmd in commands:
        if not run_command(cmd, f"Running: {cmd.split('&&')[-1].strip()}"):
            return False
    
    return True


def setup_environment_file():
    """Setup the .env file from template"""
    env_file = Path(".env")
    env_example = Path(".env.example")
    
    if env_file.exists():
        print("✓ .env file already exists")
        return True
    
    if env_example.exists():
        run_command("cp .env.example .env", "Creating .env file from template")
        print("\n⚠️  Please edit the .env file with your database credentials:")
        print("   - Set HEIMDALL_DB_* variables for your Heimdall database")
        print("   - Set ODIN_DB_* variables for your Odin database")
        print("   - Adjust migration settings as needed")
        return True
    else:
        print("✗ .env.example template not found")
        return False


def verify_setup():
    """Verify the setup is working"""
    activate_script = "./venv/bin/activate" if os.name != 'nt' else "./venv/Scripts/activate"
    
    test_command = f"source {activate_script} && python -c 'import asyncio, sqlalchemy, pandas, psycopg2; print(\"All dependencies imported successfully\")'"
    
    return run_command(test_command, "Verifying Python dependencies")


def main():
    """Main setup function"""
    print("="*60)
    print("HEIMDALL TO ODIN MIGRATION SETUP")
    print("="*60)
    
    # Change to migrator directory
    migrator_dir = Path(__file__).parent
    os.chdir(migrator_dir)
    
    steps = [
        ("Checking Python version", check_python_version),
        ("Setting up virtual environment", setup_virtual_environment),
        ("Installing dependencies", install_dependencies),
        ("Setting up environment file", setup_environment_file),
        ("Verifying setup", verify_setup),
    ]
    
    failed_steps = []
    
    for step_name, step_func in steps:
        print(f"\n{'='*40}")
        print(f"Step: {step_name}")
        print('='*40)
        
        if not step_func():
            failed_steps.append(step_name)
    
    print(f"\n{'='*60}")
    print("SETUP SUMMARY")
    print("="*60)
    
    if not failed_steps:
        print("✓ All setup steps completed successfully!")
        print("\nNext steps:")
        print("1. Edit the .env file with your database credentials")
        print("2. Ensure both Heimdall and Odin databases are running")
        print("3. Run schema analysis: python -m migrator.analyze_schemas")
        print("4. Run dry migration: python -m migrator.migrate --dry-run")
        print("5. Run actual migration: python -m migrator.migrate")
    else:
        print("✗ Some setup steps failed:")
        for step in failed_steps:
            print(f"  - {step}")
        print("\nPlease resolve the issues above and run setup again.")
    
    print("="*60)


if __name__ == "__main__":
    main()
