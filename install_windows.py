#!/usr/bin/env python3
"""
Windows Installation Helper for NL2SQL v2
Installs required dependencies for Windows environment.
"""

import subprocess
import sys
import os

def check_package(package_name):
    """Check if a package is installed"""
    try:
        __import__(package_name.replace('-', '_'))
        return True
    except ImportError:
        return False

def install_package(package):
    """Install a Python package"""
    print(f"📦 Installing {package}...")
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", package])
        print(f"✅ Successfully installed {package}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to install {package}: {e}")
        return False

def main():
    """Main installation function"""
    print("🚀 NL2SQL v2 Windows Installation Helper")
    print("=" * 50)
    
    # Required packages for basic functionality
    packages = [
        "openai>=1.12.0",
        "python-dotenv>=1.0.0", 
        "pydantic>=2.5.0",
        "click>=8.1.0",
        "rich>=13.7.0",
        "pyyaml>=6.0.1",
        "snowflake-connector-python>=3.6.0"
    ]
    
    print(f"🔍 Checking Python environment: {sys.executable}")
    print(f"🔍 Current working directory: {os.getcwd()}")
    print()
    
    failed_packages = []
    
    for package in packages:
        package_name = package.split(">=")[0].split("==")[0]
        print(f"Checking {package_name}...", end=" ")
        
        if check_package(package_name):
            print("✅ Already installed")
        else:
            print("❌ Missing")
            if install_package(package):
                print(f"✅ {package_name} installed successfully")
            else:
                failed_packages.append(package)
    
    print("\n" + "=" * 50)
    
    if failed_packages:
        print("❌ Some packages failed to install:")
        for pkg in failed_packages:
            print(f"  • {pkg}")
        print("\n💡 Try running these commands manually:")
        print(f"pip install {' '.join(failed_packages)}")
        return 1
    else:
        print("✅ All packages installed successfully!")
        print("\n🎉 You can now run:")
        print("python -m src.nl2sql_v2.cli.main chat")
        return 0

if __name__ == "__main__":
    sys.exit(main())