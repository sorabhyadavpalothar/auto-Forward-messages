# main.py
#!/usr/bin/env python3
"""
Main Entry Point for Multi-User Telegram Auto Forwarder v3.0
Handles dependency checks, version validation, and application startup
"""

import asyncio
import sys
import os
from multi_user import MultiUserTelegramBot


def check_python_version():
    """Check if Python version is compatible"""
    if sys.version_info < (3, 8):
        print("Error: Python 3.8 or higher is required.")
        print(f"Current version: {sys.version}")
        sys.exit(1)


def check_dependencies():
    """Check if required packages are installed"""
    required_packages = ['telethon', 'colorama', 'watchdog']
    missing_packages = []

    for package in required_packages:
        try:
            __import__(package)
        except ImportError:
            missing_packages.append(package)

    if missing_packages:
        print("Error: Missing required packages:")
        for package in missing_packages:
            print(f"  - {package}")
        print("\nInstall missing packages with:")
        print(f"pip install {' '.join(missing_packages)}")
        sys.exit(1)


def validate_environment():
    """Validate environment and create necessary directories"""
    # Create required directories if they don't exist
    required_dirs = ['database', 'sessions', 'logs']
    
    for directory in required_dirs:
        try:
            os.makedirs(directory, exist_ok=True)
        except PermissionError:
            print(f"Error: Permission denied creating directory '{directory}'")
            sys.exit(1)
        except Exception as e:
            print(f"Error creating directory '{directory}': {e}")
            sys.exit(1)


def display_startup_banner():
    """Display application startup banner"""
    banner = """
╔═══════════════════════════════════════════════════════════════╗
║           Multi-User Telegram Auto Forwarder v3.0            ║
║                                                               ║
║  Features:                                                    ║
║  • Multiple user support with individual configurations      ║
║  • Real-time configuration updates                           ║
║  • Concurrent forwarding for multiple accounts              ║
║  • Per-user statistics and logging                          ║
║  • Auto-start functionality                                 ║
║  • Enhanced error handling per user session                 ║
╚═══════════════════════════════════════════════════════════════╝
    """
    print(banner)


async def main():
    """Main entry point for multi-user bot"""
    # Pre-flight checks
    check_python_version()
    check_dependencies()
    validate_environment()
    
    # Display startup information
    display_startup_banner()
    
    # Create and run multi-user bot instance
    try:
        bot = MultiUserTelegramBot()
        await bot.run()
    except KeyboardInterrupt:
        print("\nShutdown requested by user (Ctrl+C)")
    except Exception as e:
        print(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    """Entry point with proper async handling"""
    try:
        # Handle different event loop policies for Windows
        if sys.platform.startswith('win'):
            # Use WindowsProactorEventLoopPolicy for better Windows compatibility
            if hasattr(asyncio, 'WindowsProactorEventLoopPolicy'):
                asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
        
        # Run the main async function
        asyncio.run(main())
        
    except KeyboardInterrupt:
        print("\nApplication interrupted by user")
    except Exception as e:
        print(f"Application failed to start: {e}")
        sys.exit(1)