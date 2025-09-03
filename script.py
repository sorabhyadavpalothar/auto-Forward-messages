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

# bot_manager.py
#!/usr/bin/env python3
"""
Bot Manager Module
Handles core bot functionality, client management, and configuration loading
"""

import asyncio
import time
import json
import signal
import os
from typing import Optional, Dict, Any, List
from pathlib import Path
from dataclasses import dataclass
from datetime import datetime

from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError, PhoneCodeInvalidError
try:
    from telethon.errors.rpcerrorlist import FloodWaitError
except Exception:
    from telethon.errors import FloodWaitError

from colorama import Fore

# Import custom modules (assumed present)
from logger_setup import LoggerSetup, LogConfig
from config_manager import ConfigManager
from url_parser import URLParser
from utils import Utils, ForwardingStats
from message_forwarder import MessageForwarder, ForwardingMode


@dataclass
class UserConfig:
    """Enhanced configuration for a single user"""
    api_id: str
    api_hash: str
    phone: str
    groups: List[str]
    active: bool = True
    last_updated: Optional[str] = None
    delay: int = 5  # in seconds
    forward_mode: ForwardingMode = ForwardingMode.PRESERVE_ORIGINAL
    mode_set: bool = False
    start: bool = True
    auto_start_forwarding: bool = True  # New field for auto-start
    expiry_date: Optional[str] = None
    is_expired: bool = False


@dataclass
class GlobalConfig:
    """Global configuration settings"""
    auto_start_forwarding: bool = True
    skip_confirmation: bool = False
    concurrent_users: bool = True
    default_delay: int = 5
    default_forward_mode: ForwardingMode = ForwardingMode.PRESERVE_ORIGINAL


class BotManager:
    """
    Core bot management class handling configuration, clients, and user management
    """
    
    def __init__(self):
        """Initialize bot manager components"""
        # Setup enhanced logging with user separation
        log_config = LogConfig(
            max_log_files=60,
            max_file_size=50 * 1024 * 1024,
            compress_old_logs=True,
            console_output=False
        )

        self.logger_setup = LoggerSetup(log_config)
        self.logger = self.logger_setup.setup_logging()

        # Initialize components
        self.config_manager = ConfigManager(self.logger)
        self.url_parser = URLParser(self.logger)
        
        # Multi-user state management
        self.user_configs: Dict[str, UserConfig] = {}
        self.user_clients: Dict[str, TelegramClient] = {}
        self.user_forwarders: Dict[str, MessageForwarder] = {}
        self.user_stats: Dict[str, ForwardingStats] = {}
        
        # Global configuration
        self.global_config = GlobalConfig()
        
        # Global settings
        self.delay = 5
        self.forwarding_mode = ForwardingMode.PRESERVE_ORIGINAL
        self.concurrent_users = True  # Run users concurrently or sequentially
        
        # Runtime flags
        self.is_running = False
        self.shutdown_requested = False
        self.loop_count = 0
        self.main_loop = None  # Store reference to main event loop

        # Headless support
        self.headless = os.environ.get("TELEGRAM_HEADLESS", "0").lower() in ("1", "true", "yes")
        
        self._setup_signal_handlers()

    def _setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown"""
        def signal_handler(signum, frame):
            self.shutdown_requested = True

        signal.signal(signal.SIGINT, signal_handler)
        if hasattr(signal, "SIGTERM"):
            signal.signal(signal.SIGTERM, signal_handler)

    def get_active_users(self) -> List[str]:
        """Get list of currently active user IDs based on current config state"""
        active_users = []
        for api_id, user_config in self.user_configs.items():
            if user_config.start and not user_config.is_expired:
                if api_id in self.user_clients:
                    active_users.append(api_id)
        return active_users

    def _load_global_config(self) -> bool:
        """Load global configuration settings"""
        try:
            config_path = Path('database/config.json')
            
            if config_path.exists():
                with config_path.open('r', encoding='utf-8') as f:
                    config_data = json.load(f)
                    
                # Load global settings
                self.global_config.auto_start_forwarding = config_data.get('auto_start_forwarding', True)
                self.global_config.skip_confirmation = config_data.get('skip_confirmation', False)
                self.global_config.concurrent_users = config_data.get('concurrent_users', True)
                self.global_config.default_delay = config_data.get('default_delay', 5)
                
                # Parse default forward mode
                default_mode_str = config_data.get('default_forward_mode', '1')
                self.global_config.default_forward_mode = self._parse_forward_mode(default_mode_str)
                
            else:
                # Create default config file
                self._create_sample_config_file(config_path)
                
            return True
        except Exception as e:
            return True

    def _create_sample_config_file(self, path: Path):
        """Create a sample config file for global settings"""
        sample_config = {
            "auto_start_forwarding": True,
            "skip_confirmation": False,
            "concurrent_users": True,
            "default_delay": 5,
            "default_forward_mode": "1"
        }
        
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open('w', encoding='utf-8') as f:
                json.dump(sample_config, f, indent=2)
        except Exception as e:
            pass

    async def load_user_configurations(self) -> bool:
        """Load all user configurations from JSON files"""
        try:
            # Load global configuration first
            self._load_global_config()
            
            # Load credentials
            credentials_path = Path('database/credentials.json')
            groups_path = Path('database/groups.json')
            
            if not credentials_path.exists() or not groups_path.exists():
                if not credentials_path.exists():
                    self._create_sample_credentials_file(credentials_path)
                if not groups_path.exists():
                    self._create_sample_groups_file(groups_path)
                return False

            # Load credentials with validation
            try:
                with credentials_path.open('r', encoding='utf-8') as f:
                    credentials_data = json.load(f)
            except json.JSONDecodeError as e:
                return False

            # Load groups with validation
            try:
                with groups_path.open('r', encoding='utf-8') as f:
                    content = f.read().strip()
                    # Fix common JSON issues like trailing commas
                    if content.endswith(',}'):
                        content = content[:-2] + '}'
                    elif content.endswith(',]'):
                        content = content[:-2] + ']'
                    groups_data = json.loads(content)
            except json.JSONDecodeError as e:
                return False

            # Process each user
            loaded_users = 0
            for api_id, cred_info in credentials_data.items():
                try:
                    # Validate credential structure
                    if not isinstance(cred_info, dict):
                        continue
                    
                    required_fields = ['api_id', 'api_hash', 'phone']
                    missing_fields = [field for field in required_fields if field not in cred_info]
                    if missing_fields:
                        continue

                    # Check if user is set to start (replaced interactive prompt)
                    user_start = cred_info.get('start', True)
                    if not user_start:
                        continue

                    # Check individual auto-start setting (overrides global)
                    auto_start_forwarding = cred_info.get('auto_start_forwarding', self.global_config.auto_start_forwarding)

                    # Check expiry date
                    expiry_date = cred_info.get('expiry_date')
                    if expiry_date and self._check_user_expiry(expiry_date):
                        continue

                    # Parse user-specific settings
                    user_delay_str = cred_info.get('delay', f'{self.global_config.default_delay}s')
                    user_delay_seconds = self._parse_user_delay(user_delay_str, self.global_config.default_delay)
                    
                    forward_mode_str = cred_info.get('forward_mode', '1')
                    user_forward_mode = self._parse_forward_mode(forward_mode_str)
                    
                    mode_set = cred_info.get('mode_set', False)

                    # Get user groups (active only)
                    user_groups = []
                    if api_id in groups_data:
                        group_list = groups_data[api_id]
                        
                        for group_info in group_list:
                            if isinstance(group_info, dict):
                                if group_info.get('active', True):
                                    user_groups.append(group_info['url'])
                            else:
                                # Handle simple string URLs
                                user_groups.append(group_info)

                    if not user_groups:
                        continue

                    # Create user config with individual settings
                    user_config = UserConfig(
                        api_id=cred_info['api_id'],
                        api_hash=cred_info['api_hash'],
                        phone=cred_info['phone'],
                        groups=user_groups,
                        last_updated=cred_info.get('last_updated'),
                        delay=user_delay_seconds,
                        forward_mode=user_forward_mode,
                        mode_set=mode_set,
                        start=user_start,
                        auto_start_forwarding=auto_start_forwarding,
                        expiry_date=expiry_date,
                        is_expired=False
                    )

                    self.user_configs[api_id] = user_config
                    self.user_stats[api_id] = ForwardingStats()
                    
                    # Create user-specific message forwarder
                    self.user_forwarders[api_id] = MessageForwarder(self.logger_setup, self.url_parser)
                    
                    loaded_users += 1

                except Exception as e:
                    import traceback
                    continue

            if loaded_users == 0:
                return False

            return True

        except Exception as e:
            import traceback
            return False

    def _parse_user_delay(self, delay_str: str, default_seconds: int = 5) -> int:
        """Parse delay string like '2m 45s' into total seconds"""
        if not delay_str or not isinstance(delay_str, str):
            return default_seconds
            
        try:
            total_seconds = 0
            delay_str = delay_str.lower().strip()
            
            # Handle formats like "2m 45s", "1h 30m", "45s", etc.
            import re
            
            # Extract hours
            hours_match = re.search(r'(\d+)\s*h', delay_str)
            if hours_match:
                total_seconds += int(hours_match.group(1)) * 3600
            
            # Extract minutes
            minutes_match = re.search(r'(\d+)\s*m', delay_str)
            if minutes_match:
                total_seconds += int(minutes_match.group(1)) * 60
            
            # Extract seconds
            seconds_match = re.search(r'(\d+)\s*s', delay_str)
            if seconds_match:
                total_seconds += int(seconds_match.group(1))
            
            # If no time units found, assume it's seconds
            if total_seconds == 0:
                try:
                    total_seconds = int(delay_str)
                except ValueError:
                    return default_seconds
            
            return max(1, total_seconds)  # Minimum 1 second
            
        except Exception:
            return default_seconds

    def _parse_forward_mode(self, mode_str: str) -> ForwardingMode:
        """Parse forward mode string to enum"""
        try:
            mode_map = {
                "1": ForwardingMode.PRESERVE_ORIGINAL,
                "2": ForwardingMode.SILENT,
                "3": ForwardingMode.AS_COPY
            }
            return mode_map.get(str(mode_str), ForwardingMode.PRESERVE_ORIGINAL)
        except Exception:
            return ForwardingMode.PRESERVE_ORIGINAL

    def _check_user_expiry(self, expiry_str: str) -> bool:
        """Check if user has expired based on expiry_date"""
        if not expiry_str:
            return False
            
        try:
            # Parse expiry date format: "2025-10-01-19:30:27"
            expiry_date = datetime.strptime(expiry_str, "%Y-%m-%d-%H:%M:%S")
            current_date = datetime.now()
            
            return current_date > expiry_date
        except Exception:
            return False

    def _create_sample_credentials_file(self, path: Path):
        """Create a sample credentials file with auto-start functionality"""
        sample_credentials = {
            "25910392": {
                "api_id": "25910392",
                "api_hash": "9e32cad6393a8598cc3a693ddfc2d66e",
                "phone": "+917354769260",
                "last_updated": datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
                "delay": "2m 45s",
                "forward_mode": "1",
                "mode_set": True,
                "start": True,
                "auto_start_forwarding": True,
                "expiry_date": "2025-10-01-19:30:27"
            }
        }
        
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open('w', encoding='utf-8') as f:
                json.dump(sample_credentials, f, indent=2)
        except Exception as e:
            pass

    def _create_sample_groups_file(self, path: Path):
        """Create a sample groups file for user reference"""
        sample_groups = {
            "25910392": [
                {
                    "url": "https://t.me/example_channel",
                    "added_date": "2025-09-01T07:20:36.593461",
                    "active": True
                }
            ]
        }
        
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open('w', encoding='utf-8') as f:
                json.dump(sample_groups, f, indent=2)
        except Exception as e:
            pass

    async def setup_user_client(self, api_id: str, user_config: UserConfig) -> bool:
        """Setup Telegram client for a specific user"""
        session_name = f'sessions/{user_config.phone.replace("+", "").replace(" ", "")}'
        
        # Create sessions directory
        Path('sessions').mkdir(exist_ok=True)

        try:
            client = TelegramClient(session_name, user_config.api_id, user_config.api_hash)
            await client.connect()

            if not await client.is_user_authorized():
                if self.headless:
                    return False
                
                success = await self._handle_user_authorization(client, user_config)
                if not success:
                    return False

            # Test connection
            me = await client.get_me()

            self.user_clients[api_id] = client
            return True

        except FloodWaitError as e:
            wait_time = getattr(e, 'seconds', 60)
            return False
        except Exception as e:
            error_msg = f"[{user_config.phone}] Failed to setup client: {e}"
            self.logger.error(error_msg)
            return False

    async def _handle_user_authorization(self, client: TelegramClient, user_config: UserConfig) -> bool:
        """Handle authorization for a specific user"""
        max_attempts = 3

        for attempt in range(max_attempts):
            try:
                await client.send_code_request(user_config.phone)

                code = Utils.get_user_input(f'[{user_config.phone}] Enter verification code')

                try:
                    await client.sign_in(phone=user_config.phone, code=code)
                    return True

                except SessionPasswordNeededError:
                    password = Utils.get_user_input(f'[{user_config.phone}] Enter 2FA password')
                    await client.sign_in(password=password)
                    return True

            except PhoneCodeInvalidError:
                if attempt == max_attempts - 1:
                    return False
            except Exception as e:
                return False

        return False

    async def setup_all_clients(self) -> List[str]:
        """Setup clients for all users and return list of successful user IDs"""
        successful_users = []
        
        for api_id, user_config in self.user_configs.items():
            if await self.setup_user_client(api_id, user_config):
                successful_users.append(api_id)

        return successful_users

    def setup_forwarding_options(self):
        """Setup global forwarding options (now individual users can override these)"""
        # Apply global config defaults
        self.delay = self.global_config.default_delay
        self.forwarding_mode = self.global_config.default_forward_mode
        self.concurrent_users = self.global_config.concurrent_users

        # Update users without individual settings
        for api_id, user_config in self.user_configs.items():
            if not user_config.mode_set:
                user_config.delay = self.delay
                user_config.forward_mode = self.forwarding_mode

    def _should_auto_start_forwarding(self) -> bool:
        """Determine if forwarding should start automatically"""
        if self.global_config.skip_confirmation:
            return True
            
        # Check if all users have auto_start_forwarding enabled
        auto_start_users = [
            user_config for user_config in self.user_configs.values() 
            if user_config.auto_start_forwarding
        ]
        
        total_users = len(self.user_configs)
        auto_start_count = len(auto_start_users)
        
        if auto_start_count == total_users and total_users > 0:
            return True
        elif auto_start_count > 0:
            return True  # Changed to True for auto-start functionality
        else:
            return False

    async def run_user_forwarding_cycle(self, api_id: str, cycle_number: int) -> Dict[str, Any]:
        """Run forwarding cycle for a single user with individual settings"""
        user_config = self.user_configs[api_id]
        client = self.user_clients[api_id]
        forwarder = self.user_forwarders[api_id]

        try:
            # Run forwarding for this user with individual settings
            cycle_results = await forwarder.forward_messages_enhanced(
                client,
                user_config.groups,
                user_config.delay,  # Use user-specific delay
                cycle_number,
                user_config.forward_mode  # Use user-specific forwarding mode
            )

            # Update user statistics
            if cycle_results and cycle_results.get("success"):
                self.user_stats[api_id].success_count += cycle_results.get("successful_forwards", 0)
                self.user_stats[api_id].failed_count += cycle_results.get("failed_forwards", 0)
                self.user_stats[api_id].total_targets = cycle_results.get("total_targets", 0)

            return {"user_id": api_id, "phone": user_config.phone, "results": cycle_results}

        except Exception as e:
            error_msg = f"[{user_config.phone}] Error in forwarding cycle: {e}"
            self.logger.error(error_msg)
            return {"user_id": api_id, "phone": user_config.phone, "error": str(e)}

    async def run_user_loop(self, api_id: str):
        """Independent infinite forwarding loop for each user with clean logging"""
        user_config = self.user_configs.get(api_id)
        if not user_config:
            self.logger.error(f"[{api_id}] No user_config found for loop")
            return

        cycle_number = 0
        # Clean startup log - just show the phone number
        print(f"[{user_config.phone}] Starting forwarding loop...")

        forwarder = self.user_forwarders.get(api_id)
        client = self.user_clients.get(api_id)
        if forwarder is None or client is None:
            print(f"[{user_config.phone}] Missing forwarder/client, stopping loop")
            return

        stats = self.user_stats.get(api_id)
        if stats and not stats.start_time:
            stats.start_time = time.time()

        while not self.shutdown_requested and not user_config.is_expired:
            cycle_number += 1
            try:
                # Show cycle start with clean format
                print(f"[{user_config.phone}] Cycle {cycle_number} starting...")
                
                result = await self.run_user_forwarding_cycle(api_id, cycle_number)
                
                # Clean cycle completion log
                if result.get("results") and result["results"].get("success"):
                    successful = result["results"].get("successful_forwards", 0)
                    failed = result["results"].get("failed_forwards", 0)
                    total = result["results"].get("total_targets", 0)
                    print(f"[{user_config.phone}] Cycle {cycle_number} completed: {successful}/{total} successful, {failed} failed")
                else:
                    print(f"[{user_config.phone}] Cycle {cycle_number} completed with issues")
                    
            except Exception as e:
                print(f"[{user_config.phone}] Cycle {cycle_number} error: {e}")
                self.logger.error(f"[{user_config.phone}] Loop error: {e}", exc_info=True)

            # Clean delay message
            delay_seconds = max(1, int(getattr(user_config, "delay", 1)))
            if delay_seconds > 60:
                delay_min = delay_seconds // 60
                delay_sec = delay_seconds % 60
                if delay_sec > 0:
                    print(f"[{user_config.phone}] Waiting {delay_min}m {delay_sec}s before next cycle...")
                else:
                    print(f"[{user_config.phone}] Waiting {delay_min}m before next cycle...")
            else:
                print(f"[{user_config.phone}] Waiting {delay_seconds}s before next cycle...")
            
            try:
                await asyncio.sleep(delay_seconds)
            except asyncio.CancelledError:
                break

        print(f"[{user_config.phone}] Forwarding loop stopped")

    async def cleanup(self):
        """Cleanup all user resources and save statistics"""
        self.is_running = False
        print("Multi-User Bot shutdown complete!")

        # Display stats for each user
        for api_id, stats in self.user_stats.items():
            if stats.start_time:
                stats.end_time = time.time()
                user_config = self.user_configs.get(api_id)
                if user_config and stats.success_count > 0:
                    runtime = stats.end_time - stats.start_time
                    runtime_str = f"{int(runtime//60)}m {int(runtime%60)}s" if runtime > 60 else f"{int(runtime)}s"
                    print(f"[{user_config.phone}] Final stats: {stats.success_count} successful, {stats.failed_count} failed (Runtime: {runtime_str})")

        # Disconnect all clients
        for api_id, client in self.user_clients.items():
            try:
                await client.disconnect()
            except Exception as e:
                pass

        self.logger.info("Multi-user bot shutdown completed")

# multi_user.py
#!/usr/bin/env python3
"""
Multi-User Bot Module
Handles multi-user coordination, configuration watching, and main loop execution
"""

import asyncio
import time
import json
import os
import threading
from typing import Dict, Any, List
from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from datetime import datetime

from colorama import Fore
from bot_manager import BotManager, UserConfig
from utils import Utils


class ConfigFileWatcher(FileSystemEventHandler):
    """File system event handler to watch for configuration changes"""
    
    def __init__(self, bot_instance):
        self.bot = bot_instance
        self.last_modified = {}
        
    def on_modified(self, event):
        if event.is_directory:
            return
            
        file_path = Path(event.src_path)
        
        # Only watch specific config files
        if file_path.name not in ['credentials.json', 'groups.json', 'config.json']:
            return
            
        # Debounce rapid file changes
        current_time = time.time()
        if file_path in self.last_modified:
            if current_time - self.last_modified[file_path] < 2:  # 2 second debounce
                return
                
        self.last_modified[file_path] = current_time
        
        # Handle the change by scheduling it in the main event loop
        try:
            # Get the event loop from the main thread
            loop = self.bot.main_loop
            if loop and not loop.is_closed():
                # Schedule the coroutine to run in the main event loop thread-safely
                asyncio.run_coroutine_threadsafe(
                    self._handle_config_change(file_path), 
                    loop
                )
        except Exception as e:
            pass
        
    async def _handle_config_change(self, file_path: Path):
        """Handle configuration file changes"""
        try:
            await asyncio.sleep(1)  # Small delay to ensure file write is complete
            
            if file_path.name == 'credentials.json':
                await self._handle_credentials_change()
            elif file_path.name == 'groups.json':
                await self._handle_groups_change()
            elif file_path.name == 'config.json':
                await self._handle_global_config_change()
                
        except Exception as e:
            pass
            
    async def _handle_credentials_change(self):
        """Handle changes to credentials.json with real-time updates"""
        try:
            credentials_path = Path('database/credentials.json')
            if not credentials_path.exists():
                return
                
            with credentials_path.open('r', encoding='utf-8') as f:
                new_credentials = json.load(f)
                
            changes_made = False
            
            # Check each user for changes
            for api_id, cred_info in new_credentials.items():
                if api_id in self.bot.user_configs:
                    old_config = self.bot.user_configs[api_id]
                    
                    # Update last_updated timestamp in real-time
                    new_last_updated = cred_info.get('last_updated')
                    if new_last_updated and new_last_updated != old_config.last_updated:
                        old_config.last_updated = new_last_updated
                        current_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")
                        changes_made = True
                    
                    # Check if user was disabled/enabled
                    new_start = cred_info.get('start', True)
                    if old_config.start != new_start:
                        old_config.start = new_start
                        active_users_count = len(self.bot.get_active_users())
                        total_users_count = len(self.bot.user_configs)
                        
                        if new_start:
                            # Setup client if not exists
                            if api_id not in self.bot.user_clients:
                                success = await self.bot.setup_user_client(api_id, old_config)
                        else:
                            # Disconnect client if running
                            if api_id in self.bot.user_clients:
                                try:
                                    await self.bot.user_clients[api_id].disconnect()
                                    del self.bot.user_clients[api_id]
                                except Exception as e:
                                    pass
                        changes_made = True
                    
                    # Update delay if changed (real-time)
                    new_delay_str = cred_info.get('delay', f'{old_config.delay}s')
                    new_delay = self.bot._parse_user_delay(new_delay_str, old_config.delay)
                    if old_config.delay != new_delay:
                        old_config.delay = new_delay
                        delay_display = f"{new_delay}s"
                        if hasattr(Utils, 'format_time_display'):
                            delay_display = Utils.format_time_display(new_delay)
                        changes_made = True
                    
                    # Update forward mode if changed (real-time)
                    new_forward_mode_str = cred_info.get('forward_mode', '1')
                    new_forward_mode = self.bot._parse_forward_mode(new_forward_mode_str)
                    if old_config.forward_mode != new_forward_mode:
                        old_config.forward_mode = new_forward_mode
                        changes_made = True
                    
                    # Update mode_set if changed (real-time)
                    new_mode_set = cred_info.get('mode_set', old_config.mode_set)
                    if old_config.mode_set != new_mode_set:
                        old_config.mode_set = new_mode_set
                        changes_made = True
                    
                    # Update expiry date if changed (real-time)
                    new_expiry_date = cred_info.get('expiry_date')
                    if old_config.expiry_date != new_expiry_date:
                        old_config.expiry_date = new_expiry_date
                        # Check if user is now expired
                        if new_expiry_date and self.bot._check_user_expiry(new_expiry_date):
                            old_config.is_expired = True
                            old_config.start = False
                        else:
                            old_config.is_expired = False
                        changes_made = True
                    
                    # Update auto_start_forwarding if changed
                    new_auto_start = cred_info.get('auto_start_forwarding', old_config.auto_start_forwarding)
                    if old_config.auto_start_forwarding != new_auto_start:
                        old_config.auto_start_forwarding = new_auto_start
                        changes_made = True
                
        except Exception as e:
            pass
            
    async def _handle_groups_change(self):
        """Handle changes to groups.json"""
        try:
            groups_path = Path('database/groups.json')
            if not groups_path.exists():
                return
                
            with groups_path.open('r', encoding='utf-8') as f:
                content = f.read().strip()
                # Fix common JSON issues
                if content.endswith(',}'):
                    content = content[:-2] + '}'
                elif content.endswith(',]'):
                    content = content[:-2] + ']'
                new_groups = json.loads(content)
                
            changes_made = False
            
            # Update group lists for each user
            for api_id, group_list in new_groups.items():
                if api_id in self.bot.user_configs:
                    user_config = self.bot.user_configs[api_id]
                    
                    # Process active groups
                    new_active_groups = []
                    for group_info in group_list:
                        if isinstance(group_info, dict):
                            if group_info.get('active', True):
                                new_active_groups.append(group_info['url'])
                        else:
                            new_active_groups.append(group_info)
                    
                    # Check if groups changed
                    if set(user_config.groups) != set(new_active_groups):
                        old_count = len(user_config.groups)
                        user_config.groups = new_active_groups
                        new_count = len(new_active_groups)
                        
                        changes_made = True
            
        except Exception as e:
            pass
            
    async def _handle_global_config_change(self):
        """Handle changes to global config"""
        try:
            if self.bot._load_global_config():
                pass
        except Exception as e:
            pass


class MultiUserTelegramBot(BotManager):
    """
    Enhanced multi-user bot class supporting concurrent forwarding
    Extends BotManager to add multi-user coordination and configuration watching
    """
    
    def __init__(self):
        """Initialize multi-user bot components"""
        super().__init__()
        
        # File watcher for real-time config changes
        self.config_watcher = None
        self.file_observer = None

    def _setup_config_watcher(self):
        """Setup file system watcher for configuration changes"""
        try:
            database_path = Path('database')
            if not database_path.exists():
                database_path.mkdir(parents=True, exist_ok=True)
                
            self.config_watcher = ConfigFileWatcher(self)
            self.file_observer = Observer()
            self.file_observer.schedule(self.config_watcher, str(database_path), recursive=False)
            self.file_observer.start()
            
        except Exception as e:
            pass

    def _stop_config_watcher(self):
        """Stop the configuration file watcher"""
        try:
            if self.file_observer:
                self.file_observer.stop()
                self.file_observer.join()
        except Exception as e:
            pass

    async def run_main_loop(self, active_users: List[str]):
        """Enhanced main loop supporting multiple users with real-time config changes"""
        total_groups = sum(len(self.user_configs[user_id].groups) for user_id in active_users)
        delay_display = Utils.format_time_display(self.delay) if hasattr(Utils, 'format_time_display') else f"{self.delay}s"

        additional_info = {
            "Mode": self.forwarding_mode.value,
            "Users": len(active_users),
            "Total Groups": total_groups,
            "Concurrent": self.concurrent_users,
            "Auto-start": self.global_config.auto_start_forwarding
        }

        if hasattr(Utils, 'display_startup_info'):
            Utils.display_startup_info(total_groups, delay_display, additional_info)

        # Store reference to the main event loop for the config watcher
        self.main_loop = asyncio.get_running_loop()

        # Setup configuration watcher for real-time changes
        self._setup_config_watcher()

        # Initialize user stats
        for user_id in active_users:
            self.user_stats[user_id].start_time = time.time()

        self.is_running = True
        loop_count = 0

        try:
            while not self.shutdown_requested:
                loop_count += 1
                self.loop_count = loop_count

                # Get currently active users (may have changed via config file)
                current_active_users = self.get_active_users()
                
                if not current_active_users:
                    await asyncio.sleep(5)
                    continue
                
                # Check if user list changed
                if set(current_active_users) != set(active_users):
                    old_count = len(active_users)
                    active_users = current_active_users
                    new_count = len(active_users)
                    
                    # Initialize stats for new users
                    for user_id in active_users:
                        if user_id not in self.user_stats or not self.user_stats[user_id].start_time:
                            self.user_stats[user_id].start_time = time.time()

                # Run forwarding for all active users
                if self.concurrent_users:
                    # Concurrent execution
                    tasks = [
                        self.run_user_forwarding_cycle(user_id, loop_count)
                        for user_id in active_users
                    ]
                    cycle_results = await asyncio.gather(*tasks, return_exceptions=True)
                else:
                    # Sequential execution
                    cycle_results = []
                    for user_id in active_users:
                        if self.shutdown_requested:
                            break
                        
                        # Check if user is still active before processing
                        if user_id in self.get_active_users():
                            result = await self.run_user_forwarding_cycle(user_id, loop_count)
                            cycle_results.append(result)

                # Process results
                successful_users = 0
                for result in cycle_results:
                    if isinstance(result, dict) and "error" not in result:
                        successful_users += 1

                # Wait between cycles
                for i in range(10):
                    if self.shutdown_requested:
                        break
                    await asyncio.sleep(1)

                # Clear screen for next cycle
                if not self.shutdown_requested:
                    if hasattr(Utils, 'clear_screen'):
                        Utils.clear_screen()
                    if hasattr(Utils, 'display_banner'):
                        Utils.display_banner()

        except Exception as e:
            error_msg = f"Critical error in main loop: {e}"
            self.logger.error(error_msg)
        finally:
            self._stop_config_watcher()
            await self.cleanup()

    async def run(self):
        """Main execution method for multi-user bot"""
        try:
            print(f"{Fore.BLUE}Starting Multi-User Telegram Forwarder v3.0...")

            if hasattr(Utils, 'clear_screen'):
                Utils.clear_screen()
            if hasattr(Utils, 'display_banner'):
                Utils.display_banner()

            # Load user configurations
            if not await self.load_user_configurations():
                return

            # Setup clients for all users
            active_users = await self.setup_all_clients()
            
            if not active_users:
                return

            # Setup forwarding options
            self.setup_forwarding_options()

            # Check if we should start automatically based on JSON configuration
            should_auto_start = self._should_auto_start_forwarding()
            
            if not should_auto_start and not self.headless:
                # Only prompt if auto-start is disabled and not in headless mode
                if hasattr(Utils, 'confirm_action'):
                    if not Utils.confirm_action("Start forwarding?", default=True):
                        return
                else:
                    # Fallback confirmation
                    confirm = input(f"Start forwarding? [Y/n]: ").lower()
                    if confirm in ['n', 'no', 'false']:
                        return

            # Run main loop
            await self.run_main_loop(active_users)

        except KeyboardInterrupt:
            self.shutdown_requested = True
        except Exception as e:
            error_msg = f"Critical error in multi-user bot execution: {e}"
            self.logger.error(error_msg)
        finally:
            await self.cleanup()
            print(f"{Fore.BLUE}Multi-User Bot shutdown complete!")

# config_manager.py
import os
import json
import logging
from colorama import Fore, Style

class ConfigManager:
    """Handles configuration files and credentials management using JSON database"""
    
    def __init__(self, logger):
        self.logger = logger
        self.database_dir = 'database'
        self.credentials_file = os.path.join(self.database_dir, 'credentials.json')
        self.groups_file = os.path.join(self.database_dir, 'groups.json')
    
    def check_and_create_files(self):
        """Check and create necessary directories and files if they don't exist"""
        # Create database directory
        if not os.path.exists(self.database_dir):
            os.makedirs(self.database_dir)
            print(Fore.YELLOW + f"Created {self.database_dir} directory")
            self.logger.info(f"Created database directory: {self.database_dir}")
        
        # Create credentials.json if it doesn't exist
        if not os.path.exists(self.credentials_file):
            with open(self.credentials_file, 'w') as file:
                json.dump({}, file, indent=2)
            print(Fore.YELLOW + f"Created {os.path.basename(self.credentials_file)} file")
            self.logger.info(f"Created missing file: {self.credentials_file}")
        
        # Create groups.json if it doesn't exist
        if not os.path.exists(self.groups_file):
            with open(self.groups_file, 'w') as file:
                json.dump({"groups": []}, file, indent=2)
            print(Fore.YELLOW + f"Created {os.path.basename(self.groups_file)} file")
            self.logger.info(f"Created missing file: {self.groups_file}")
        
        # Create logs directory
        if not os.path.exists('logs'):
            os.makedirs('logs')
            print(Fore.YELLOW + "Created logs directory")
            self.logger.info("Created logs directory")

    def save_credentials(self, api_id, api_hash, phone):
        """Save credentials to JSON file"""
        try:
            credentials_data = {
                "api_id": str(api_id),
                "api_hash": api_hash,
                "phone": phone,
                "last_updated": self._get_current_timestamp()
            }
            
            with open(self.credentials_file, 'w') as file:
                json.dump(credentials_data, file, indent=2)
            
            print(Fore.GREEN + Style.BRIGHT + '✓ Credentials saved successfully.')
            self.logger.info(f"Credentials saved for phone: {phone}")
        except Exception as e:
            print(Fore.RED + f'Error saving credentials: {e}')
            self.logger.error(f"Failed to save credentials: {e}")

    def load_credentials(self):
        """Load credentials from JSON file"""
        try:
            if not os.path.exists(self.credentials_file):
                self.logger.info("No credentials file found")
                return None
            
            with open(self.credentials_file, 'r') as file:
                credentials_data = json.load(file)
            
            # Check if the required fields exist
            required_fields = ['api_id', 'api_hash', 'phone']
            if all(field in credentials_data for field in required_fields):
                phone = credentials_data['phone']
                self.logger.info(f"Credentials loaded for phone: {phone}")
                return credentials_data['api_id'], credentials_data['api_hash'], phone
            else:
                self.logger.warning("Incomplete credentials found in file")
                return None
                
        except json.JSONDecodeError as e:
            print(Fore.RED + f'Error parsing credentials JSON: {e}')
            self.logger.error(f"Failed to parse credentials JSON: {e}")
            return None
        except Exception as e:
            print(Fore.RED + f'Error loading credentials: {e}')
            self.logger.error(f"Failed to load credentials: {e}")
            return None

    def load_group_urls(self):
        """Load group URLs from JSON file"""
        try:
            if not os.path.exists(self.groups_file):
                self.logger.warning("groups.json file not found")
                return []
            
            with open(self.groups_file, 'r') as file:
                groups_data = json.load(file)
            
            # Extract group URLs from the JSON structure
            group_urls = []
            if 'groups' in groups_data:
                if isinstance(groups_data['groups'], list):
                    # Handle both simple list of URLs and list of objects
                    for group in groups_data['groups']:
                        if isinstance(group, str):
                            group_urls.append(group)
                        elif isinstance(group, dict) and 'url' in group:
                            group_urls.append(group['url'])
                        elif isinstance(group, dict) and 'link' in group:
                            group_urls.append(group['link'])
            
            # Filter out empty strings
            group_urls = [url.strip() for url in group_urls if url.strip()]
            
            self.logger.info(f"Loaded {len(group_urls)} group URLs from file")
            return group_urls
            
        except json.JSONDecodeError as e:
            print(Fore.RED + f'Error parsing groups JSON: {e}')
            self.logger.error(f"Failed to parse groups JSON: {e}")
            return []
        except Exception as e:
            print(Fore.RED + f'Error loading group URLs: {e}')
            self.logger.error(f"Failed to load group URLs: {e}")
            return []

    def save_group_urls(self, group_urls):
        """Save group URLs to JSON file"""
        try:
            # Create a more structured format for groups
            groups_data = {
                "groups": [
                    {
                        "url": url,
                        "added_date": self._get_current_timestamp(),
                        "active": True
                    } for url in group_urls
                ],
                "last_updated": self._get_current_timestamp()
            }
            
            with open(self.groups_file, 'w') as file:
                json.dump(groups_data, file, indent=2)
            
            print(Fore.GREEN + Style.BRIGHT + f'✓ Saved {len(group_urls)} group URLs successfully.')
            self.logger.info(f"Saved {len(group_urls)} group URLs to file")
        except Exception as e:
            print(Fore.RED + f'Error saving group URLs: {e}')
            self.logger.error(f"Failed to save group URLs: {e}")

    def add_group_url(self, new_url):
        """Add a single group URL to the existing list"""
        try:
            current_urls = self.load_group_urls()
            
            # Check if URL already exists
            if new_url in current_urls:
                print(Fore.YELLOW + "Group URL already exists")
                self.logger.info(f"Attempted to add duplicate URL: {new_url}")
                return False
            
            # Add new URL
            current_urls.append(new_url)
            self.save_group_urls(current_urls)
            
            print(Fore.GREEN + Style.BRIGHT + '✓ Group URL added successfully.')
            self.logger.info(f"Added new group URL: {new_url}")
            return True
            
        except Exception as e:
            print(Fore.RED + f'Error adding group URL: {e}')
            self.logger.error(f"Failed to add group URL: {e}")
            return False

    def remove_group_url(self, url_to_remove):
        """Remove a group URL from the list"""
        try:
            current_urls = self.load_group_urls()
            
            if url_to_remove in current_urls:
                current_urls.remove(url_to_remove)
                self.save_group_urls(current_urls)
                
                print(Fore.GREEN + Style.BRIGHT + '✓ Group URL removed successfully.')
                self.logger.info(f"Removed group URL: {url_to_remove}")
                return True
            else:
                print(Fore.YELLOW + "Group URL not found")
                self.logger.info(f"Attempted to remove non-existent URL: {url_to_remove}")
                return False
                
        except Exception as e:
            print(Fore.RED + f'Error removing group URL: {e}')
            self.logger.error(f"Failed to remove group URL: {e}")
            return False

    def get_credentials_info(self):
        """Get basic info about stored credentials without revealing sensitive data"""
        try:
            if not os.path.exists(self.credentials_file):
                return None
            
            with open(self.credentials_file, 'r') as file:
                credentials_data = json.load(file)
            
            return {
                "phone": credentials_data.get('phone', 'Unknown'),
                "last_updated": credentials_data.get('last_updated', 'Unknown'),
                "has_api_id": bool(credentials_data.get('api_id')),
                "has_api_hash": bool(credentials_data.get('api_hash'))
            }
            
        except Exception as e:
            self.logger.error(f"Failed to get credentials info: {e}")
            return None

    def _get_current_timestamp(self):
        """Get current timestamp in ISO format"""
        from datetime import datetime
        return datetime.now().isoformat()

    def clear_credentials(self):
        """Clear stored credentials"""
        try:
            if os.path.exists(self.credentials_file):
                with open(self.credentials_file, 'w') as file:
                    json.dump({}, file, indent=2)
                
                print(Fore.GREEN + Style.BRIGHT + '✓ Credentials cleared successfully.')
                self.logger.info("Credentials cleared")
                return True
            else:
                print(Fore.YELLOW + "No credentials file found")
                return False
                
        except Exception as e:
            print(Fore.RED + f'Error clearing credentials: {e}')
            self.logger.error(f"Failed to clear credentials: {e}")
            return False

# logger_setup.py
import os
import json
import logging
import logging.handlers
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from pathlib import Path
from dataclasses import dataclass, asdict
from enum import Enum
import gzip
import shutil

class LogLevel(Enum):
    """Log level enumeration"""
    DEBUG = logging.DEBUG
    INFO = logging.INFO
    WARNING = logging.WARNING
    ERROR = logging.ERROR
    CRITICAL = logging.CRITICAL

@dataclass
class LogConfig:
    """Configuration for logging setup"""
    log_directory: str = "logs"
    max_log_files: int = 30  # Keep logs for 30 days
    max_file_size: int = 10 * 1024 * 1024  # 10MB per file
    backup_count: int = 5  # Keep 5 backup files per log type
    compress_old_logs: bool = True
    log_format: str = "%(asctime)s | %(name)s | %(levelname)s | %(funcName)s:%(lineno)d | %(message)s"
    date_format: str = "%Y-%m-%d %H:%M:%S"
    console_output: bool = True
    console_level: LogLevel = LogLevel.INFO

@dataclass
class ForwardingSession:
    """Data class to track a forwarding session"""
    session_id: str
    start_time: datetime
    end_time: Optional[datetime] = None
    total_targets: int = 0
    successful_forwards: int = 0
    failed_forwards: int = 0
    errors: List[Dict] = None
    message_preview: str = ""
    
    def __post_init__(self):
        if self.errors is None:
            self.errors = []
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate percentage"""
        return (self.successful_forwards / self.total_targets * 100) if self.total_targets > 0 else 0
    
    @property
    def duration(self) -> timedelta:
        """Calculate session duration"""
        end = self.end_time or datetime.now()
        return end - self.start_time

class CustomJSONEncoder(json.JSONEncoder):
    """Custom JSON encoder for datetime and other objects"""
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, timedelta):
            return obj.total_seconds()
        return super().default(obj)

class LoggerSetup:
    """Enhanced logging configuration and operations manager"""
    
    def __init__(self, config: LogConfig = None):
        self.config = config or LogConfig()
        self.logger = None
        self.current_session: Optional[ForwardingSession] = None
        self.log_files = {
            'main': None,
            'success': None,
            'error': None,
            'debug': None,
            'stats': None
        }
        
    def setup_logging(self) -> logging.Logger:
        """Setup comprehensive logging configuration"""
        self._create_log_directory()
        self._cleanup_old_logs()
        
        # Setup main logger
        logger = logging.getLogger('TelegramForwarder')
        logger.setLevel(logging.DEBUG)
        
        # Clear existing handlers
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)
        
        # Setup different log handlers
        self._setup_main_handler(logger)
        self._setup_success_handler(logger)
        self._setup_error_handler(logger)
        self._setup_debug_handler(logger)
        self._setup_stats_handler(logger)
        
        if self.config.console_output:
            self._setup_console_handler(logger)
        
        self.logger = logger
        logger.info("Enhanced logging system initialized")
        logger.info(f"Log configuration: {asdict(self.config)}")
        
        return logger

    def _create_log_directory(self):
        """Create log directory structure"""
        log_path = Path(self.config.log_directory)
        log_path.mkdir(exist_ok=True)
        
        # Create subdirectories for organization
        for subdir in ['daily', 'sessions', 'archives']:
            (log_path / subdir).mkdir(exist_ok=True)

    def _cleanup_old_logs(self):
        """Clean up old log files based on retention policy"""
        try:
            log_path = Path(self.config.log_directory)
            cutoff_date = datetime.now() - timedelta(days=self.config.max_log_files)
            
            for log_file in log_path.glob('*.log'):
                if log_file.stat().st_mtime < cutoff_date.timestamp():
                    if self.config.compress_old_logs:
                        self._compress_and_archive(log_file)
                    else:
                        log_file.unlink()
                        
        except Exception as e:
            print(f"Warning: Failed to cleanup old logs: {e}")

    def _compress_and_archive(self, log_file: Path):
        """Compress old log files and move to archives"""
        try:
            archive_path = Path(self.config.log_directory) / 'archives'
            compressed_name = f"{log_file.stem}_{datetime.now().strftime('%Y%m%d')}.gz"
            compressed_path = archive_path / compressed_name
            
            with open(log_file, 'rb') as f_in:
                with gzip.open(compressed_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            
            log_file.unlink()  # Remove original file
            
        except Exception as e:
            print(f"Warning: Failed to compress log file {log_file}: {e}")

    def _get_rotating_handler(self, filename: str, level: int) -> logging.handlers.RotatingFileHandler:
        """Create a rotating file handler"""
        handler = logging.handlers.RotatingFileHandler(
            filename=filename,
            maxBytes=self.config.max_file_size,
            backupCount=self.config.backup_count,
            encoding='utf-8'
        )
        handler.setLevel(level)
        
        formatter = logging.Formatter(
            self.config.log_format,
            datefmt=self.config.date_format
        )
        handler.setFormatter(formatter)
        
        return handler

    def _setup_main_handler(self, logger: logging.Logger):
        """Setup main activity log handler"""
        filename = os.path.join(
            self.config.log_directory, 
            f"bot_activity_{datetime.now().strftime('%Y%m%d')}.log"
        )
        handler = self._get_rotating_handler(filename, logging.INFO)
        logger.addHandler(handler)
        self.log_files['main'] = filename

    def _setup_success_handler(self, logger: logging.Logger):
        """Setup success-only log handler"""
        filename = os.path.join(
            self.config.log_directory, 
            f"success_{datetime.now().strftime('%Y%m%d')}.log"
        )
        handler = self._get_rotating_handler(filename, logging.INFO)
        handler.addFilter(lambda record: 'SUCCESS' in record.getMessage() or record.levelname == 'SUCCESS')
        logger.addHandler(handler)
        self.log_files['success'] = filename

    def _setup_error_handler(self, logger: logging.Logger):
        """Setup error-only log handler"""
        filename = os.path.join(
            self.config.log_directory, 
            f"errors_{datetime.now().strftime('%Y%m%d')}.log"
        )
        handler = self._get_rotating_handler(filename, logging.ERROR)
        logger.addHandler(handler)
        self.log_files['error'] = filename

    def _setup_debug_handler(self, logger: logging.Logger):
        """Setup debug log handler"""
        filename = os.path.join(
            self.config.log_directory, 
            f"debug_{datetime.now().strftime('%Y%m%d')}.log"
        )
        handler = self._get_rotating_handler(filename, logging.DEBUG)
        handler.addFilter(lambda record: record.levelname == 'DEBUG')
        logger.addHandler(handler)
        self.log_files['debug'] = filename

    def _setup_stats_handler(self, logger: logging.Logger):
        """Setup statistics log handler"""
        filename = os.path.join(
            self.config.log_directory, 
            f"stats_{datetime.now().strftime('%Y%m%d')}.log"
        )
        handler = self._get_rotating_handler(filename, logging.INFO)
        handler.addFilter(lambda record: 'STATS' in record.getMessage() or 'FORWARDING' in record.getMessage())
        logger.addHandler(handler)
        self.log_files['stats'] = filename

    def _setup_console_handler(self, logger: logging.Logger):
        """Setup console output handler"""
        console_handler = logging.StreamHandler()
        console_handler.setLevel(self.config.console_level.value)
        
        # Simplified format for console
        console_formatter = logging.Formatter(
            '%(asctime)s | %(levelname)s | %(message)s',
            datefmt='%H:%M:%S'
        )
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)

    def start_forwarding_session(self, session_id: str = None, total_targets: int = 0, message_preview: str = "") -> ForwardingSession:
        """Start a new forwarding session"""
        if not session_id:
            session_id = f"session_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        self.current_session = ForwardingSession(
            session_id=session_id,
            start_time=datetime.now(),
            total_targets=total_targets,
            message_preview=message_preview[:100]  # Limit preview length
        )
        
        self.log_info(f"SESSION_START | {session_id} | Targets: {total_targets}")
        return self.current_session

    def end_forwarding_session(self) -> Optional[ForwardingSession]:
        """End the current forwarding session"""
        if not self.current_session:
            return None
        
        self.current_session.end_time = datetime.now()
        
        # Log session summary
        self.log_session_summary(self.current_session)
        
        # Save session data
        self._save_session_data(self.current_session)
        
        session = self.current_session
        self.current_session = None
        return session

    def log_forwarding_result(self, target: str, success: bool, error_message: str = None):
        """Log individual forwarding result"""
        if not self.current_session:
            self.log_warning("No active session for forwarding result")
            return
        
        if success:
            self.current_session.successful_forwards += 1
            self.log_success(f"Forward to {target}")
        else:
            self.current_session.failed_forwards += 1
            error_data = {
                'target': target,
                'error': error_message,
                'timestamp': datetime.now().isoformat()
            }
            self.current_session.errors.append(error_data)
            self.log_error(f"Forward failed to {target}: {error_message}")

    def log_session_summary(self, session: ForwardingSession):
        """Log comprehensive session summary"""
        summary = {
            'session_id': session.session_id,
            'duration': session.duration.total_seconds(),
            'total_targets': session.total_targets,
            'successful_forwards': session.successful_forwards,
            'failed_forwards': session.failed_forwards,
            'success_rate': session.success_rate,
            'errors_count': len(session.errors)
        }
        
        self.log_info(f"STATS | SESSION_SUMMARY | {json.dumps(summary)}")

    def _save_session_data(self, session: ForwardingSession):
        """Save detailed session data to JSON file"""
        try:
            sessions_dir = Path(self.config.log_directory) / 'sessions'
            session_file = sessions_dir / f"{session.session_id}.json"
            
            session_data = asdict(session)
            
            with open(session_file, 'w', encoding='utf-8') as f:
                json.dump(session_data, f, indent=2, cls=CustomJSONEncoder)
                
        except Exception as e:
            self.log_error(f"Failed to save session data: {e}")

    def write_daily_summary(self, additional_stats: Dict = None):
        """Write comprehensive daily summary"""
        try:
            summary_file = Path(self.config.log_directory) / 'daily' / f"summary_{datetime.now().strftime('%Y%m%d')}.txt"
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # Collect daily statistics
            daily_stats = self._collect_daily_statistics()
            
            with open(summary_file, 'w', encoding='utf-8') as f:
                f.write(f"{'='*80}\n")
                f.write(f"DAILY SUMMARY - {timestamp}\n")
                f.write(f"{'='*80}\n\n")
                
                # Session statistics
                f.write("📊 SESSION STATISTICS:\n")
                f.write(f"   Total Sessions: {daily_stats.get('total_sessions', 0)}\n")
                f.write(f"   Total Forwards: {daily_stats.get('total_forwards', 0)}\n")
                f.write(f"   Successful: {daily_stats.get('successful_forwards', 0)}\n")
                f.write(f"   Failed: {daily_stats.get('failed_forwards', 0)}\n")
                f.write(f"   Success Rate: {daily_stats.get('success_rate', 0):.1f}%\n\n")
                
                # Error analysis
                if daily_stats.get('common_errors'):
                    f.write("❌ COMMON ERRORS:\n")
                    for error, count in daily_stats['common_errors'].items():
                        f.write(f"   {error}: {count} occurrences\n")
                    f.write("\n")
                
                # Additional stats if provided
                if additional_stats:
                    f.write("📈 ADDITIONAL STATISTICS:\n")
                    for key, value in additional_stats.items():
                        f.write(f"   {key}: {value}\n")
                    f.write("\n")
                
                f.write(f"{'='*80}\n")
                f.write("Enhanced Telegram Forwarder v2.5\n")
                f.write(f"Generated: {timestamp}\n")
                f.write(f"{'='*80}\n")
                
        except Exception as e:
            self.log_error(f"Failed to write daily summary: {e}")

    def _collect_daily_statistics(self) -> Dict:
        """Collect statistics from daily session files"""
        try:
            sessions_dir = Path(self.config.log_directory) / 'sessions'
            today = datetime.now().strftime('%Y%m%d')
            
            stats = {
                'total_sessions': 0,
                'total_forwards': 0,
                'successful_forwards': 0,
                'failed_forwards': 0,
                'common_errors': {}
            }
            
            for session_file in sessions_dir.glob(f"session_{today}_*.json"):
                try:
                    with open(session_file, 'r', encoding='utf-8') as f:
                        session_data = json.load(f)
                    
                    stats['total_sessions'] += 1
                    stats['successful_forwards'] += session_data.get('successful_forwards', 0)
                    stats['failed_forwards'] += session_data.get('failed_forwards', 0)
                    
                    # Analyze errors
                    for error in session_data.get('errors', []):
                        error_msg = error.get('error', 'Unknown error')
                        stats['common_errors'][error_msg] = stats['common_errors'].get(error_msg, 0) + 1
                        
                except Exception:
                    continue
            
            stats['total_forwards'] = stats['successful_forwards'] + stats['failed_forwards']
            if stats['total_forwards'] > 0:
                stats['success_rate'] = (stats['successful_forwards'] / stats['total_forwards']) * 100
            else:
                stats['success_rate'] = 0
                
            return stats
            
        except Exception as e:
            self.log_error(f"Failed to collect daily statistics: {e}")
            return {}

    def get_log_file_info(self) -> Dict[str, Dict]:
        """Get information about current log files"""
        info = {}
        
        for log_type, filename in self.log_files.items():
            if filename and os.path.exists(filename):
                stat = os.stat(filename)
                info[log_type] = {
                    'filename': filename,
                    'size': stat.st_size,
                    'size_human': self._format_file_size(stat.st_size),
                    'modified': datetime.fromtimestamp(stat.st_mtime).isoformat()
                }
            else:
                info[log_type] = {'filename': filename, 'exists': False}
                
        return info

    def _format_file_size(self, size_bytes: int) -> str:
        """Format file size in human readable format"""
        if size_bytes == 0:
            return "0 B"
        
        size_names = ["B", "KB", "MB", "GB", "TB"]
        i = 0
        while size_bytes >= 1024.0 and i < len(size_names) - 1:
            size_bytes /= 1024.0
            i += 1
        
        return f"{size_bytes:.1f} {size_names[i]}"

    # Enhanced logging methods with better formatting
    def log_success(self, message: str, extra_data: Dict = None):
        """Log success message with optional extra data"""
        if self.logger:
            log_message = f"SUCCESS | {message}"
            if extra_data:
                log_message += f" | {json.dumps(extra_data)}"
            self.logger.info(log_message)

    def log_error(self, message: str, exception: Exception = None, extra_data: Dict = None):
        """Log error message with optional exception and extra data"""
        if self.logger:
            log_message = f"ERROR | {message}"
            if exception:
                log_message += f" | Exception: {str(exception)}"
            if extra_data:
                log_message += f" | {json.dumps(extra_data)}"
            self.logger.error(log_message, exc_info=exception is not None)

    def log_warning(self, message: str, extra_data: Dict = None):
        """Log warning message with optional extra data"""
        if self.logger:
            log_message = f"WARNING | {message}"
            if extra_data:
                log_message += f" | {json.dumps(extra_data)}"
            self.logger.warning(log_message)

    def log_info(self, message: str, extra_data: Dict = None):
        """Log info message with optional extra data"""
        if self.logger:
            log_message = message
            if extra_data:
                log_message += f" | {json.dumps(extra_data)}"
            self.logger.info(log_message)

    def log_debug(self, message: str, extra_data: Dict = None):
        """Log debug message with optional extra data"""
        if self.logger:
            log_message = f"DEBUG | {message}"
            if extra_data:
                log_message += f" | {json.dumps(extra_data)}"
            self.logger.debug(log_message)

    def export_logs(self, start_date: datetime = None, end_date: datetime = None, log_types: List[str] = None) -> str:
        """Export logs for a specific date range and types"""
        try:
            if not start_date:
                start_date = datetime.now() - timedelta(days=7)  # Last week by default
            if not end_date:
                end_date = datetime.now()
            if not log_types:
                log_types = ['main', 'success', 'error']
            
            export_filename = f"logs/export_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.txt"
            
            with open(export_filename, 'w', encoding='utf-8') as export_file:
                export_file.write(f"Log Export - {start_date} to {end_date}\n")
                export_file.write("="*80 + "\n\n")
                
                for log_type in log_types:
                    if log_type in self.log_files:
                        export_file.write(f"[{log_type.upper()} LOGS]\n")
                        export_file.write("-"*40 + "\n")
                        
                        # Here you would implement date-range filtering
                        # This is a simplified version
                        filename = self.log_files[log_type]
                        if filename and os.path.exists(filename):
                            with open(filename, 'r', encoding='utf-8') as log_file:
                                export_file.write(log_file.read())
                        
                        export_file.write("\n\n")
            
            self.log_info(f"Logs exported to: {export_filename}")
            return export_filename
            
        except Exception as e:
            self.log_error(f"Failed to export logs: {e}")
            return ""

# message_forwarder.py
import time
import asyncio
from typing import List, Tuple, Optional, Dict, Any, Union
from datetime import datetime, timedelta
from dataclasses import dataclass
from enum import Enum

from telethon.tl.functions.messages import GetHistoryRequest, ForwardMessagesRequest, SendMessageRequest
from telethon.tl.types import Message, MessageMediaPhoto, MessageMediaDocument, Channel, Chat
from telethon.errors import (
    FloodWaitError, ChatAdminRequiredError, UserBannedInChannelError, 
    ChannelPrivateError, MessageNotModifiedError, SlowModeWaitError,
    UsernameNotOccupiedError, ChatWriteForbiddenError, UserNotParticipantError,
    MessageIdInvalidError, InviteHashExpiredError, InviteHashInvalidError,
    UserAlreadyParticipantError
)

# Try to import topic-related errors if they exist
try:
    from telethon.errors import TopicClosedError
except ImportError:
    try:
        from telethon.errors import TopicDeletedError as TopicClosedError
    except ImportError:
        class TopicClosedError(Exception):
            pass

from telethon import TelegramClient
from colorama import Fore, Style

from utils import Utils
from logger_setup import ForwardingSession

class ForwardingMode(Enum):
    """Enumeration for different forwarding modes"""
    PRESERVE_ORIGINAL = "preserve"
    SILENT = "silent"
    AS_COPY = "copy"

class MessageType(Enum):
    """Enumeration for message types"""
    TEXT = "text"
    PHOTO = "photo"
    VIDEO = "video"
    DOCUMENT = "document"
    AUDIO = "audio"
    STICKER = "sticker"
    VOICE = "voice"
    VIDEO_NOTE = "video_note"
    POLL = "poll"
    LOCATION = "location"
    CONTACT = "contact"
    UNKNOWN = "unknown"

@dataclass
class ForwardingTarget:
    """Enhanced data class representing a forwarding target"""
    url: str
    identifier: str
    topic_id: Optional[int]
    url_type: str
    chat_id: Optional[int]
    entity: Any = None
    retry_count: int = 0
    last_error: Optional[str] = None
    last_attempt: Optional[datetime] = None
    success_count: int = 0
    failure_count: int = 0
    # Enhanced fields for invite link support
    invite_hash: Optional[str] = None
    requires_join: bool = False
    entity_type: Optional[str] = None  # 'channel', 'group', 'supergroup'
    entity_title: Optional[str] = None
    join_attempted: bool = False
    join_successful: bool = False

@dataclass
class ForwardingResult:
    """Enhanced data class for forwarding results"""
    target: ForwardingTarget
    success: bool
    message: str
    execution_time: float
    message_type: MessageType
    error_type: Optional[str] = None
    retry_after: Optional[int] = None
    # Enhanced fields
    join_attempted: bool = False
    join_successful: bool = False
    entity_info: Optional[Dict] = None

class MessageForwarder:
    """
    Enhanced message forwarding operations with comprehensive invite link support
    
    Key enhancements:
    - Automatic invite link joining
    - Better entity type detection
    - Improved error handling for invite-specific errors
    - Enhanced logging for join attempts
    """
    
    def __init__(self, logger_setup, url_parser):
        self.logger_setup = logger_setup
        self.url_parser = url_parser
        self.logger = logger_setup.logger
        self.current_session: Optional[ForwardingSession] = None
        
        # Enhanced configuration
        self.default_mode = ForwardingMode.PRESERVE_ORIGINAL
        self.max_retries = 3
        self.base_retry_delay = 30
        self.enable_flood_wait_handling = True
        self.enable_auto_join = True  # New: Enable automatic joining
        self.join_timeout = 30  # Timeout for join operations
        
        # Enhanced statistics
        self.total_forwards = 0
        self.successful_forwards = 0
        self.failed_forwards = 0
        self.flood_waits_encountered = 0
        self.access_denied_count = 0
        self.successful_joins = 0  # New: Track successful joins
        self.failed_joins = 0  # New: Track failed joins

    async def test_invite_link(self, client: TelegramClient, invite_url: str) -> Dict[str, Any]:
        """
        Test an invite link to see if it can be joined and used for forwarding
        """
        test_result = {
            'url': invite_url,
            'valid': False,
            'can_join': False,
            'entity_info': {},
            'error': None
        }
        
        try:
            # Parse the invite URL
            parsed_url = self.url_parser.parse_telegram_url(invite_url)
            
            if not parsed_url.is_valid:
                test_result['error'] = "Invalid URL format"
                return test_result
            
            if parsed_url.url_type not in ['invite_link_plus', 'joinchat', 'invite_link_hash']:
                test_result['error'] = "Not an invite link"
                return test_result
            
            test_result['valid'] = True
            
            # Try to resolve the entity (this will join if needed)
            entity, join_info = await self._resolve_entity_with_join(client, ForwardingTarget(
                url=invite_url,
                identifier=parsed_url.identifier,
                topic_id=parsed_url.topic_id,
                url_type=parsed_url.url_type,
                chat_id=parsed_url.chat_id
            ))
            
            test_result['can_join'] = join_info.get('join_successful', False)
            test_result['entity_info'] = join_info.get('entity_info', {})
            
            return test_result
            
        except Exception as e:
            test_result['error'] = str(e)
            return test_result

    async def _resolve_entity_with_join(
        self, client: TelegramClient, target: ForwardingTarget
    ) -> Tuple[Any, Dict]:
        """
        Enhanced entity resolution with automatic joining for invite links
        """
        join_info = {
            'join_attempted': False,
            'join_successful': False,
            'entity_info': {}
        }

        
        try:
            # Parse URL to get detailed information
            parsed_url = self.url_parser.parse_telegram_url(target.url)
            
            # Update target with parsed information
            target.invite_hash = getattr(parsed_url, 'invite_hash', None)
            target.requires_join = getattr(parsed_url, 'requires_join', False)
            
            # Log the resolution attempt
            self.logger_setup.log_info(f"Resolving entity for {target.url} (type: {parsed_url.url_type})")
            
            # Resolve entity using enhanced parser
            entity = await self.url_parser.resolve_entity_advanced(client, parsed_url)
            
            # Extract entity information
            entity_info = self._extract_entity_info(entity)
            join_info['entity_info'] = entity_info
            
            # Update target with entity information
            target.entity_type = entity_info.get('type', 'unknown')
            target.entity_title = entity_info.get('title', 'Unknown')
            
            # If this was an invite link, mark join as successful
            if parsed_url.url_type in ['invite_link_plus', 'joinchat', 'invite_link_hash']:
                join_info['join_attempted'] = True
                join_info['join_successful'] = True
                target.join_attempted = True
                target.join_successful = True
                self.successful_joins += 1
                
                self.logger_setup.log_success(
                    f"Successfully joined {entity_info.get('type', 'chat')}: {entity_info.get('title', 'Unknown')}",
                    extra_data=entity_info
                )
            
            return entity, join_info
            
        except UserAlreadyParticipantError:
            # Already in the chat, this is actually success
            self.logger_setup.log_info(f"Already a participant in {target.url}")
            join_info['join_attempted'] = True
            join_info['join_successful'] = True
            
            # Try to get entity info anyway
            try:
                entity = await self.url_parser.resolve_entity_advanced(client, parsed_url)
                entity_info = self._extract_entity_info(entity)
                join_info['entity_info'] = entity_info
                return entity, join_info
            except Exception as e:
                raise Exception(f"Already participant but couldn't get entity: {str(e)}")
                
        except (InviteHashExpiredError, InviteHashInvalidError) as e:
            join_info['join_attempted'] = True
            join_info['join_successful'] = False
            self.failed_joins += 1
            
            error_msg = f"Invalid or expired invite link: {target.url}"
            self.logger_setup.log_error(error_msg, exception=e)
            raise Exception(error_msg)
            
        except Exception as e:
            # If it's an invite link, mark join as failed
            parsed_url = self.url_parser.parse_telegram_url(target.url)
            if parsed_url.url_type in ['invite_link_plus', 'joinchat', 'invite_link_hash']:
                join_info['join_attempted'] = True
                join_info['join_successful'] = False
                target.join_attempted = True
                target.join_successful = False
                self.failed_joins += 1
            
            error_msg = f"Failed to resolve entity {target.url}: {str(e)}"
            self.logger_setup.log_error(error_msg, exception=e)
            raise Exception(error_msg)

    def _extract_entity_info(self, entity) -> Dict:
        """Extract useful information from a Telegram entity"""
        info = {
            'id': getattr(entity, 'id', None),
            'title': getattr(entity, 'title', None),
            'username': getattr(entity, 'username', None),
            'type': 'unknown'
        }
        
        # Determine entity type
        if isinstance(entity, Channel):
            if getattr(entity, 'broadcast', False):
                info['type'] = 'channel'
            elif getattr(entity, 'megagroup', False):
                info['type'] = 'supergroup'
            else:
                info['type'] = 'channel'
        elif isinstance(entity, Chat):
            info['type'] = 'group'
        else:
            info['type'] = type(entity).__name__.lower()
        
        # Additional information
        info['participants_count'] = getattr(entity, 'participants_count', None)
        info['access_hash'] = getattr(entity, 'access_hash', None)
        
        return info

    async def _forward_to_topic(self, client: TelegramClient, target: ForwardingTarget, 
                               message: Message, mode: ForwardingMode) -> Tuple[bool, str]:
        """Enhanced topic-specific forwarding"""
        try:
            entity_type = getattr(target, 'entity_type', 'unknown')
            entity_title = getattr(target, 'entity_title', 'Unknown')
            
            self.logger_setup.log_info(f"Forwarding to topic {target.topic_id} in {entity_type}: {entity_title}")
            
            if mode == ForwardingMode.AS_COPY:
                if message.message:
                    await client.send_message(
                        entity=target.entity,
                        message=message.message,
                        reply_to=target.topic_id
                    )
                    return True, f"Copied to topic {target.topic_id} in {entity_title}"
                else:
                    await client.send_file(
                        entity=target.entity,
                        file=message.media,
                        caption=message.message or "",
                        reply_to=target.topic_id
                    )
                    return True, f"Media copied to topic {target.topic_id} in {entity_title}"
            else:
                await client(ForwardMessagesRequest(
                    from_peer='me',
                    id=[message.id],
                    to_peer=target.entity,
                    top_msg_id=target.topic_id,
                    silent=(mode == ForwardingMode.SILENT)
                ))
                return True, f"Forwarded to topic {target.topic_id} in {entity_title}"
                
        except TopicClosedError:
            self.logger_setup.log_warning(f"Topic {target.topic_id} is closed, trying main chat")
            return await self._forward_to_main_chat_fallback(client, target, message, mode)
        except MessageIdInvalidError:
            self.logger_setup.log_warning(f"Topic {target.topic_id} doesn't exist, trying main chat")
            return await self._forward_to_main_chat_fallback(client, target, message, mode)
        except Exception as e:
            self.logger_setup.log_error(f"Topic forwarding failed: {str(e)}")
            return await self._forward_to_main_chat_fallback(client, target, message, mode)

    async def _forward_to_entity(self, client: TelegramClient, target: ForwardingTarget, 
                                message: Message, mode: ForwardingMode) -> Tuple[bool, str]:
        """Enhanced regular entity forwarding"""
        entity_type = getattr(target, 'entity_type', 'unknown')
        entity_title = getattr(target, 'entity_title', 'Unknown')
        
        self.logger_setup.log_info(f"Forwarding to {entity_type}: {entity_title}")
        
        if mode == ForwardingMode.AS_COPY:
            if message.message:
                await client.send_message(
                    entity=target.entity,
                    message=message.message
                )
                return True, f"Copied to {entity_title}"
            else:
                await client.send_file(
                    entity=target.entity,
                    file=message.media,
                    caption=message.message or ""
                )
                return True, f"Media copied to {entity_title}"
        else:
            await client.forward_messages(
                entity=target.entity,
                messages=message.id,
                from_peer='me',
                silent=(mode == ForwardingMode.SILENT)
            )
            return True, f"Forwarded to {entity_title}"

    async def _forward_to_main_chat_fallback(self, client: TelegramClient, target: ForwardingTarget, 
                                           message: Message, mode: ForwardingMode) -> Tuple[bool, str]:
        """Enhanced fallback forwarding to main chat"""
        try:
            entity_title = getattr(target, 'entity_title', 'Unknown')
            
            await client.forward_messages(
                entity=target.entity,
                messages=message.id,
                from_peer='me',
                silent=(mode == ForwardingMode.SILENT)
            )
            return True, f"Forwarded to main chat in {entity_title} (topic {target.topic_id} unavailable)"
        except Exception as fallback_error:
            return False, str(fallback_error)

    def _classify_error(self, error: Exception) -> str:
        """Enhanced error classification including invite-specific errors"""
        if isinstance(error, FloodWaitError):
            return "flood_wait"
        elif isinstance(error, (ChannelPrivateError, ChatAdminRequiredError, UserBannedInChannelError)):
            return "access_denied"
        elif isinstance(error, SlowModeWaitError):
            return "slow_mode"
        elif isinstance(error, (UsernameNotOccupiedError, UsernameInvalidError)):
            return "invalid_target"
        elif isinstance(error, ChatWriteForbiddenError):
            return "write_forbidden"
        elif isinstance(error, UserNotParticipantError):
            return "not_participant"
        elif isinstance(error, TopicClosedError):
            return "topic_closed"
        elif isinstance(error, (InviteHashExpiredError, InviteHashInvalidError)):
            return "invite_invalid"
        elif isinstance(error, UserAlreadyParticipantError):
            return "already_participant"
        else:
            return "unknown"

    def _get_retry_delay(self, error: Exception) -> Optional[int]:
        """Get appropriate retry delay based on error type"""
        if isinstance(error, FloodWaitError):
            return error.seconds
        elif isinstance(error, SlowModeWaitError):
            return error.seconds
        else:
            return None

    async def forward_messages_enhanced(self, client: TelegramClient, group_urls: List[str], 
                                       delay: int, loop_count: int, mode: ForwardingMode = None,
                                       source: str = 'me') -> Dict[str, Any]:
        """
        Enhanced forwarding function with comprehensive invite link support
        """
        session_id = f"enhanced_forward_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        session = self.logger_setup.start_forwarding_session(
            session_id=session_id,
            total_targets=len(group_urls)
        )
        
        try:
            print(Fore.BLUE + Style.BRIGHT + f"Fetching latest message from {source}...")
            self.logger_setup.log_info(f"Starting enhanced forward cycle #{loop_count}", extra_data={
                "mode": (mode or self.default_mode).value,
                "source": source,
                "targets": len(group_urls)
            })
            
            # Retrieve the latest message
            latest_message, message_preview = await self.get_latest_message(client, source)
            if not latest_message:
                print(Fore.RED + Style.BRIGHT + f'Failed: {message_preview}')
                return {"success": False, "error": message_preview}

            session.message_preview = message_preview
            
            # Display operation details with enhanced info
            print(Fore.CYAN + f"Message preview: {message_preview}")
            print(Fore.BLUE + Style.BRIGHT + f"Starting enhanced forward to {len(group_urls)} targets...")
            print(Fore.MAGENTA + Style.BRIGHT + f"Mode: {(mode or self.default_mode).value.upper()}")
            print(Fore.YELLOW + Style.BRIGHT + f"Auto-join enabled: {self.enable_auto_join}")
            print("")

            # Prepare targets with enhanced parsing
            targets = await self._prepare_targets_enhanced(client, group_urls)
            
            # Initialize enhanced statistics
            results = []
            flood_waits = 0
            access_denied = 0
            successful_joins = 0
            failed_joins = 0
            
            # Process each target with enhanced handling
            for i, target in enumerate(targets, 1):
                try:
                    self._display_enhanced_target_info(i, len(targets), target)
                    
                    # Attempt forwarding with enhanced retry logic
                    result = await self._forward_with_retry_enhanced(client, target, latest_message, mode)
                    results.append(result)
                    
                    # Enhanced result handling
                    if result.success:
                        success_msg = f"SUCCESS: {result.message}"
                        if result.join_attempted:
                            if result.join_successful:
                                success_msg += f" (Joined: {result.entity_info.get('title', 'Unknown')})"
                                successful_joins += 1
                            else:
                                failed_joins += 1
                        print(Fore.GREEN + Style.BRIGHT + success_msg)
                        self.logger_setup.log_forwarding_result(target.identifier, True)
                    else:
                        error_msg = f"FAILED: {result.message}"
                        if result.join_attempted and not result.join_successful:
                            error_msg += " (Join failed)"
                            failed_joins += 1
                        print(Fore.RED + Style.BRIGHT + error_msg)
                        self.logger_setup.log_forwarding_result(target.identifier, False, result.message)
                        
                        # Track specific error types
                        if result.error_type == "flood_wait":
                            flood_waits += 1
                        elif result.error_type == "access_denied":
                            access_denied += 1
                    
                    # Handle special delays
                    if result.retry_after and i < len(targets):
                        wait_time = result.retry_after
                        print(Fore.YELLOW + f"Required wait: {Utils.format_time_display(wait_time)}")
                        await asyncio.sleep(wait_time)
                    elif i < len(targets):
                        print(Fore.BLUE + f"Waiting {Utils.format_time_display(delay)}...")
                        await asyncio.sleep(delay)
                        
                except Exception as e:
                    error_msg = f"Critical error processing target {target.url}: {e}"
                    print(Fore.RED + Style.BRIGHT + f"Critical error: {error_msg}")
                    self.logger_setup.log_error(error_msg, exception=e)

            # Calculate enhanced statistics
            stats = self._calculate_enhanced_statistics(results, flood_waits, access_denied, 
                                                      successful_joins, failed_joins)
            
            # Display enhanced final statistics
            self._display_enhanced_final_stats(stats, results)
            
            # End session
            self.logger_setup.end_forwarding_session()
            
            return stats
            
        except Exception as e:
            error_msg = f'Critical error during enhanced forwarding: {e}'
            print(Fore.RED + Style.BRIGHT + f'Critical error: {error_msg}')
            self.logger_setup.log_error(error_msg, exception=e)
            self.logger_setup.end_forwarding_session()
            return {"success": False, "error": error_msg}

    async def _prepare_targets_enhanced(self, client: TelegramClient, group_urls: List[str]) -> List[ForwardingTarget]:
        """Enhanced target preparation with invite link analysis"""
        targets = []
        
        print(Fore.CYAN + "Preparing targets with enhanced parsing...")
        
        for url in group_urls:
            try:
                parsed = self.url_parser.parse_telegram_url(url)
                if not parsed.is_valid:
                    self.logger_setup.log_warning(f"Invalid URL skipped: {url}")
                    continue
                
                target = ForwardingTarget(
                    url=parsed.original_url,
                    identifier=parsed.identifier,
                    topic_id=parsed.topic_id,
                    url_type=parsed.url_type,
                    chat_id=parsed.chat_id,
                    invite_hash=getattr(parsed, 'invite_hash', None),
                    requires_join=getattr(parsed, 'requires_join', False)
                )
                
                targets.append(target)
                
            except Exception as e:
                self.logger_setup.log_error(f"Failed to prepare target {url}: {e}")
                print(f"Error preparing target {url}: {e}")
        
        # Enhanced target summary
        invite_targets = sum(1 for t in targets if t.requires_join)
        print(Fore.GREEN + f"Prepared {len(targets)} valid targets ({invite_targets} invite links)")
        
        return targets

    async def _forward_with_retry_enhanced(self, client: TelegramClient, target: ForwardingTarget, 
                                         message: Message, mode: ForwardingMode) -> ForwardingResult:
        """Enhanced forwarding with improved retry logic"""
        last_result = None
        
        for attempt in range(self.max_retries + 1):
            if attempt > 0:
                delay = self.base_retry_delay * (2 ** (attempt - 1))
                print(Fore.YELLOW + f"  Retry {attempt}/{self.max_retries} after {delay}s...")
                await asyncio.sleep(delay)
            
            result = await self.forward_to_target(client, target, message, mode)
            last_result = result
            
            if result.success:
                return result
            
            # Enhanced retry logic - don't retry certain error types
            if result.error_type in ["access_denied", "invalid_target", "write_forbidden", 
                                   "invite_invalid", "already_participant"]:
                break
            
            # Enhanced flood wait handling
            if result.error_type == "flood_wait" and result.retry_after:
                if self.enable_flood_wait_handling:
                    print(Fore.YELLOW + f"  Flood wait: {result.retry_after}s")
                    await asyncio.sleep(result.retry_after + 1)
                    continue
                else:
                    break
        
        return last_result

    def _calculate_enhanced_statistics(self, results: List[ForwardingResult], flood_waits: int, 
                                     access_denied: int, successful_joins: int, failed_joins: int) -> Dict[str, Any]:
        """Calculate comprehensive enhanced statistics"""
        successful = sum(1 for r in results if r.success)
        failed = len(results) - successful
        total_time = sum(r.execution_time for r in results)
        
        # Enhanced error type analysis
        error_types = {}
        for result in results:
            if not result.success and result.error_type:
                error_types[result.error_type] = error_types.get(result.error_type, 0) + 1
        
        # Message type analysis
        message_types = {}
        for result in results:
            msg_type = result.message_type.value
            message_types[msg_type] = message_types.get(msg_type, 0) + 1
        
        # Join statistics
        join_attempts = sum(1 for r in results if r.join_attempted)
        
        return {
            "success": True,
            "successful_forwards": successful,
            "failed_forwards": failed,
            "total_targets": len(results),
            "success_rate": (successful / len(results) * 100) if results else 0,
            "total_execution_time": total_time,
            "average_execution_time": total_time / len(results) if results else 0,
            "flood_waits": flood_waits,
            "access_denied": access_denied,
            "error_types": error_types,
            "message_types": message_types,
            "results": results,
            # Enhanced statistics
            "join_attempts": join_attempts,
            "successful_joins": successful_joins,
            "failed_joins": failed_joins,
            "join_success_rate": (successful_joins / join_attempts * 100) if join_attempts > 0 else 0
        }

    def _display_enhanced_target_info(self, current: int, total: int, target: ForwardingTarget):
        """Enhanced target information display"""
        class TempParsedURL:
            def __init__(self, target):
                self.identifier = target.identifier
                self.topic_id = target.topic_id
                self.url_type = target.url_type
                self.chat_id = target.chat_id
                self.is_valid = True
                self.original_url = target.url
                self.requires_join = target.requires_join
        
        temp_parsed = TempParsedURL(target)
        display_info = self.url_parser.get_url_display_info(temp_parsed)
        
        progress = f"[{current}/{total}]"
        print(Fore.YELLOW + f"{progress} -> {display_info}...", end=" ")

    def _display_enhanced_final_stats(self, stats: Dict[str, Any], results: List[ForwardingResult]):
        """Enhanced final statistics display with join information"""
        print("\n")
        print(Fore.CYAN + Style.BRIGHT + "=" * 70)
        print(Fore.CYAN + Style.BRIGHT + " " * 20 + "ENHANCED FORWARDING STATISTICS" + " " * 19)
        print(Fore.CYAN + Style.BRIGHT + "=" * 70)
        
        # Basic forwarding stats
        success_rate = stats["success_rate"]
        rate_color = Fore.GREEN if success_rate >= 80 else Fore.YELLOW if success_rate >= 50 else Fore.RED
        
        print(f"{Fore.GREEN}Successful Forwards: {stats['successful_forwards']:<10} {Fore.BLUE}Avg Time: {stats['average_execution_time']:.2f}s")
        print(f"{Fore.RED}Failed Forwards: {stats['failed_forwards']:<14} {Fore.YELLOW}Flood Waits: {stats['flood_waits']}")
        print(f"{rate_color}Success Rate: {success_rate:.1f}%{' ' * 15} {Fore.RED}Access Denied: {stats['access_denied']}")
        
        # Enhanced join statistics
        if stats["join_attempts"] > 0:
            print(Fore.CYAN + Style.BRIGHT + "=" * 70)
            join_rate_color = Fore.GREEN if stats["join_success_rate"] >= 80 else Fore.YELLOW if stats["join_success_rate"] >= 50 else Fore.RED
            print(f"{Fore.MAGENTA}Join Attempts: {stats['join_attempts']:<16} {join_rate_color}Join Success Rate: {stats['join_success_rate']:.1f}%")
            print(f"{Fore.GREEN}Successful Joins: {stats['successful_joins']:<11} {Fore.RED}Failed Joins: {stats['failed_joins']}")
        
        # Error breakdown if any
        if stats["error_types"]:
            print(Fore.CYAN + Style.BRIGHT + "=" * 70)
            print(f"{Fore.MAGENTA}Error Breakdown:")
            for error_type, count in stats["error_types"].items():
                error_name = error_type.replace('_', ' ').title()
                print(f"    {Fore.WHITE}{error_name}: {count}")
        
        print(Fore.CYAN + Style.BRIGHT + "=" * 70)

    def get_enhanced_statistics(self) -> Dict[str, Any]:
        """Get enhanced forwarding statistics including join data"""
        return {
            "total_forwards": self.total_forwards,
            "successful_forwards": self.successful_forwards,
            "failed_forwards": self.failed_forwards,
            "success_rate": (self.successful_forwards / self.total_forwards * 100) if self.total_forwards > 0 else 0,
            "flood_waits_encountered": self.flood_waits_encountered,
            "access_denied_count": self.access_denied_count,
            "successful_joins": self.successful_joins,
            "failed_joins": self.failed_joins,
            "join_success_rate": (self.successful_joins / (self.successful_joins + self.failed_joins) * 100) 
                                if (self.successful_joins + self.failed_joins) > 0 else 0
        }

    async def get_latest_message(self, client: TelegramClient, source: str = 'me') -> Tuple[Optional[Message], str]:
        """Retrieve the most recent message from specified source"""
        try:
            self.logger_setup.log_debug(f"Fetching latest message from source: {source}")
            
            saved_messages = await client(GetHistoryRequest(
                peer=source,
                offset_id=0,
                offset_date=None,
                add_offset=0,
                limit=1,
                max_id=0,
                min_id=0,
                hash=0
            ))
            
            if not saved_messages.messages:
                error_msg = f"No messages found in source: {source}"
                self.logger_setup.log_warning(error_msg)
                return None, error_msg
            
            latest_message = saved_messages.messages[0]
            message_preview = await self._get_enhanced_message_preview(latest_message)
            message_type = self._detect_message_type(latest_message)
            
            self.logger_setup.log_success(f"Retrieved message from {source}", extra_data={
                "message_id": latest_message.id,
                "message_type": message_type.value,
                "preview": message_preview
            })
            
            return latest_message, message_preview
            
        except Exception as e:
            error_msg = f"Error fetching latest message from {source}: {e}"
            self.logger_setup.log_error(error_msg, exception=e)
            return None, error_msg

    async def _get_enhanced_message_preview(self, message: Message) -> str:
        """Create an enhanced preview of the message content"""
        message_type = self._detect_message_type(message)
        
        if message_type == MessageType.TEXT:
            if message.message and len(message.message) > 50:
                return f"Text: {message.message[:50]}..."
            elif message.message:
                return f"Text: {message.message}"
            else:
                return "Empty text message"
                
        elif message_type == MessageType.PHOTO:
            caption = message.message[:30] + "..." if message.message and len(message.message) > 30 else message.message or ""
            return f"Photo{f' - {caption}' if caption else ''}"
            
        elif message_type == MessageType.VIDEO:
            caption = message.message[:30] + "..." if message.message and len(message.message) > 30 else message.message or ""
            return f"Video{f' - {caption}' if caption else ''}"
            
        elif message_type == MessageType.DOCUMENT:
            filename = "Unknown"
            if hasattr(message.media, 'document') and message.media.document:
                for attr in message.media.document.attributes:
                    if hasattr(attr, 'file_name') and attr.file_name:
                        filename = attr.file_name
                        break
            return f"Document: {filename}"
            
        elif message_type == MessageType.AUDIO:
            return "Audio file"
        elif message_type == MessageType.STICKER:
            return "Sticker"
        elif message_type == MessageType.VOICE:
            return "Voice message"
        elif message_type == MessageType.VIDEO_NOTE:
            return "Video note"
        elif message_type == MessageType.POLL:
            question = message.media.poll.question if hasattr(message.media, 'poll') else 'Unknown'
            return f"Poll: {question[:30]}..."
        elif message_type == MessageType.LOCATION:
            return "Location"
        elif message_type == MessageType.CONTACT:
            return "Contact"
        else:
            return "Unknown content"

    def _detect_message_type(self, message: Message) -> MessageType:
        """Detect the type of message content"""
        if not message.media:
            return MessageType.TEXT
        
        if isinstance(message.media, MessageMediaPhoto):
            return MessageType.PHOTO
        elif isinstance(message.media, MessageMediaDocument):
            if hasattr(message.media, 'document') and message.media.document:
                mime_type = getattr(message.media.document, 'mime_type', '')
                
                if mime_type.startswith('video/'):
                    return MessageType.VIDEO
                elif mime_type.startswith('audio/'):
                    return MessageType.AUDIO
                elif 'sticker' in mime_type:
                    return MessageType.STICKER
                elif mime_type == 'audio/ogg':
                    return MessageType.VOICE
                else:
                    for attr in message.media.document.attributes:
                        attr_type = type(attr).__name__
                        if 'Video' in attr_type:
                            if hasattr(attr, 'round_message') and attr.round_message:
                                return MessageType.VIDEO_NOTE
                            return MessageType.VIDEO
                        elif 'Audio' in attr_type:
                            if hasattr(attr, 'voice') and attr.voice:
                                return MessageType.VOICE
                            return MessageType.AUDIO
            return MessageType.DOCUMENT
        else:
            media_type = type(message.media).__name__
            if 'Poll' in media_type:
                return MessageType.POLL
            elif 'Geo' in media_type or 'Location' in media_type:
                return MessageType.LOCATION
            elif 'Contact' in media_type:
                return MessageType.CONTACT
            
            return MessageType.UNKNOWN

    async def forward_to_target(self, client: TelegramClient, target: ForwardingTarget, 
                               message: Message, mode: ForwardingMode = None) -> ForwardingResult:
        """
        Enhanced forwarding with comprehensive invite link support
        """
        start_time = time.time()
        mode = mode or self.default_mode
        message_type = self._detect_message_type(message)
        join_attempted = False
        join_successful = False
        entity_info = {}
        
        try:
            # Enhanced entity resolution with automatic joining
            if not target.entity:
                target.entity, join_info = await self._resolve_entity_with_join(client, target)
                join_attempted = join_info.get('join_attempted', False)
                join_successful = join_info.get('join_successful', False)
                entity_info = join_info.get('entity_info', {})
            
            # Handle different forwarding modes and target types
            if target.topic_id:
                success, result_msg = await self._forward_to_topic(client, target, message, mode)
            else:
                success, result_msg = await self._forward_to_entity(client, target, message, mode)
            
            execution_time = time.time() - start_time
            
            if success:
                target.success_count += 1
                target.last_error = None
                
                return ForwardingResult(
                    target=target,
                    success=True,
                    message=result_msg,
                    execution_time=execution_time,
                    message_type=message_type,
                    join_attempted=join_attempted,
                    join_successful=join_successful,
                    entity_info=entity_info
                )
            else:
                raise Exception(result_msg)
                
        except Exception as e:
            execution_time = time.time() - start_time
            target.failure_count += 1
            target.last_error = str(e)
            target.last_attempt = datetime.now()
            
            error_type = self._classify_error(e)
            retry_after = self._get_retry_delay(e)
            
            return ForwardingResult(
                target=target,
                success=False,
                message=str(e),
                execution_time=execution_time,
                message_type=message_type,
                error_type=error_type,
                retry_after=retry_after,
                join_attempted=join_attempted,
                join_successful=join_successful,
                entity_info=entity_info
            )

# url_parser.py
import re
import logging
from typing import Tuple, Optional, Union
from telethon.tl.types import PeerChannel, PeerChat, PeerUser, Channel, Chat
from telethon.errors import (
    UsernameNotOccupiedError, UsernameInvalidError, ChannelPrivateError,
    InviteHashExpiredError, InviteHashInvalidError, UserAlreadyParticipantError
)
from telethon.tl.functions.messages import CheckChatInviteRequest, ImportChatInviteRequest
from dataclasses import dataclass

@dataclass
class ParsedURL:
    """Data class to hold parsed URL information"""
    identifier: str
    topic_id: Optional[int]
    url_type: str
    chat_id: Optional[int]
    is_valid: bool
    original_url: str
    invite_hash: Optional[str] = None  # Added for invite links
    requires_join: bool = False  # Added to track if joining is needed

class URLParser:
    """Enhanced URL parser with comprehensive invite link support"""
    
    # Updated regex patterns to better handle invite links
    URL_PATTERNS = {
        'private_topic': re.compile(r'https?://t\.me/c/(-?\d+)/(\d+)/?(?:\?[^#]*)?(?:#.*)?$', re.IGNORECASE),
        'private_channel': re.compile(r'https?://t\.me/c/(-?\d+)/?(?:\?[^#]*)?(?:#.*)?$', re.IGNORECASE),
        'public_topic': re.compile(r'https?://t\.me/([a-zA-Z][a-zA-Z0-9_]{3,31})/(\d+)/?(?:\?[^#]*)?(?:#.*)?$', re.IGNORECASE),
        'public_channel': re.compile(r'https?://t\.me/([a-zA-Z][a-zA-Z0-9_]{3,31})/?(?:\?[^#]*)?(?:#.*)?$', re.IGNORECASE),
        'username_format': re.compile(r'^@([a-zA-Z][a-zA-Z0-9_]{3,31})/?$'),
        'direct_chat_id': re.compile(r'^(-?\d+)$'),
        'joinchat': re.compile(r'https?://t\.me/joinchat/([a-zA-Z0-9_-]+)/?(?:\?[^#]*)?(?:#.*)?$', re.IGNORECASE),
        'invite_link_plus': re.compile(r'https?://t\.me/\+([a-zA-Z0-9_-]+)/?(?:\?[^#]*)?(?:#.*)?$', re.IGNORECASE),  # Updated pattern
        'invite_link_hash': re.compile(r'https?://t\.me/([a-zA-Z0-9_-]{22,})/?(?:\?[^#]*)?(?:#.*)?$', re.IGNORECASE)  # For hash-based invites
    }
    
    def __init__(self, logger):
        self.logger = logger
    
    def parse_telegram_url(self, url: str) -> ParsedURL:
        """
        Enhanced parser with comprehensive invite link support
        """
        try:
            original_url = url
            url = url.strip()
            
            if not url:
                return ParsedURL(
                    identifier=original_url,
                    topic_id=None,
                    url_type='empty',
                    chat_id=None,
                    is_valid=False,
                    original_url=original_url
                )
            
            # Try each pattern in priority order
            for url_type, pattern in self.URL_PATTERNS.items():
                match = pattern.match(url)
                if match:
                    return self._process_match(match, url_type, original_url)
            
            # If no pattern matches, try to handle as username without @
            if self._is_valid_username(url):
                return ParsedURL(
                    identifier=url,
                    topic_id=None,
                    url_type='plain_username',
                    chat_id=None,
                    is_valid=True,
                    original_url=original_url
                )
            
            return ParsedURL(
                identifier=url,
                topic_id=None,
                url_type='unknown',
                chat_id=None,
                is_valid=False,
                original_url=original_url
            )
            
        except Exception as e:
            self.logger.error(f"Error parsing URL {url}: {e}")
            return ParsedURL(
                identifier=original_url,
                topic_id=None,
                url_type='error',
                chat_id=None,
                is_valid=False,
                original_url=original_url
            )

    def _process_match(self, match: re.Match, url_type: str, original_url: str) -> ParsedURL:
        """Enhanced match processing with invite link support"""
        try:
            if url_type in ['invite_link_plus', 'joinchat', 'invite_link_hash']:
                # Handle invite links
                invite_hash = match.group(1)
                return ParsedURL(
                    identifier=invite_hash,
                    topic_id=None,
                    url_type=url_type,
                    chat_id=None,
                    is_valid=len(invite_hash) >= 10,  # Basic validation
                    original_url=original_url,
                    invite_hash=invite_hash,
                    requires_join=True  # Invite links typically require joining
                )
            
            elif url_type == 'private_topic':
                chat_id = int(match.group(1))
                topic_id = int(match.group(2))
                formatted_chat_id = self._format_chat_id(chat_id)
                return ParsedURL(
                    identifier=str(formatted_chat_id),
                    topic_id=topic_id,
                    url_type=url_type,
                    chat_id=formatted_chat_id,
                    is_valid=True,
                    original_url=original_url
                )
            
            elif url_type == 'private_channel':
                chat_id = int(match.group(1))
                formatted_chat_id = self._format_chat_id(chat_id)
                return ParsedURL(
                    identifier=str(formatted_chat_id),
                    topic_id=None,
                    url_type=url_type,
                    chat_id=formatted_chat_id,
                    is_valid=True,
                    original_url=original_url
                )
            
            elif url_type == 'public_topic':
                username = match.group(1)
                topic_id = int(match.group(2))
                return ParsedURL(
                    identifier=username,
                    topic_id=topic_id,
                    url_type=url_type,
                    chat_id=None,
                    is_valid=self._is_valid_username(username),
                    original_url=original_url
                )
            
            elif url_type == 'public_channel':
                username = match.group(1)
                return ParsedURL(
                    identifier=username,
                    topic_id=None,
                    url_type=url_type,
                    chat_id=None,
                    is_valid=self._is_valid_username(username),
                    original_url=original_url
                )
            
            elif url_type == 'username_format':
                username = match.group(1)
                return ParsedURL(
                    identifier=username,
                    topic_id=None,
                    url_type=url_type,
                    chat_id=None,
                    is_valid=self._is_valid_username(username),
                    original_url=original_url
                )
            
            elif url_type == 'direct_chat_id':
                chat_id = int(match.group(1))
                return ParsedURL(
                    identifier=str(chat_id),
                    topic_id=None,
                    url_type=url_type,
                    chat_id=chat_id,
                    is_valid=True,
                    original_url=original_url
                )
            
        except (ValueError, IndexError) as e:
            self.logger.error(f"Error processing match for {url_type}: {e}")
            
        return ParsedURL(
            identifier=original_url,
            topic_id=None,
            url_type='invalid',
            chat_id=None,
            is_valid=False,
            original_url=original_url
        )

    def _format_chat_id(self, chat_id: int) -> int:
        """Format chat ID to proper channel ID format"""
        if chat_id > 0:
            return int('-100' + str(chat_id))
        return chat_id

    def _is_valid_username(self, username: str) -> bool:
        """Validate Telegram username format"""
        if not username:
            return False
        
        username = username.lstrip('@')
        
        if len(username) < 5 or len(username) > 32:
            return False
        
        if not re.match(r'^[a-zA-Z][a-zA-Z0-9_]*[a-zA-Z0-9]$', username):
            return False
        
        if '__' in username:
            return False
        
        return True

    async def resolve_entity_advanced(self, client, parsed_url: ParsedURL):
        """
        Enhanced entity resolution with comprehensive invite link support
        """
        try:
            url_type = parsed_url.url_type
            identifier = parsed_url.identifier
            chat_id = parsed_url.chat_id
            
            # Handle invite links with automatic joining
            if url_type in ['invite_link_plus', 'joinchat', 'invite_link_hash']:
                return await self._resolve_invite_link(client, parsed_url)
            
            elif url_type in ['private_channel', 'private_topic']:
                return await self._resolve_private_entity(client, chat_id)
            
            elif url_type == 'direct_chat_id':
                return await self._resolve_by_id(client, chat_id)
            
            elif url_type in ['public_channel', 'public_topic', 'username_format', 'plain_username']:
                return await self._resolve_public_entity(client, identifier)
            
            else:
                return await client.get_entity(identifier)
                    
        except Exception as e:
            self.logger.error(f"Failed to resolve entity {identifier} (type: {url_type}): {e}")
            raise e

    async def _resolve_invite_link(self, client, parsed_url: ParsedURL):
        """
        Enhanced invite link resolution with automatic joining
        """
        invite_hash = parsed_url.invite_hash
        
        try:
            # First, check the invite without joining
            self.logger.info(f"Checking invite link: {invite_hash}")
            invite_info = await client(CheckChatInviteRequest(hash=invite_hash))
            
            # Log invite information
            if hasattr(invite_info, 'chat'):
                chat_info = invite_info.chat
                self.logger.info(f"Invite leads to: {getattr(chat_info, 'title', 'Unknown')} "
                               f"(Type: {type(chat_info).__name__})")
                
                # If we already have access, return the chat
                if hasattr(invite_info, 'chat') and not getattr(invite_info, 'request_needed', False):
                    return chat_info
            
            # Join the chat/channel using the invite
            self.logger.info(f"Attempting to join using invite: {invite_hash}")
            result = await client(ImportChatInviteRequest(hash=invite_hash))
            
            # Extract the chat from the result
            if hasattr(result, 'chats') and result.chats:
                joined_chat = result.chats[0]
                self.logger.info(f"Successfully joined: {getattr(joined_chat, 'title', 'Unknown')}")
                return joined_chat
            elif hasattr(result, 'chat'):
                self.logger.info(f"Successfully joined: {getattr(result.chat, 'title', 'Unknown')}")
                return result.chat
            else:
                raise Exception("Could not extract chat information from join result")
                
        except UserAlreadyParticipantError:
            # We're already in the chat, try to get it by checking the invite again
            self.logger.info("Already a participant, retrieving chat info")
            invite_info = await client(CheckChatInviteRequest(hash=invite_hash))
            if hasattr(invite_info, 'chat'):
                return invite_info.chat
            raise Exception("Already a participant but could not retrieve chat info")
            
        except InviteHashExpiredError:
            raise Exception(f"Invite link has expired: {parsed_url.original_url}")
            
        except InviteHashInvalidError:
            raise Exception(f"Invalid invite link: {parsed_url.original_url}")
            
        except Exception as e:
            raise Exception(f"Failed to join using invite link {parsed_url.original_url}: {str(e)}")

    async def _resolve_private_entity(self, client, chat_id: int):
        """Resolve private channel/group entity"""
        try:
            return await client.get_entity(chat_id)
        except Exception as e:
            if chat_id < 0:
                try:
                    actual_id = abs(chat_id)
                    if str(actual_id).startswith('100'):
                        actual_id = int(str(actual_id)[3:])
                    return await client.get_entity(PeerChannel(actual_id))
                except Exception:
                    pass
            raise e

    async def _resolve_by_id(self, client, chat_id: int):
        """Resolve entity by direct chat ID"""
        return await client.get_entity(chat_id)

    async def _resolve_public_entity(self, client, username: str):
        """Resolve public channel/group entity"""
        try:
            if not username.startswith('@'):
                return await client.get_entity('@' + username)
            else:
                return await client.get_entity(username)
        except (UsernameNotOccupiedError, UsernameInvalidError):
            clean_username = username.lstrip('@')
            if clean_username != username:
                return await client.get_entity(clean_username)
            raise

    def get_url_display_info(self, parsed_url) -> str:
        """Enhanced display information including invite link types"""
        try:
            original_url = getattr(parsed_url, 'original_url', getattr(parsed_url, 'url', 'Unknown URL'))
            url_type = getattr(parsed_url, 'url_type', 'unknown')
            identifier = getattr(parsed_url, 'identifier', 'Unknown')
            topic_id = getattr(parsed_url, 'topic_id', None)
            chat_id = getattr(parsed_url, 'chat_id', None)
            is_valid = getattr(parsed_url, 'is_valid', False)
            requires_join = getattr(parsed_url, 'requires_join', False)
            
            if not is_valid:
                return f"❌ Invalid URL: {original_url}"
            
            join_indicator = " (requires join)" if requires_join else ""
            
            if url_type == 'private_topic':
                return f"🔒 Private topic {topic_id} in channel {chat_id}"
            elif url_type == 'private_channel':
                return f"🔒 Private channel {chat_id}"
            elif url_type == 'public_topic':
                return f"📢 Topic {topic_id} in @{identifier}"
            elif url_type in ['public_channel', 'username_format', 'plain_username']:
                return f"📢 @{identifier}"
            elif url_type == 'direct_chat_id':
                entity_type = self._get_entity_type_by_id(chat_id)
                return f"{entity_type} {chat_id}"
            elif url_type in ['invite_link_plus', 'joinchat', 'invite_link_hash']:
                return f"🔗 Invite link: {identifier[:15]}...{join_indicator}"
            else:
                return f"❓ Unknown: {identifier}"
                
        except Exception as e:
            return f"❌ Error displaying URL info: {str(e)}"

    def _get_entity_type_by_id(self, chat_id: int) -> str:
        """Determine entity type based on chat ID"""
        if chat_id > 0:
            return "👤 User"
        elif str(abs(chat_id)).startswith('100'):
            return "📢 Channel"
        else:
            return "👥 Group"

    def validate_url_batch(self, urls: list) -> dict:
        """Enhanced batch validation with invite link categorization"""
        results = {
            'valid': [],
            'invalid': [],
            'private': [],
            'public': [],
            'topics': [],
            'invite_links': [],
            'requires_join': []  # New category for links requiring join
        }
        
        for url in urls:
            parsed = self.parse_telegram_url(url)
            
            if parsed.is_valid:
                results['valid'].append(parsed)
                
                if parsed.url_type.startswith('private'):
                    results['private'].append(parsed)
                elif parsed.url_type.startswith('public') or parsed.url_type in ['username_format', 'plain_username']:
                    results['public'].append(parsed)
                    
                if parsed.topic_id is not None:
                    results['topics'].append(parsed)
                    
                if parsed.url_type in ['invite_link_plus', 'joinchat', 'invite_link_hash']:
                    results['invite_links'].append(parsed)
                    
                if getattr(parsed, 'requires_join', False):
                    results['requires_join'].append(parsed)
            else:
                results['invalid'].append(parsed)
        
        return results
        
# utils.py
import os
import re
import time
import json
import platform
from datetime import datetime, timedelta
from typing import Optional, Dict, Tuple, List
from colorama import Fore, Style, init
from dataclasses import dataclass

# Initialize colorama for cross-platform color support
init(autoreset=True)

@dataclass
class ForwardingStats:
    """Data class to hold forwarding statistics"""
    success_count: int = 0
    failed_count: int = 0
    total_targets: int = 0
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    errors: List[str] = None
    
    def __post_init__(self):
        if self.errors is None:
            self.errors = []
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate as percentage"""
        return (self.success_count / self.total_targets * 100) if self.total_targets > 0 else 0
    
    @property
    def duration(self) -> timedelta:
        """Calculate total duration"""
        if self.start_time and self.end_time:
            return timedelta(seconds=self.end_time - self.start_time)
        return timedelta()

class Utils:
    """Enhanced utility functions for the Telegram forwarding bot"""
    
    # Color schemes for different message types
    COLORS = {
        'success': Fore.GREEN,
        'error': Fore.RED,
        'warning': Fore.YELLOW,
        'info': Fore.CYAN,
        'highlight': Fore.MAGENTA,
        'accent': Fore.BLUE,
        'neutral': Fore.WHITE
    }
    
    # Time unit multipliers
    TIME_UNITS = {
        'ms': 0.001,  # milliseconds
        's': 1,       # seconds
        'm': 60,      # minutes
        'h': 3600,    # hours
        'd': 86400    # days
    }
    
    @staticmethod
    def parse_time_input(time_str: str) -> int:
        """
        Enhanced parser for time input with support for multiple formats
        Supports: '1h 5m 30s', '5m 30s', '30s', '1500ms', '1.5h', etc.
        Returns total seconds (rounded to integer)
        """
        if not time_str or not time_str.strip():
            return 5  # Default 5 seconds
        
        time_str = time_str.strip().lower().replace(',', '')
        
        # Handle simple numeric input (assume seconds)
        if time_str.replace('.', '').isdigit():
            return max(1, int(float(time_str)))
        
        total_seconds = 0.0
        
        # Enhanced regex patterns for different time formats
        patterns = [
            (r'(\d+(?:\.\d+)?)\s*d(?:ays?)?', 'd'),
            (r'(\d+(?:\.\d+)?)\s*h(?:ours?|rs?)?', 'h'),
            (r'(\d+(?:\.\d+)?)\s*m(?:ins?|inutes?)?(?!s)', 'm'),  # minutes (not ms)
            (r'(\d+(?:\.\d+)?)\s*s(?:ecs?|econds?)?(?!$)', 's'),  # seconds
            (r'(\d+(?:\.\d+)?)\s*ms', 'ms')  # milliseconds
        ]
        
        for pattern, unit in patterns:
            matches = re.findall(pattern, time_str)
            for match in matches:
                value = float(match)
                total_seconds += value * Utils.TIME_UNITS[unit]
        
        # If no valid time patterns found, try to extract first number as seconds
        if total_seconds == 0:
            numbers = re.findall(r'\d+(?:\.\d+)?', time_str)
            if numbers:
                total_seconds = float(numbers[0])
        
        return max(1, int(total_seconds))  # Minimum 1 second

    @staticmethod
    def format_time_display(seconds: int, include_ms: bool = False) -> str:
        """
        Enhanced time formatting with optional millisecond support
        """
        if seconds == 0:
            return "0s"
        
        # Handle sub-second times
        if seconds < 1 and include_ms:
            ms = int(seconds * 1000)
            return f"{ms}ms"
        
        parts = []
        remaining = seconds
        
        # Days
        if remaining >= 86400:
            days = remaining // 86400
            parts.append(f"{days}d")
            remaining %= 86400
        
        # Hours
        if remaining >= 3600:
            hours = remaining // 3600
            parts.append(f"{hours}h")
            remaining %= 3600
        
        # Minutes
        if remaining >= 60:
            minutes = remaining // 60
            parts.append(f"{minutes}m")
            remaining %= 60
        
        # Seconds
        if remaining > 0 or not parts:  # Always show seconds if no other parts
            parts.append(f"{remaining}s")
        
        return " ".join(parts)

    @staticmethod
    def clear_screen():
        """Enhanced cross-platform screen clearing"""
        try:
            if platform.system().lower() == 'windows':
                os.system('cls')
            else:
                os.system('clear')
        except Exception:
            # Fallback: print newlines
            print('\n' * 50)

    @staticmethod
    def display_banner():
        """Enhanced banner with better formatting"""
        banner_text = """
╔══════════════════════════════════════════════════════════════╗
║                 🤖 TELEGRAM AUTO FORWARDER V2.5             ║
║                     Enhanced Multi-Format Bot                ║
╚══════════════════════════════════════════════════════════════╝
        """
        
        print(Fore.CYAN + Style.BRIGHT + banner_text)
        print(Fore.GREEN + Style.BRIGHT + "🚀 Features:")
        print(Fore.WHITE + "   ✓ Private/Public Channels & Groups")
        print(Fore.WHITE + "   ✓ Topic-based Forwarding")
        print(Fore.WHITE + "   ✓ Multiple URL Formats")
        print(Fore.WHITE + "   ✓ Advanced Error Handling")
        print(Fore.WHITE + "   ✓ Real-time Statistics")
        print(Fore.CYAN + Style.BRIGHT + "═" * 66)
        print()

    @staticmethod
    def display_supported_formats():
        """Enhanced format display with examples"""
        print(Fore.CYAN + Style.BRIGHT + "\n📋 Supported URL Formats:")
        
        formats = [
            ("Public Channels", "https://t.me/example_channel", "📢"),
            ("Public Topics", "https://t.me/example_channel/123", "📝"),
            ("Private Channels", "https://t.me/c/1234567890", "🔒"),
            ("Private Topics", "https://t.me/c/1234567890/456", "🔐"),
            ("Username Format", "@example_channel", "👤"),
            ("Direct Chat ID", "-1001234567890", "🆔"),
            ("Invite Links", "https://t.me/+abc123xyz", "🔗"),
            ("Join Chat Links", "https://t.me/joinchat/abc123", "🔗")
        ]
        
        for name, example, emoji in formats:
            print(f"   {emoji} {Fore.YELLOW}{name:<15}{Fore.WHITE}: {example}")
        
        print()

    @staticmethod
    def display_time_examples():
        """Enhanced time format examples"""
        print(Fore.CYAN + Style.BRIGHT + "⏱️  Time Format Examples:")
        
        examples = [
            ("30s", "30 seconds"),
            ("5m", "5 minutes"),
            ("1h 30m", "1 hour 30 minutes"),
            ("2h 15m 45s", "2 hours 15 minutes 45 seconds"),
            ("1.5h", "1.5 hours (90 minutes)"),
            ("500ms", "500 milliseconds"),
            ("1d 12h", "1 day 12 hours"),
            ("30", "30 seconds (plain number)")
        ]
        
        for format_ex, description in examples:
            print(f"   {Fore.GREEN}•{Fore.WHITE} {format_ex:<12} = {description}")
        
        print()

    @staticmethod
    def get_user_input(prompt: str, default: str = "", input_type: str = "string") -> str:
        """
        Enhanced user input with type validation and better prompting
        """
        display_prompt = prompt
        if default:
            display_prompt += f" [{Fore.YELLOW}{default}{Fore.WHITE}]"
        display_prompt += ": "
        
        while True:
            try:
                user_input = input(Fore.WHITE + display_prompt).strip()
                result = user_input if user_input else default
                
                # Type validation
                if input_type == "int" and result:
                    int(result)
                elif input_type == "float" and result:
                    float(result)
                elif input_type == "time" and result:
                    Utils.parse_time_input(result)  # Validate time format
                
                return result
                
            except ValueError:
                print(f"{Fore.RED}Invalid {input_type} format. Please try again.")
            except KeyboardInterrupt:
                print(f"\n{Fore.YELLOW}Operation cancelled.")
                return default

    @staticmethod
    def display_startup_info(group_count: int, delay_display: str, additional_info: Dict = None):
        """Enhanced startup information display"""
        print()
        print(Fore.GREEN + Style.BRIGHT + "🚀 Bot Configuration Summary:")
        print(Fore.CYAN + "═" * 50)
        
        # Basic info
        print(f"{Fore.WHITE}📊 Target Groups/Channels: {Fore.YELLOW}{group_count}")
        print(f"{Fore.WHITE}⏱️  Forward Delay: {Fore.YELLOW}{delay_display}")
        print(f"{Fore.WHITE}🕒 Start Time: {Fore.YELLOW}{datetime.now().strftime('%H:%M:%S')}")
        
        # Additional info if provided
        if additional_info:
            for key, value in additional_info.items():
                print(f"{Fore.WHITE}{key}: {Fore.YELLOW}{value}")
        
        print(Fore.CYAN + "═" * 50)
        print(f"{Fore.GREEN}✅ Bot ready! Press {Fore.RED}Ctrl+C{Fore.GREEN} to stop")
        print(f"{Fore.BLUE}📁 Logs saved to 'logs/' directory")
        print()

    @staticmethod
    def display_cycle_header(loop_count: int, additional_info: str = ""):
        """Enhanced cycle header with more information"""
        timestamp = datetime.now().strftime('%H:%M:%S')
        separator = "─" * 60
        
        print(f"{Fore.MAGENTA}{separator}")
        header = f"🔄 Cycle #{loop_count} | {timestamp}"
        if additional_info:
            header += f" | {additional_info}"
        print(f"{Fore.MAGENTA}{Style.BRIGHT}{header}")
        print(f"{Fore.MAGENTA}{separator}")

    @staticmethod
    def display_progress_bar(current: int, total: int, width: int = 40):
        """Display a progress bar"""
        if total == 0:
            return
        
        progress = current / total
        filled = int(width * progress)
        bar = "█" * filled + "░" * (width - filled)
        percentage = progress * 100
        
        print(f"\r{Fore.CYAN}Progress: [{bar}] {percentage:.1f}% ({current}/{total})", end="", flush=True)

    @staticmethod
    def display_final_stats(stats: ForwardingStats):
        """Enhanced final statistics display"""
        print("\n")
        print(Fore.CYAN + Style.BRIGHT + "╔" + "═" * 60 + "╗")
        print(Fore.CYAN + Style.BRIGHT + "║" + " " * 22 + "FINAL STATISTICS" + " " * 22 + "║")
        print(Fore.CYAN + Style.BRIGHT + "╠" + "═" * 60 + "╣")
        
        # Success/Failure counts
        print(f"║ {Fore.GREEN}✅ Successful forwards: {stats.success_count:<25} {Fore.CYAN}║")
        print(f"║ {Fore.RED}❌ Failed forwards: {stats.failed_count:<29} {Fore.CYAN}║")
        print(f"║ {Fore.BLUE}📊 Total targets: {stats.total_targets:<31} {Fore.CYAN}║")
        
        # Success rate with color coding
        success_rate = stats.success_rate
        rate_color = Fore.GREEN if success_rate >= 80 else Fore.YELLOW if success_rate >= 50 else Fore.RED
        print(f"║ {rate_color}📈 Success rate: {success_rate:.1f}%{' ' * (31 - len(f'{success_rate:.1f}%'))} {Fore.CYAN}║")
        
        # Duration if available
        if stats.duration:
            duration_str = Utils.format_time_display(int(stats.duration.total_seconds()))
            print(f"║ {Fore.MAGENTA}⏱️  Total duration: {duration_str:<28} {Fore.CYAN}║")
        
        # Mode info
        print(f"║ {Fore.YELLOW}🔄 Mode: Enhanced Multi-Format{' ' * 21} {Fore.CYAN}║")
        
        print(Fore.CYAN + Style.BRIGHT + "╚" + "═" * 60 + "╝")

    @staticmethod
    def save_stats_to_file(stats: ForwardingStats, filename: str = None):
        """Save statistics to JSON file"""
        if not filename:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"logs/stats_{timestamp}.json"
        
        os.makedirs("logs", exist_ok=True)
        
        stats_dict = {
            'success_count': stats.success_count,
            'failed_count': stats.failed_count,
            'total_targets': stats.total_targets,
            'success_rate': stats.success_rate,
            'start_time': stats.start_time,
            'end_time': stats.end_time,
            'duration_seconds': int(stats.duration.total_seconds()) if stats.duration else 0,
            'errors': stats.errors,
            'timestamp': datetime.now().isoformat()
        }
        
        try:
            with open(filename, 'w') as f:
                json.dump(stats_dict, f, indent=2)
            print(f"{Fore.GREEN}📊 Statistics saved to: {filename}")
        except Exception as e:
            print(f"{Fore.RED}Failed to save statistics: {e}")

    @staticmethod
    def format_file_size(size_bytes: int) -> str:
        """Format file size in human readable format"""
        if size_bytes == 0:
            return "0 B"
        
        size_names = ["B", "KB", "MB", "GB", "TB"]
        i = 0
        while size_bytes >= 1024.0 and i < len(size_names) - 1:
            size_bytes /= 1024.0
            i += 1
        
        return f"{size_bytes:.1f} {size_names[i]}"

    @staticmethod
    def validate_delay_input(delay_str: str) -> Tuple[bool, int, str]:
        """
        Validate delay input and return validation result
        Returns: (is_valid, seconds, error_message)
        """
        try:
            seconds = Utils.parse_time_input(delay_str)
            
            if seconds < 1:
                return False, 0, "Delay must be at least 1 second"
            
            if seconds > 86400:  # More than 24 hours
                return False, 0, "Delay cannot exceed 24 hours"
            
            return True, seconds, ""
            
        except Exception as e:
            return False, 0, f"Invalid time format: {str(e)}"

    @staticmethod
    def create_backup(file_path: str) -> bool:
        """Create a backup of a file with timestamp"""
        try:
            if not os.path.exists(file_path):
                return False
            
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_path = f"{file_path}.backup_{timestamp}"
            
            with open(file_path, 'r') as src, open(backup_path, 'w') as dst:
                dst.write(src.read())
            
            print(f"{Fore.GREEN}✅ Backup created: {backup_path}")
            return True
            
        except Exception as e:
            print(f"{Fore.RED}Failed to create backup: {e}")
            return False

    @staticmethod
    def print_colored_message(message: str, message_type: str = "info", prefix: str = ""):
        """Print a colored message with optional prefix"""
        color = Utils.COLORS.get(message_type, Fore.WHITE)
        timestamp = datetime.now().strftime('%H:%M:%S')
        
        if prefix:
            print(f"{Fore.CYAN}[{timestamp}] {color}{prefix}: {message}")
        else:
            print(f"{Fore.CYAN}[{timestamp}] {color}{message}")

    @staticmethod
    def confirm_action(prompt: str, default: bool = False) -> bool:
        """Ask for user confirmation"""
        default_text = "Y/n" if default else "y/N"
        response = input(f"{Fore.YELLOW}{prompt} [{default_text}]: ").strip().lower()
        
        if not response:
            return default
        
        return response in ['y', 'yes', 'true', '1']

Write or update the Python code for a Telegram forwarding bot that works with multiple users.
1. Each user runs in a separate loop (parallel / independent) using threading.Thread.
2. The bot should respect each user’s delay setting (e.g., "10s", "5s").
3. For each user, messages are forwarded to all active targets in their target list.
4. After completing one full cycle of forwards for a user, wait for that user’s delay before starting the next cycle.
5. Forwarding should work for both channels and groups, including topics (/1, /42, etc.).
6. If forwarding is successful, log SUCCESS; if it fails, log the error (Access Denied, Flood Wait, etc.).
7. Each user’s loop should continue until their expiry_date is reached.
8. The system should automatically start forwarding if auto_start_forwarding is set to true.
9. The implementation must use multithreading only (threading.Thread) to run multiple users in parallel.