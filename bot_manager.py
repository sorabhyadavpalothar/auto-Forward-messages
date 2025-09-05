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
            config_path = Path('config.json')
            
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