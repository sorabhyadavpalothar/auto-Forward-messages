#!/usr/bin/env python3
"""
Multi-User Bot Module
Handles multi-user coordination, configuration watching, and independent user loops
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
                        
                        if new_start:
                            # Start new user loop if not running
                            if api_id not in self.bot.user_tasks or self.bot.user_tasks[api_id].done():
                                if api_id in self.bot.user_clients:
                                    task = asyncio.create_task(self.bot.run_user_loop(api_id))
                                    self.bot.user_tasks[api_id] = task
                                    print(f"[{old_config.phone}] User loop started due to config change")
                                else:
                                    # Setup client first
                                    success = await self.bot.setup_user_client(api_id, old_config)
                                    if success:
                                        task = asyncio.create_task(self.bot.run_user_loop(api_id))
                                        self.bot.user_tasks[api_id] = task
                                        print(f"[{old_config.phone}] User loop started after client setup")
                        else:
                            # Stop user loop
                            if api_id in self.bot.user_tasks and not self.bot.user_tasks[api_id].done():
                                self.bot.user_tasks[api_id].cancel()
                                try:
                                    await self.bot.user_tasks[api_id]
                                except asyncio.CancelledError:
                                    pass
                                print(f"[{old_config.phone}] User loop stopped due to config change")
                            
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
                        print(f"[{old_config.phone}] Delay updated to {delay_display}")
                        changes_made = True
                    
                    # Update forward mode if changed (real-time)
                    new_forward_mode_str = cred_info.get('forward_mode', '1')
                    new_forward_mode = self.bot._parse_forward_mode(new_forward_mode_str)
                    if old_config.forward_mode != new_forward_mode:
                        old_config.forward_mode = new_forward_mode
                        print(f"[{old_config.phone}] Forward mode updated to {new_forward_mode.value}")
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
                            # Stop user loop if running
                            if api_id in self.bot.user_tasks and not self.bot.user_tasks[api_id].done():
                                self.bot.user_tasks[api_id].cancel()
                                print(f"[{old_config.phone}] User loop stopped - expired")
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
        """Handle changes to groups.json with enhanced real-time tracking and instant responses"""
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
                
            # Track changes across all users
            total_changes = 0
            
            # Update group lists for each user with detailed change tracking
            for api_id, group_list in new_groups.items():
                if api_id in self.bot.user_configs:
                    user_config = self.bot.user_configs[api_id]
                    
                    # Process active groups with metadata
                    new_active_groups = []
                    added_groups = []
                    removed_groups = []
                    activated_groups = []
                    deactivated_groups = []
                    
                    # Track old groups for comparison
                    old_groups = set(user_config.groups)
                    
                    for group_info in group_list:
                        if isinstance(group_info, dict):
                            url = group_info['url']
                            is_active = group_info.get('active', True)
                            
                            if is_active:
                                new_active_groups.append(url)
                                if url not in old_groups:
                                    added_groups.append(url)
                            else:
                                # Track groups that were deactivated
                                if url in old_groups:
                                    deactivated_groups.append(url)
                        else:
                            # Simple string URL
                            new_active_groups.append(group_info)
                            if group_info not in old_groups:
                                added_groups.append(group_info)
                    
                    # Find removed groups (completely removed from JSON)
                    new_groups_set = set(new_active_groups)
                    removed_groups = [url for url in old_groups if url not in new_groups_set]
                    
                    # Check if groups changed
                    if old_groups != new_groups_set:
                        old_count = len(user_config.groups)
                        user_config.groups = new_active_groups
                        new_count = len(new_active_groups)
                        total_changes += 1
                        
                        # Enhanced change logging with instant feedback
                        print(f"\n{'='*60}")
                        print(f"[{user_config.phone}] REAL-TIME GROUP UPDATE DETECTED")
                        print(f"{'='*60}")
                        
                        # Show added groups
                        if added_groups:
                            print(f"[{user_config.phone}] ✅ ADDED {len(added_groups)} groups:")
                            for url in added_groups:
                                group_name = self._extract_group_name_from_url(url)
                                print(f"  + {group_name}")
                        
                        # Show removed groups
                        if removed_groups:
                            print(f"[{user_config.phone}] ❌ REMOVED {len(removed_groups)} groups:")
                            for url in removed_groups:
                                group_name = self._extract_group_name_from_url(url)
                                print(f"  - {group_name}")
                        
                        # Show deactivated groups
                        if deactivated_groups:
                            print(f"[{user_config.phone}] ⏸️ DEACTIVATED {len(deactivated_groups)} groups:")
                            for url in deactivated_groups:
                                group_name = self._extract_group_name_from_url(url)
                                print(f"  ⏸️ {group_name}")
                        
                        print(f"[{user_config.phone}] Total active groups: {old_count} -> {new_count}")
                        
                        # Instant response and cache clearing - Use safe method calls
                        try:
                            # Check if the bot has the required methods before calling them
                            if hasattr(self.bot, 'handle_user_group_update'):
                                await self.bot.handle_user_group_update(api_id)
                            else:
                                # Fallback: Just clear forwarder cache if available
                                if (hasattr(self.bot, 'user_forwarders') and 
                                    api_id in self.bot.user_forwarders):
                                    forwarder = self.bot.user_forwarders[api_id]
                                    if hasattr(forwarder, 'clear_target_cache'):
                                        forwarder.clear_target_cache()
                            
                            if hasattr(self.bot, 'force_user_cycle_refresh'):
                                await self.bot.force_user_cycle_refresh(api_id)
                            
                            # Show immediate status
                            if (hasattr(self.bot, 'user_tasks') and 
                                api_id in self.bot.user_tasks and 
                                not self.bot.user_tasks[api_id].done()):
                                print(f"[{user_config.phone}] 🔄 ACTIVE LOOP - Changes applied immediately!")
                                print(f"[{user_config.phone}] Next cycle will use updated group list")
                            else:
                                print(f"[{user_config.phone}] 💤 INACTIVE LOOP - Changes saved for when loop starts")
                            
                        except Exception as e:
                            print(f"[{user_config.phone}] ⚠️ Error applying instant update: {e}")
                        
                        print(f"{'='*60}\n")
                        
            # Summary message
            if total_changes > 0:
                timestamp = datetime.now().strftime('%H:%M:%S')
                print(f"🎯 [{timestamp}] Real-time update completed for {total_changes} users")
                print("💡 All changes are now active - no restart required!")
            else:
                print("ℹ️ No active group changes detected")
            
        except json.JSONDecodeError as e:
            print(f"❌ JSON Error in groups.json: {e}")
            print("Please check your JSON formatting")
        except Exception as e:
            print(f"❌ Error handling groups change: {e}")
            import traceback
            traceback.print_exc()
            
    def _extract_group_name_from_url(self, url: str) -> str:
        """Extract a readable group name from URL for display"""
        try:
            if url.startswith('@'):
                return url
            elif 't.me/' in url:
                # Extract from t.me/groupname or t.me/c/123456/789
                parts = url.split('/')
                if 'c' in parts:
                    # Private group: t.me/c/1234567890 or t.me/c/1234567890/123
                    idx = parts.index('c')
                    if len(parts) > idx + 1:
                        group_id = parts[idx + 1]
                        if len(parts) > idx + 2:
                            return f"Private Group (ID: {group_id}) - Topic {parts[idx + 2]}"
                        return f"Private Group (ID: {group_id})"
                elif parts[-1].isdigit():
                    # Public group with topic: t.me/groupname/123
                    return f"@{parts[-2]} - Topic {parts[-1]}"
                else:
                    # Public group: t.me/groupname
                    return f"@{parts[-1]}"
            elif url.startswith('-'):
                return f"Chat ID: {url}"
            else:
                return url
        except:
            return url
            
    async def _handle_global_config_change(self):
        """Handle changes to global config"""
        try:
            if self.bot._load_global_config():
                print("Global configuration updated")
        except Exception as e:
            pass


class MultiUserTelegramBot(BotManager):
    """
    Enhanced multi-user bot class supporting independent user cycles
    Each user runs their own forwarding loop with individual delays
    """
    
    def __init__(self):
        """Initialize multi-user bot components"""
        super().__init__()
        
        # File watcher for real-time config changes
        self.config_watcher = None
        self.file_observer = None
        
        # Individual user tasks for independent cycles
        self.user_tasks: Dict[str, asyncio.Task] = {}
        self.user_stop_events: Dict[str, asyncio.Event] = {}

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

    async def handle_user_group_update(self, api_id: str):
        """Handle instant group updates for a running user with immediate response"""
        try:
            user_config = self.user_configs.get(api_id)
            if not user_config:
                return False
                
            # Check if user loop is running
            is_running = api_id in self.user_tasks and not self.user_tasks[api_id].done()
            
            if is_running:
                # Force immediate cache clear if forwarder supports it
                if api_id in self.user_forwarders:
                    forwarder = self.user_forwarders[api_id]
                    if hasattr(forwarder, 'clear_target_cache'):
                        forwarder.clear_target_cache()
                    if hasattr(forwarder, 'force_refresh_targets'):
                        await forwarder.force_refresh_targets()
                
                print(f"[{user_config.phone}] ✅ Real-time update applied - changes effective immediately!")
                
                # Trigger immediate stats update if user has active stats
                if api_id in self.user_stats:
                    stats = self.user_stats[api_id]
                    stats.total_targets = len(user_config.groups)
                
                return True
            else:
                print(f"[{user_config.phone}] Groups updated (will apply when loop starts)")
                return True
                
        except Exception as e:
            print(f"Error applying real-time update for user {api_id}: {e}")
            return False
            
    async def force_user_cycle_refresh(self, api_id: str):
        """Force immediate refresh of user's current cycle data"""
        try:
            user_config = self.user_configs.get(api_id)
            if not user_config:
                return
                
            # Update the next cycle to use fresh data
            if api_id in self.user_forwarders:
                forwarder = self.user_forwarders[api_id]
                
                # Clear any cached entity data
                if hasattr(forwarder, 'clear_entity_cache'):
                    forwarder.clear_entity_cache()
                if hasattr(forwarder, 'reset_target_cache'):
                    forwarder.reset_target_cache()
                    
                print(f"[{user_config.phone}] 🔄 Forced cycle refresh - next run will use updated groups")
                
        except Exception as e:
            print(f"Error forcing cycle refresh for user {api_id}: {e}")

    async def run_user_loop(self, api_id: str):
        """Independent infinite forwarding loop for each user with real-time URL updates"""
        user_config = self.user_configs.get(api_id)
        if not user_config:
            self.logger.error(f"[{api_id}] No user_config found for loop")
            return

        cycle_number = 0
        print(f"[{user_config.phone}] Starting independent forwarding loop...")

        forwarder = self.user_forwarders.get(api_id)
        client = self.user_clients.get(api_id)
        if forwarder is None or client is None:
            print(f"[{user_config.phone}] Missing forwarder/client, stopping loop")
            return

        stats = self.user_stats.get(api_id)
        if stats and not stats.start_time:
            stats.start_time = time.time()

        # Create stop event for this user
        stop_event = asyncio.Event()
        self.user_stop_events[api_id] = stop_event
        
        # Track last known group count for change detection
        last_group_count = len(user_config.groups)
        last_groups_hash = hash(tuple(sorted(user_config.groups)))

        try:
            while not self.shutdown_requested and not user_config.is_expired and not stop_event.is_set():
                cycle_number += 1
                
                # Check for real-time group changes before each cycle
                current_groups_hash = hash(tuple(sorted(user_config.groups)))
                current_group_count = len(user_config.groups)
                
                if current_groups_hash != last_groups_hash:
                    if current_group_count != last_group_count:
                        print(f"[{user_config.phone}] Groups updated: {last_group_count} -> {current_group_count}")
                    else:
                        print(f"[{user_config.phone}] Group URLs updated (same count: {current_group_count})")
                    
                    last_groups_hash = current_groups_hash
                    last_group_count = current_group_count
                    
                    # Refresh the forwarder's target cache if it exists
                    if hasattr(forwarder, 'clear_target_cache'):
                        forwarder.clear_target_cache()
                
                # Skip cycle if no groups configured
                if not user_config.groups:
                    print(f"[{user_config.phone}] No active groups configured, skipping cycle {cycle_number}")
                    await asyncio.sleep(30)  # Wait 30s before checking again
                    continue
                
                try:
                    # Show cycle start with current group count
                    print(f"[{user_config.phone}] Cycle {cycle_number} starting ({len(user_config.groups)} groups)...")
                    
                    result = await self.run_user_forwarding_cycle(api_id, cycle_number)
                    
                    # Enhanced cycle completion log with group info
                    if result.get("results") and result["results"].get("success"):
                        successful = result["results"].get("successful_forwards", 0)
                        failed = result["results"].get("failed_forwards", 0)
                        total = result["results"].get("total_targets", 0)
                        print(f"[{user_config.phone}] Cycle {cycle_number} completed: {successful}/{total} successful, {failed} failed")
                        
                        # Update stats
                        if successful > 0:
                            stats.success_count += successful
                        if failed > 0:
                            stats.failed_count += failed
                    else:
                        print(f"[{user_config.phone}] Cycle {cycle_number} completed with issues")
                        
                except Exception as e:
                    print(f"[{user_config.phone}] Cycle {cycle_number} error: {e}")
                    self.logger.error(f"[{user_config.phone}] Loop error: {e}", exc_info=True)

                # Individual delay for this user with real-time config updates
                if not stop_event.is_set() and not self.shutdown_requested:
                    # Get fresh delay value (may have been updated via config)
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
                        # Use asyncio.wait_for with the stop event to allow interruption
                        await asyncio.wait_for(stop_event.wait(), timeout=delay_seconds)
                        # If we reach here, stop was requested
                        break
                    except asyncio.TimeoutError:
                        # Normal timeout - continue to next cycle
                        continue
                    except asyncio.CancelledError:
                        break

        except asyncio.CancelledError:
            print(f"[{user_config.phone}] Forwarding loop cancelled")
        finally:
            print(f"[{user_config.phone}] Forwarding loop stopped")
            # Clean up
            if api_id in self.user_stop_events:
                del self.user_stop_events[api_id]

    async def start_user_loops(self, active_users: List[str]):
        """Start independent forwarding loops for all active users with enhanced monitoring"""
        total_groups = sum(len(self.user_configs[user_id].groups) for user_id in active_users)
        
        print(f"\nStarting {len(active_users)} independent user loops...")
        print(f"Total groups across all users: {total_groups}")
        print(f"Auto-start enabled: {self.global_config.auto_start_forwarding}")
        print("-" * 80)
        
        # Display detailed user information
        for api_id in active_users:
            user_config = self.user_configs[api_id]
            delay_display = f"{user_config.delay}s"
            if hasattr(Utils, 'format_time_display'):
                delay_display = Utils.format_time_display(user_config.delay)
            
            print(f"[{user_config.phone}] Groups: {len(user_config.groups)}, "
                  f"Delay: {delay_display}, Mode: {user_config.forward_mode.value}, "
                  f"Auto-start: {user_config.auto_start_forwarding}")
        
        print("-" * 80)
        
        # Start individual loops for each user
        for api_id in active_users:
            user_config = self.user_configs[api_id]
            
            # Initialize user stats
            if api_id not in self.user_stats:
                from utils import ForwardingStats
                self.user_stats[api_id] = ForwardingStats()
            
            if not self.user_stats[api_id].start_time:
                self.user_stats[api_id].start_time = time.time()
            
            # Create and start user task
            task = asyncio.create_task(self.run_user_loop(api_id))
            self.user_tasks[api_id] = task
            
            print(f"[{user_config.phone}] Loop started successfully")

        print(f"\nAll {len(active_users)} user loops are now running independently!")
        print("Each user will follow their own cycle timing without waiting for others.")
        print("Group changes in database/groups.json will be applied instantly.")
        print("=" * 80)

    async def monitor_user_loops(self):
        """Enhanced monitoring of user loops with periodic status updates"""
        last_status_update = time.time()
        status_interval = 300  # Show status every 5 minutes
        
        try:
            while not self.shutdown_requested and self.user_tasks:
                # Check for completed tasks
                completed_tasks = [
                    (api_id, task) for api_id, task in self.user_tasks.items() 
                    if task.done()
                ]
                
                for api_id, task in completed_tasks:
                    user_config = self.user_configs.get(api_id)
                    phone = user_config.phone if user_config else api_id
                    
                    try:
                        await task  # Get the result/exception
                        print(f"[{phone}] User loop completed normally")
                    except asyncio.CancelledError:
                        print(f"[{phone}] User loop cancelled")
                    except Exception as e:
                        print(f"[{phone}] User loop failed: {e}")
                        self.logger.error(f"[{phone}] User loop error: {e}", exc_info=True)
                        
                        # Attempt to restart the loop if it failed unexpectedly
                        if not self.shutdown_requested and user_config and user_config.start and not user_config.is_expired:
                            print(f"[{phone}] Attempting to restart failed loop...")
                            try:
                                new_task = asyncio.create_task(self.run_user_loop(api_id))
                                self.user_tasks[api_id] = new_task
                                print(f"[{phone}] Loop restarted successfully")
                                continue
                            except Exception as restart_error:
                                print(f"[{phone}] Failed to restart loop: {restart_error}")
                    
                    # Remove completed task
                    del self.user_tasks[api_id]
                
                # Periodic status update
                current_time = time.time()
                if current_time - last_status_update >= status_interval:
                    await self._display_status_summary()
                    last_status_update = current_time
                
                # Wait a bit before checking again
                await asyncio.sleep(5)
                
        except Exception as e:
            self.logger.error(f"Error monitoring user loops: {e}")
            
    async def _display_status_summary(self):
        """Display a comprehensive status summary of all running users"""
        if not self.user_tasks:
            return
            
        print("\n" + "=" * 80)
        print(f"STATUS UPDATE - {datetime.now().strftime('%H:%M:%S')}")
        print("=" * 80)
        
        total_success = 0
        total_failed = 0
        total_groups = 0
        
        for api_id, task in self.user_tasks.items():
            user_config = self.user_configs.get(api_id)
            if not user_config:
                continue
                
            stats = self.user_stats.get(api_id)
            status = "Running" if not task.done() else "Stopped"
            
            # Calculate runtime
            runtime_str = "N/A"
            if stats and stats.start_time:
                runtime = time.time() - stats.start_time
                if runtime > 3600:
                    hours = int(runtime // 3600)
                    minutes = int((runtime % 3600) // 60)
                    runtime_str = f"{hours}h {minutes}m"
                elif runtime > 60:
                    minutes = int(runtime // 60)
                    seconds = int(runtime % 60)
                    runtime_str = f"{minutes}m {seconds}s"
                else:
                    runtime_str = f"{int(runtime)}s"
            
            # Get stats
            success_count = stats.success_count if stats else 0
            failed_count = stats.failed_count if stats else 0
            group_count = len(user_config.groups)
            
            total_success += success_count
            total_failed += failed_count
            total_groups += group_count
            
            # Calculate success rate
            total_attempts = success_count + failed_count
            success_rate = (success_count / total_attempts * 100) if total_attempts > 0 else 0
            
            print(f"[{user_config.phone}] {status} | "
                  f"Groups: {group_count} | "
                  f"Success: {success_count} | "
                  f"Failed: {failed_count} | "
                  f"Rate: {success_rate:.1f}% | "
                  f"Runtime: {runtime_str}")
        
        # Overall summary
        overall_attempts = total_success + total_failed
        overall_rate = (total_success / overall_attempts * 100) if overall_attempts > 0 else 0
        
        print("-" * 80)
        print(f"OVERALL: {len(self.user_tasks)} users running | "
              f"Total groups: {total_groups} | "
              f"Success: {total_success} | "
              f"Failed: {total_failed} | "
              f"Overall rate: {overall_rate:.1f}%")
        print("=" * 80)
        print()

    async def stop_all_user_loops(self):
        """Stop all running user loops gracefully"""
        print("\nStopping all user loops...")
        
        # Set stop events for graceful shutdown
        for api_id, stop_event in self.user_stop_events.items():
            stop_event.set()
        
        # Cancel all running tasks
        for api_id, task in self.user_tasks.items():
            if not task.done():
                task.cancel()
        
        # Wait for all tasks to complete with timeout
        if self.user_tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*self.user_tasks.values(), return_exceptions=True),
                    timeout=10.0  # 10 second timeout
                )
            except asyncio.TimeoutError:
                print("Warning: Some user loops did not stop within timeout")
        
        self.user_tasks.clear()
        self.user_stop_events.clear()

    async def run(self):
        """Main execution method for multi-user bot with independent cycles"""
        try:
            print(f"{Fore.BLUE}Starting Multi-User Telegram Forwarder v3.0...")

            if hasattr(Utils, 'clear_screen'):
                Utils.clear_screen()
            if hasattr(Utils, 'display_banner'):
                Utils.display_banner()

            # Load user configurations
            if not await self.load_user_configurations():
                print("Failed to load user configurations. Check database files.")
                return

            # Setup clients for all users
            active_users = await self.setup_all_clients()
            
            if not active_users:
                print("No active users found or failed to setup clients.")
                return

            # Setup forwarding options
            self.setup_forwarding_options()

            # Store reference to the main event loop for the config watcher
            self.main_loop = asyncio.get_running_loop()

            # Setup configuration watcher for real-time changes
            self._setup_config_watcher()

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

            print(f"\n{Fore.GREEN}Starting independent forwarding loops for {len(active_users)} users...")
            
            # Start independent user loops
            await self.start_user_loops(active_users)
            
            # Monitor loops until shutdown
            monitor_task = asyncio.create_task(self.monitor_user_loops())
            
            try:
                # Wait for shutdown signal
                while not self.shutdown_requested:
                    await asyncio.sleep(1)
                    
                    # Check if all user loops have ended
                    if not self.user_tasks:
                        print("All user loops have ended.")
                        break
                        
            except KeyboardInterrupt:
                self.shutdown_requested = True
                print("\nShutdown requested...")
            
            # Cancel monitor task
            monitor_task.cancel()
            try:
                await monitor_task
            except asyncio.CancelledError:
                pass
                
        except KeyboardInterrupt:
            self.shutdown_requested = True
            print("\nShutdown requested by user...")
        except Exception as e:
            error_msg = f"Critical error in multi-user bot execution: {e}"
            self.logger.error(error_msg)
            print(f"{Fore.RED}Critical error: {e}")
        finally:
            # Stop all user loops
            await self.stop_all_user_loops()
            
            # Stop config watcher
            self._stop_config_watcher()
            
            # Cleanup
            await self.cleanup()
            print(f"{Fore.BLUE}Multi-User Bot shutdown complete!")