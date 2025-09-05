import asyncio
import json
import os
import shutil
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import logging
from telethon import TelegramClient, events, Button
from telethon.errors import SessionPasswordNeededError, PhoneCodeInvalidError
import re
import signal
import sys

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TelegramBot:
    def __init__(self):
        self.config = self.load_config()
        self.bot_token = self.config['bot_token']
        self.database_folder = self.config['database_config']['folder']
        self.admin_data = self.load_admin_data()
        self.credentials_data = self.load_credentials_data()
        self.groups_data = self.load_groups_data()
        
        # User sessions for managing state
        self.user_sessions = {}
        self.pending_verifications = {}
        
        # Initialize bot
        self.bot = TelegramClient('bot', api_id=25910392, api_hash='9e32cad6393a8598cc3a693ddfc2d66e')
        
        # Add shutdown flag
        self.shutdown_flag = False
    
    def load_config(self):
        """Load configuration from config.json"""
        with open('config.json', 'r') as f:
            return json.load(f)
    
    def load_admin_data(self):
        """Load admin data from admin.json"""
        admin_file = os.path.join(self.database_folder, 'admin.json')
        if os.path.exists(admin_file):
            with open(admin_file, 'r') as f:
                return json.load(f)
        return {
            "primary_admin": 7543932618,
            "admin_limit": 2,
            "secondary_admins": []
        }
    
    def load_credentials_data(self):
        """Load credentials data from credentials.json"""
        credentials_file = os.path.join(self.database_folder, 'credentials.json')
        if os.path.exists(credentials_file):
            with open(credentials_file, 'r') as f:
                return json.load(f)
        return {}
    
    def load_groups_data(self):
        """Load groups data from groups.json"""
        groups_file = os.path.join(self.database_folder, 'groups.json')
        if os.path.exists(groups_file):
            with open(groups_file, 'r') as f:
                return json.load(f)
        return {}
    
    def save_admin_data(self):
        """Save admin data to admin.json"""
        os.makedirs(self.database_folder, exist_ok=True)
        admin_file = os.path.join(self.database_folder, 'admin.json')
        with open(admin_file, 'w') as f:
            json.dump(self.admin_data, f, indent=2)
    
    def save_credentials_data(self):
        """Save credentials data to credentials.json"""
        os.makedirs(self.database_folder, exist_ok=True)
        credentials_file = os.path.join(self.database_folder, 'credentials.json')
        with open(credentials_file, 'w') as f:
            json.dump(self.credentials_data, f, indent=2)
    
    def save_groups_data(self):
        """Save groups data to groups.json"""
        os.makedirs(self.database_folder, exist_ok=True)
        groups_file = os.path.join(self.database_folder, 'groups.json')
        with open(groups_file, 'w') as f:
            json.dump(self.groups_data, f, indent=2)
    
    def is_authorized(self, user_id):
        """Check if user is authorized (primary or secondary admin)"""
        return (user_id == self.admin_data['primary_admin'] or 
                user_id in self.admin_data['secondary_admins'])
    
    def is_primary_admin(self, user_id):
        """Check if user is primary admin"""
        return user_id == self.admin_data['primary_admin']
    
    async def cleanup(self):
        """Clean up resources before shutdown"""
        logger.info("Cleaning up resources...")
        
        # Clean up pending verifications
        for user_id, verification_data in self.pending_verifications.items():
            try:
                client = verification_data.get('client')
                if client and client.is_connected():
                    await client.disconnect()
            except Exception as e:
                logger.error(f"Error disconnecting client for user {user_id}: {e}")
        
        self.pending_verifications.clear()
        self.user_sessions.clear()
        
        # Disconnect main bot
        try:
            if self.bot.is_connected():
                await self.bot.disconnect()
                logger.info("Bot disconnected successfully")
        except Exception as e:
            logger.error(f"Error disconnecting bot: {e}")
    
    def setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown"""
        def signal_handler(signum, frame):
            logger.info(f"Received signal {signum}, initiating shutdown...")
            self.shutdown_flag = True
            
            # Create a new event loop if needed
            try:
                loop = asyncio.get_event_loop()
            except RuntimeError:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
            
            # Schedule cleanup
            if loop.is_running():
                asyncio.create_task(self.cleanup())
            else:
                loop.run_until_complete(self.cleanup())
            
            sys.exit(0)
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
    
    async def start_bot(self):
        """Start the bot and register handlers"""
        try:
            await self.bot.start(bot_token=self.bot_token)
            logger.info("Bot started successfully!")
            
            # Register handlers
            self.bot.add_event_handler(self.handle_start, events.NewMessage(pattern='/start'))
            self.bot.add_event_handler(self.handle_callback, events.CallbackQuery())
            self.bot.add_event_handler(self.handle_message, events.NewMessage())
            
            # Setup signal handlers
            self.setup_signal_handlers()
            
            logger.info("Bot is running... Press Ctrl+C to stop")
            
            # Use a custom loop to handle shutdown gracefully
            while not self.shutdown_flag:
                try:
                    await asyncio.sleep(1)
                except asyncio.CancelledError:
                    logger.info("Bot execution cancelled")
                    break
                except KeyboardInterrupt:
                    logger.info("Keyboard interrupt received")
                    break
            
        except Exception as e:
            logger.error(f"Error starting bot: {e}")
            raise
        finally:
            await self.cleanup()
    
    async def handle_start(self, event):
        """Handle /start command"""
        try:
            user_id = event.sender_id
            
            if self.is_authorized(user_id):
                await self.send_welcome_message(event)
            else:
                await event.respond("❌ **Unauthorized Access**\n\nYou are not authorized to use this bot.")
        except Exception as e:
            logger.error(f"Error in handle_start: {e}")
    
    async def send_welcome_message(self, event):
        """Send welcome message with main menu"""
        try:
            user_id = event.sender_id
            buttons = [
                [Button.inline("👥 Manage Users", b"manage_users")],
                [Button.inline("🔗 Manage URLs", b"manage_urls")]
            ]
            
            # Only show settings to primary admin
            if self.is_primary_admin(user_id):
                buttons.append([Button.inline("⚙️ Settings", b"settings")])
            
            await event.respond(
                "🤖 **Welcome to Admin Panel**\n\n"
                "Choose an option from the menu below:",
                buttons=buttons
            )
        except Exception as e:
            logger.error(f"Error in send_welcome_message: {e}")
    
    async def handle_callback(self, event):
        """Enhanced callback handler with user controls"""
        try:
            data = event.data.decode('utf-8')
            user_id = event.sender_id
            
            if not self.is_authorized(user_id):
                await event.answer("❌ Unauthorized", alert=True)
                return
            
            # Handle user control callbacks
            if (data.startswith("toggle_start_") or data.startswith("set_delay_") or 
                data.startswith("set_mode_") or data.startswith("set_expiry_")):
                await self.handle_user_control_callbacks(event)
                return
            
            # Handle forward mode selection
            if data.startswith("mode_"):
                await self.handle_mode_selection(event, data)
                return
            
            # Handle expiry selection
            if data.startswith("expiry_"):
                await self.handle_expiry_selection(event, data)
                return
            
            # Handle URL management callbacks
            if data.startswith("add_urls_") or data.startswith("delete_urls_"):
                await self.handle_url_callbacks(event)
                return
            
            # Main menu callbacks
            if data == "manage_users":
                await self.show_manage_users(event)
            elif data == "manage_urls":
                await self.show_manage_urls(event)
            elif data == "settings" and self.is_primary_admin(user_id):
                await self.show_settings(event)
            
            # User management callbacks
            elif data == "add_user":
                await self.show_add_user(event)
            elif data == "view_users":
                await self.show_view_users(event)
            elif data == "delete_user":
                await self.show_delete_user_list(event)
            elif data == "back_main":
                await self.send_welcome_message(event)
            elif data == "back_manage_users":
                await self.show_manage_users(event)
            elif data == "cancel":
                await self.cancel_operation(event)
            
            # Settings callbacks
            elif data == "add_secondary_admin":
                await self.show_add_secondary_admin(event)
            elif data == "delete_secondary_admin":
                await self.show_delete_secondary_admin_list(event)
            elif data == "update_admin_limit":
                await self.show_update_admin_limit(event)
            elif data == "back_settings":
                await self.show_settings(event)
            
            # Handle pagination and specific user/admin callbacks
            elif data.startswith("user_"):
                await self.show_user_details(event, data)
            elif data.startswith("delete_user_"):
                await self.confirm_delete_user(event, data)
            elif data.startswith("confirm_delete_user_"):
                await self.delete_user_confirmed(event, data)
            elif data.startswith("delete_admin_"):
                await self.confirm_delete_admin(event, data)
            elif data.startswith("confirm_delete_admin_"):
                await self.delete_admin_confirmed(event, data)
            elif data.startswith("page_"):
                await self.handle_pagination(event, data)
                
        except Exception as e:
            logger.error(f"Error in handle_callback: {e}")
            try:
                await event.answer("An error occurred", alert=True)
            except:
                pass
    
    async def handle_user_control_callbacks(self, event):
        """Handle user control callbacks (start/stop, delay, mode, expiry)"""
        try:
            data = event.data.decode('utf-8')
            user_id = event.sender_id
            
            if not self.is_authorized(user_id):
                await event.answer("❌ Unauthorized", alert=True)
                return
            
            # Parse callback data
            if data.startswith("toggle_start_"):
                target_user_id = data.split("_", 2)[2]
                await self.toggle_user_start(event, target_user_id)
            
            elif data.startswith("set_delay_"):
                target_user_id = data.split("_", 2)[2]
                await self.show_set_delay(event, target_user_id)
            
            elif data.startswith("set_mode_"):
                target_user_id = data.split("_", 2)[2]
                await self.show_set_forward_mode(event, target_user_id)
            
            elif data.startswith("set_expiry_"):
                target_user_id = data.split("_", 2)[2]
                await self.show_set_expiry(event, target_user_id)
                
        except Exception as e:
            logger.error(f"Error in handle_user_control_callbacks: {e}")
    
    async def toggle_user_start(self, event, user_id):
        """Toggle user start/stop status"""
        try:
            if user_id not in self.credentials_data:
                await event.answer("User not found", alert=True)
                return
            
            current_status = self.credentials_data[user_id].get('start', False)
            new_status = not current_status
            
            self.credentials_data[user_id]['start'] = new_status
            self.credentials_data[user_id]['last_updated'] = datetime.now().isoformat()
            self.save_credentials_data()
            
            status_text = "started" if new_status else "stopped"
            phone = self.credentials_data[user_id].get('phone', 'Unknown')
            
            await event.answer(f"✅ Forwarding {status_text} for {phone}", alert=True)
            
            # Refresh user details
            await self.show_user_details(event, f"user_{user_id}")
            
        except Exception as e:
            logger.error(f"Error in toggle_user_start: {e}")
    
    async def handle_mode_selection(self, event, callback_data):
        """Handle forward mode selection"""
        try:
            parts = callback_data.split("_")
            user_id = parts[1]
            mode = parts[2]
            
            if user_id not in self.credentials_data:
                await event.answer("User not found", alert=True)
                return
            
            self.credentials_data[user_id]['forward_mode'] = mode
            self.credentials_data[user_id]['last_updated'] = datetime.now().isoformat()
            self.save_credentials_data()
            
            mode_name = {
                '1': 'Resolve Original',
                '2': 'Silent',
                '3': 'As Copy'
            }.get(mode, 'Unknown')
            
            await event.answer(f"✅ Forward mode set to: {mode_name}", alert=True)
            await self.show_user_details(event, f"user_{user_id}")
            
        except Exception as e:
            logger.error(f"Error in handle_mode_selection: {e}")
    
    async def handle_expiry_selection(self, event, callback_data):
        """Handle expiry date selection"""
        try:
            parts = callback_data.split("_")
            user_id = parts[1]
            period = parts[2]
            
            if user_id not in self.credentials_data:
                await event.answer("User not found", alert=True)
                return
            
            if period == "custom":
                # Show custom date input form
                self.user_sessions[event.sender_id] = {
                    "action": "set_custom_expiry",
                    "target_user_id": user_id
                }
                
                phone = self.credentials_data[user_id].get('phone', 'Unknown')
                await event.edit(
                    f"📅 **Set Custom Expiry for {phone}**\n\n"
                    "Enter expiry date in format: YYYY-MM-DD-HH:MM:SS\n"
                    "Example: `2025-12-31-23:59:59`",
                    buttons=[[Button.inline("🔙 Back", f"set_expiry_{user_id}".encode())]]
                )
                return
            
            # Calculate expiry date
            now = datetime.now()
            if period == "unlimited":
                expiry = None
            elif period == "1m":
                expiry = (now + timedelta(days=30)).strftime("%Y-%m-%d-%H:%M:%S")
            elif period == "3m":
                expiry = (now + timedelta(days=90)).strftime("%Y-%m-%d-%H:%M:%S")
            elif period == "6m":
                expiry = (now + timedelta(days=180)).strftime("%Y-%m-%d-%H:%M:%S")
            elif period == "1y":
                expiry = (now + timedelta(days=365)).strftime("%Y-%m-%d-%H:%M:%S")
            else:
                expiry = (now + timedelta(days=30)).strftime("%Y-%m-%d-%H:%M:%S")
            
            self.credentials_data[user_id]['expiry_date'] = expiry
            self.credentials_data[user_id]['last_updated'] = now.isoformat()
            self.save_credentials_data()
            
            await event.answer(f"✅ Expiry date set to: {expiry}", alert=True)
            await self.show_user_details(event, f"user_{user_id}")
            
        except Exception as e:
            logger.error(f"Error in handle_expiry_selection: {e}")
    
    async def handle_url_callbacks(self, event):
        """Handle URL management specific callbacks"""
        try:
            data = event.data.decode('utf-8')
            
            if data.startswith("add_urls_"):
                user_id = data.split("_", 2)[2]
                await self.show_add_urls_form(event, user_id)
            elif data.startswith("delete_urls_"):
                user_id = data.split("_", 2)[2]
                await self.show_delete_urls_form(event, user_id)
        except Exception as e:
            logger.error(f"Error in handle_url_callbacks: {e}")
    
    async def show_add_urls_form(self, event, user_id):
        """Show form to add URLs for a user"""
        try:
            if user_id not in self.credentials_data:
                await event.answer("User not found", alert=True)
                return
            
            self.user_sessions[event.sender_id] = {"action": "add_urls", "user_id": user_id}
            
            phone = self.credentials_data[user_id].get('phone', 'Unknown')
            await event.edit(
                f"➕ **Add URLs for {phone}**\n\n"
                "Please enter Telegram URLs (one per line):\n"
                "Example:\n"
                "`https://t.me/channel1`\n"
                "`https://t.me/channel2`\n"
                "`https://t.me/+invitelink`",
                buttons=[[Button.inline("❌ Cancel", b"cancel")]]
            )
        except Exception as e:
            logger.error(f"Error in show_add_urls_form: {e}")
    
    async def show_delete_urls_form(self, event, user_id):
        """Show form to delete URLs for a user"""
        try:
            if user_id not in self.groups_data or not self.groups_data[user_id]:
                await event.edit(
                    "❌ **No URLs Found**\n\n"
                    "This user has no URLs to delete.",
                    buttons=[[Button.inline("🔙 Back", f"user_{user_id}".encode())]]
                )
                return
            
            self.user_sessions[event.sender_id] = {"action": "delete_urls", "user_id": user_id}
            
            phone = self.credentials_data[user_id].get('phone', 'Unknown')
            await event.edit(
                f"🗑️ **Delete URLs for {phone}**\n\n"
                "Please enter URL indices to delete (e.g., 1,2,3,4):\n\n"
                "Note: Refer to the URL list shown in user details for correct indices.",
                buttons=[[Button.inline("❌ Cancel", b"cancel")]]
            )
        except Exception as e:
            logger.error(f"Error in show_delete_urls_form: {e}")
    
    async def show_manage_users(self, event):
        """Show manage users menu"""
        try:
            buttons = [
                [Button.inline("➕ Add User", b"add_user")],
                [Button.inline("👁️ View Users", b"view_users")],
                [Button.inline("🗑️ Delete User", b"delete_user")],
                [Button.inline("🔙 Back", b"back_main")]
            ]
            
            await event.edit(
                "👥 **User Management**\n\n"
                "Choose an option:",
                buttons=buttons
            )
        except Exception as e:
            logger.error(f"Error in show_manage_users: {e}")
    
    async def show_add_user(self, event):
        """Show add user form"""
        try:
            user_id = event.sender_id
            self.user_sessions[user_id] = {"action": "add_user", "step": "credentials"}
            
            await event.edit(
                "➕ **Add New User**\n\n"
                "Please enter the user credentials in one of these formats:\n\n"
                "**Format 1 (Separate lines):**\n"
                "```\n25910392\n494845d6ac932abff6e830d28e5a3037\n+919098769260```\n\n"
                "**Format 2 (Pipe-separated):**\n"
                "```25910392 | 494845d6ac932abff6e830d28e5a3037 | +919098769260```",
                buttons=[[Button.inline("❌ Cancel", b"cancel")]]
            )
        except Exception as e:
            logger.error(f"Error in show_add_user: {e}")
    
    async def show_view_users(self, event, page=1):
        """Show list of users with pagination"""
        try:
            users = list(self.credentials_data.keys())
            per_page = 10
            total_pages = (len(users) - 1) // per_page + 1 if users else 1
            
            start_idx = (page - 1) * per_page
            end_idx = start_idx + per_page
            page_users = users[start_idx:end_idx]
            
            if not users:
                buttons = [[Button.inline("🔙 Back", b"back_manage_users")]]
                await event.edit(
                    "👥 **View Users**\n\n"
                    "No users found.",
                    buttons=buttons
                )
                return
            
            # Create user buttons
            user_buttons = []
            for user_id in page_users:
                phone = self.credentials_data[user_id].get('phone', 'Unknown')
                user_buttons.append([Button.inline(f"📱 {phone}", f"user_{user_id}".encode())])
            
            # Pagination buttons
            pagination = []
            if page > 1:
                pagination.append(Button.inline("⬅️ Prev", f"page_view_users_{page-1}".encode()))
            
            pagination.append(Button.inline(f"{page}/{total_pages}", b"current_page"))
            
            if page < total_pages:
                pagination.append(Button.inline("➡️ Next", f"page_view_users_{page+1}".encode()))
            
            user_buttons.append(pagination)
            user_buttons.append([Button.inline("🔙 Back", b"back_manage_users")])
            
            await event.edit(
                "👥 **View Users**\n\n"
                f"Showing page {page} of {total_pages}:",
                buttons=user_buttons
            )
        except Exception as e:
            logger.error(f"Error in show_view_users: {e}")
    
    async def show_user_details(self, event, callback_data):
        """Show detailed user information with control buttons"""
        try:
            user_id = callback_data.split("_", 1)[1]
            user_data = self.credentials_data.get(user_id)
            
            if not user_data:
                await event.answer("User not found", alert=True)
                return
            
            # Get user's groups/URLs
            user_groups = self.groups_data.get(user_id, [])
            
            # Build user details message
            url_text = "📱 **User Details**\n\n"
            url_text += f"**Phone:** {user_data.get('phone', 'Unknown')}\n"
            url_text += f"**API ID:** {user_data.get('api_id', 'Unknown')}\n"
            
            # Status based on start field
            start_status = user_data.get('start', False)
            url_text += f"**Status:** {'Active' if start_status else 'Inactive'}\n"
            url_text += f"**Delay:** {user_data.get('delay', '1m')}\n"
            
            # Forward mode description
            forward_mode = user_data.get('forward_mode', '1')
            mode_desc = {
                '1': 'Resolve Original',
                '2': 'Silent',
                '3': 'As Copy'
            }.get(forward_mode, 'Unknown')
            url_text += f"**Forward Mode:** {forward_mode} ({mode_desc})\n"
            url_text += f"**Expiry Date:** {user_data.get('expiry_date', 'Not set')}\n\n"
            
            if user_groups:
                url_text += "**URLs:**\n"
                for i, group in enumerate(user_groups, 1):
                    status = "🟢" if group.get('active', False) else "🔴"
                    url_text += f"{i}. {status} {group.get('url', 'Unknown URL')}\n"
            else:
                url_text += "**URLs:** No URLs configured\n"
            
            # Create control buttons
            buttons = []
            
            # Start/Stop forwarding button
            if start_status:
                buttons.append([Button.inline("🛑 Stop Forwarding", f"toggle_start_{user_id}".encode())])
            else:
                buttons.append([Button.inline("▶️ Start Forwarding", f"toggle_start_{user_id}".encode())])
            
            # Settings buttons (two per row)
            buttons.extend([
                [Button.inline("⏱️ Set Delay", f"set_delay_{user_id}".encode())],
                [Button.inline("🔄 Forward Mode", f"set_mode_{user_id}".encode())],
                [Button.inline("📅 Set Expiry", f"set_expiry_{user_id}".encode())],
                [Button.inline("➕ Add URLs", f"add_urls_{user_id}".encode())],
                [Button.inline("🗑️ Delete URLs", f"delete_urls_{user_id}".encode())],
                [Button.inline("🔙 Back", b"view_users")]])
            
            await event.edit(url_text, buttons=buttons)
        except Exception as e:
            logger.error(f"Error in show_user_details: {e}")
    
    async def show_set_delay(self, event, user_id):
        """Show delay setting form"""
        try:
            if user_id not in self.credentials_data:
                await event.answer("User not found", alert=True)
                return
            
            self.user_sessions[event.sender_id] = {
                "action": "set_delay", 
                "target_user_id": user_id
            }
            
            phone = self.credentials_data[user_id].get('phone', 'Unknown')
            current_delay = self.credentials_data[user_id].get('delay', '1m')
            
            await event.edit(
                f"⏱️ **Set Delay for {phone}**\n\n"
                f"**Current Delay:** {current_delay}\n\n"
                "Enter new delay (examples):\n"
                "• `30s` - 30 seconds\n"
                "• `2m` - 2 minutes\n"
                "• `1h` - 1 hour\n"
                "• `5m30s` - 5 minutes 30 seconds",
                buttons=[[Button.inline("❌ Cancel", f"user_{user_id}".encode())]]
            )
        except Exception as e:
            logger.error(f"Error in show_set_delay: {e}")
    
    async def show_set_forward_mode(self, event, user_id):
        """Show forward mode selection"""
        try:
            if user_id not in self.credentials_data:
                await event.answer("User not found", alert=True)
                return
            
            phone = self.credentials_data[user_id].get('phone', 'Unknown')
            current_mode = self.credentials_data[user_id].get('forward_mode', '1')
            
            buttons = [
                [Button.inline(f"1️⃣ Original {'✅' if current_mode == '1' else ''}", f"mode_{user_id}_1".encode()),
                Button.inline(f"2️⃣ Silent {'✅' if current_mode == '2' else ''}", f"mode_{user_id}_2".encode()),
                Button.inline(f"3️⃣ As Copy {'✅' if current_mode == '3' else ''}", f"mode_{user_id}_3".encode())],
                [Button.inline("🔙 Back", f"user_{user_id}".encode())]
            ]
            
            await event.edit(
                f"🔄 **Set Forward Mode for {phone}**\n\n"
                "**Current Mode:** " + {
                    '1': 'Resolve Original',
                    '2': 'Silent', 
                    '3': 'As Copy'
                }.get(current_mode, 'Unknown') + "\n\n"
                "**Mode Descriptions:**\n"
                "1️⃣ **Resolve Original** - Forward with original formatting\n"
                "2️⃣ **Silent** - Forward without notification\n"
                "3️⃣ **As Copy** - Forward as copied message",
                buttons=buttons
            )
        except Exception as e:
            logger.error(f"Error in show_set_forward_mode: {e}")
    
    async def show_set_expiry(self, event, user_id):
        """Show expiry date setting form"""
        try:
            if user_id not in self.credentials_data:
                await event.answer("User not found", alert=True)
                return
            
            phone = self.credentials_data[user_id].get('phone', 'Unknown')
            current_expiry = self.credentials_data[user_id].get('expiry_date', 'Not set')
            
            # Quick options buttons
            buttons = [
                [Button.inline("♾️ Unlimited", f"expiry_{user_id}_unlimited".encode())],
                [
                    Button.inline("1️⃣ 1 Month", f"expiry_{user_id}_1m".encode()),
                    Button.inline("3️⃣ 3 Months", f"expiry_{user_id}_3m".encode())
                ],
                [
                    Button.inline("6️⃣ 6 Months", f"expiry_{user_id}_6m".encode()),
                    Button.inline("1️⃣ 1 Year", f"expiry_{user_id}_1y".encode())
                ],
                [Button.inline("✏️ Custom Date", f"expiry_{user_id}_custom".encode()),
                Button.inline("🔙 Back", f"user_{user_id}".encode())]
            ]
            
            await event.edit(
                f"📅 **Set Expiry Date for {phone}**\n\n"
                f"**Current Expiry:** {current_expiry}\n\n"
                "Choose an option:",
                buttons=buttons
            )
        except Exception as e:
            logger.error(f"Error in show_set_expiry: {e}")
    
    async def show_delete_user_list(self, event, page=1):
        """Show list of users for deletion"""
        try:
            users = list(self.credentials_data.keys())
            per_page = 20
            total_pages = (len(users) - 1) // per_page + 1 if users else 1
            
            start_idx = (page - 1) * per_page
            end_idx = start_idx + per_page
            page_users = users[start_idx:end_idx]
            
            if not users:
                buttons = [[Button.inline("🔙 Back", b"back_manage_users")]]
                await event.edit(
                    "🗑️ **Delete User**\n\n"
                    "No users found.",
                    buttons=buttons
                )
                return
            
            user_buttons = []
            for user_id in page_users:
                phone = self.credentials_data[user_id].get('phone', 'Unknown')
                user_buttons.append([Button.inline(f"📱 {phone}", f"delete_user_{user_id}".encode())])
            
            # Pagination
            pagination = []
            if page > 1:
                pagination.append(Button.inline("⬅️ Prev", f"page_delete_users_{page-1}".encode()))
            
            pagination.append(Button.inline(f"{page}/{total_pages}", b"current_page"))
            
            if page < total_pages:
                pagination.append(Button.inline("➡️ Next", f"page_delete_users_{page+1}".encode()))
            
            user_buttons.append(pagination)
            user_buttons.append([Button.inline("🔙 Back", b"back_manage_users")])
            
            await event.edit(
                "🗑️ **Delete User**\n\n"
                f"Select a user to delete (Page {page}/{total_pages}):",
                buttons=user_buttons
            )
        except Exception as e:
            logger.error(f"Error in show_delete_user_list: {e}")
    
    async def confirm_delete_user(self, event, callback_data):
        """Show confirmation for user deletion"""
        try:
            user_id = callback_data.split("_", 2)[2]
            user_data = self.credentials_data.get(user_id)
            
            if not user_data:
                await event.answer("User not found", alert=True)
                return
            
            phone = user_data.get('phone', 'Unknown')
            buttons = [
                [Button.inline("✅ Yes", f"confirm_delete_user_{user_id}".encode()),
                Button.inline("❌ No", b"delete_user")]
            ]
            
            await event.edit(
                f"⚠️ **Confirm Deletion**\n\n"
                f"Are you sure you want to delete user {phone}?\n"
                f"This action cannot be undone.",
                buttons=buttons
            )
        except Exception as e:
            logger.error(f"Error in confirm_delete_user: {e}")
    
    async def delete_user_confirmed(self, event, callback_data):
        """Delete user after confirmation"""
        try:
            user_id = callback_data.split("_", 3)[3]
            
            if user_id in self.credentials_data:
                # Get session file path if it exists
                user_data = self.credentials_data[user_id]
                session_file = user_data.get('session_file')
                
                # Delete session file if it exists
                if session_file and os.path.exists(session_file):
                    try:
                        os.remove(session_file)
                        logger.info(f"Deleted session file: {session_file}")
                    except Exception as e:
                        logger.error(f"Error deleting session file {session_file}: {e}")
                
                # Also check for old temp session files
                temp_session = f"sessions/{user_data.get('phone')}.session"
                if os.path.exists(temp_session):
                    try:
                        os.remove(temp_session)
                        logger.info(f"Deleted temp session file: {temp_session}")
                    except Exception as e:
                        logger.error(f"Error deleting temp session file {temp_session}: {e}")
                
                del self.credentials_data[user_id]
                self.save_credentials_data()
                
                # Also remove from groups data
                if user_id in self.groups_data:
                    del self.groups_data[user_id]
                    self.save_groups_data()
                
                await event.edit(
                    "✅ **User Deleted**\n\n"
                    "User and associated session file have been successfully deleted.",
                    buttons=[[Button.inline("🔙 Back to Users", b"delete_user")]]
                )
            else:
                await event.answer("User not found", alert=True)
        except Exception as e:
            logger.error(f"Error in delete_user_confirmed: {e}")
    
    async def show_manage_urls(self, event, page=1):
        """Show manage URLs menu"""
        try:
            users = list(self.credentials_data.keys())
            per_page = 20
            total_pages = (len(users) - 1) // per_page + 1 if users else 1
            
            start_idx = (page - 1) * per_page
            end_idx = start_idx + per_page
            page_users = users[start_idx:end_idx]
            
            if not users:
                buttons = [[Button.inline("🔙 Back", b"back_main")]]
                await event.edit(
                    "🔗 **Manage URLs**\n\n"
                    "No users found.",
                    buttons=buttons
                )
                return
            
            user_buttons = []
            for user_id in page_users:
                phone = self.credentials_data[user_id].get('phone', 'Unknown')
                user_buttons.append([Button.inline(f"📱 {phone}", f"user_{user_id}".encode())])
            
            # Pagination
            pagination = []
            if page > 1:
                pagination.append(Button.inline("⬅️ Prev", f"page_manage_urls_{page-1}".encode()))
            
            pagination.append(Button.inline(f"{page}/{total_pages}", b"current_page"))
            
            if page < total_pages:
                pagination.append(Button.inline("➡️ Next", f"page_manage_urls_{page+1}".encode()))
            
            user_buttons.append(pagination)
            user_buttons.append([Button.inline("🔙 Back", b"back_main")])
            
            await event.edit(
                "🔗 **Manage URLs**\n\n"
                f"Select a user to manage URLs (Page {page}/{total_pages}):",
                buttons=user_buttons
            )
        except Exception as e:
            logger.error(f"Error in show_manage_urls: {e}")
    
    async def show_settings(self, event):
        """Show settings menu (primary admin only)"""
        try:
            if not self.is_primary_admin(event.sender_id):
                await event.answer("❌ Access denied", alert=True)
                return
            
            buttons = [
                [Button.inline("➕ Add Secondary Admin", b"add_secondary_admin")],
                [Button.inline("🗑️ Delete Secondary Admin", b"delete_secondary_admin")],
                [Button.inline("🔢 Update Admin Limit", b"update_admin_limit")],
                [Button.inline("🔙 Back", b"back_main")]
            ]
            
            secondary_count = len(self.admin_data['secondary_admins'])
            limit = self.admin_data['admin_limit']
            
            await event.edit(
                "⚙️ **Settings**\n\n"
                f"**Secondary Admins:** {secondary_count}/{limit}\n"
                f"**Admin Limit:** {limit}\n\n"
                "Choose an option:",
                buttons=buttons
            )
        except Exception as e:
            logger.error(f"Error in show_settings: {e}")
    
    async def show_add_secondary_admin(self, event):
        """Show add secondary admin form"""
        try:
            user_id = event.sender_id
            self.user_sessions[user_id] = {"action": "add_secondary_admin"}
            
            current_count = len(self.admin_data['secondary_admins'])
            limit = self.admin_data['admin_limit']
            
            if current_count >= limit:
                await event.edit(
                    f"❌ **Cannot Add Admin**\n\n"
                    f"Maximum secondary admin limit ({limit}) reached.\n"
                    f"Please increase the limit first.",
                    buttons=[[Button.inline("🔙 Back", b"back_settings")]]
                )
                return
            
            await event.edit(
                "➕ **Add Secondary Admin**\n\n"
                "Please enter the admin ID:",
                buttons=[[Button.inline("❌ Cancel", b"cancel")]]
            )
        except Exception as e:
            logger.error(f"Error in show_add_secondary_admin: {e}")
    
    async def show_delete_secondary_admin_list(self, event, page=1):
        """Show list of secondary admins for deletion"""
        try:
            admins = self.admin_data['secondary_admins']
            per_page = 10
            total_pages = (len(admins) - 1) // per_page + 1 if admins else 1
            
            if not admins:
                await event.edit(
                    "🗑️ **Delete Secondary Admin**\n\n"
                    "No secondary admins found.",
                    buttons=[[Button.inline("🔙 Back", b"back_settings")]]
                )
                return
            
            start_idx = (page - 1) * per_page
            end_idx = start_idx + per_page
            page_admins = admins[start_idx:end_idx]
            
            admin_buttons = []
            for admin_id in page_admins:
                admin_buttons.append([Button.inline(f"👤 {admin_id}", f"delete_admin_{admin_id}".encode())])
            
            # Pagination
            pagination = []
            if page > 1:
                pagination.append(Button.inline("⬅️ Prev", f"page_delete_admins_{page-1}".encode()))
            
            pagination.append(Button.inline(f"{page}/{total_pages}", b"current_page"))
            
            if page < total_pages:
                pagination.append(Button.inline("➡️ Next", f"page_delete_admins_{page+1}".encode()))
            
            admin_buttons.append(pagination)
            admin_buttons.append([Button.inline("🔙 Back", b"back_settings")])
            
            await event.edit(
                "🗑️ **Delete Secondary Admin**\n\n"
                f"Select an admin to delete (Page {page}/{total_pages}):",
                buttons=admin_buttons
            )
        except Exception as e:
            logger.error(f"Error in show_delete_secondary_admin_list: {e}")
    
    async def confirm_delete_admin(self, event, callback_data):
        """Show confirmation for admin deletion"""
        try:
            admin_id = int(callback_data.split("_", 2)[2])
            
            buttons = [
                [Button.inline("✅ Yes", f"confirm_delete_admin_{admin_id}".encode()),
                Button.inline("❌ No", b"delete_secondary_admin")]
            ]
            
            await event.edit(
                f"⚠️ **Confirm Deletion**\n\n"
                f"Are you sure you want to remove admin {admin_id}?\n"
                f"This action cannot be undone.",
                buttons=buttons
            )
        except Exception as e:
            logger.error(f"Error in confirm_delete_admin: {e}")
    
    async def delete_admin_confirmed(self, event, callback_data):
        """Delete admin after confirmation"""
        try:
            admin_id = int(callback_data.split("_", 3)[3])
            
            if admin_id in self.admin_data['secondary_admins']:
                self.admin_data['secondary_admins'].remove(admin_id)
                self.save_admin_data()
                
                await event.edit(
                    "✅ **Admin Deleted**\n\n"
                    "Secondary admin has been successfully removed.",
                    buttons=[[Button.inline("🔙 Back to Settings", b"back_settings")]]
                )
            else:
                await event.answer("Admin not found", alert=True)
        except Exception as e:
            logger.error(f"Error in delete_admin_confirmed: {e}")
    
    async def show_update_admin_limit(self, event):
        """Show update admin limit form"""
        try:
            user_id = event.sender_id
            self.user_sessions[user_id] = {"action": "update_admin_limit"}
            
            current_limit = self.admin_data['admin_limit']
            
            await event.edit(
                "🔢 **Update Admin Limit**\n\n"
                f"Current limit: {current_limit}\n\n"
                "Please enter the new secondary admin limit:",
                buttons=[[Button.inline("❌ Cancel", b"cancel")]]
            )
        except Exception as e:
            logger.error(f"Error in show_update_admin_limit: {e}")
    
    async def handle_pagination(self, event, callback_data):
        """Handle pagination callbacks"""
        try:
            parts = callback_data.split("_")
            page_type = "_".join(parts[1:-1])
            page = int(parts[-1])
            
            if page_type == "view_users":
                await self.show_view_users(event, page)
            elif page_type == "delete_users":
                await self.show_delete_user_list(event, page)
            elif page_type == "manage_urls":
                await self.show_manage_urls(event, page)
            elif page_type == "delete_admins":
                await self.show_delete_secondary_admin_list(event, page)
        except Exception as e:
            logger.error(f"Error in handle_pagination: {e}")
    
    async def cancel_operation(self, event):
        """Cancel current operation"""
        try:
            user_id = event.sender_id
            if user_id in self.user_sessions:
                del self.user_sessions[user_id]
            if user_id in self.pending_verifications:
                # Clean up any pending client connections
                verification_data = self.pending_verifications[user_id]
                client = verification_data.get('client')
                if client and client.is_connected():
                    await client.disconnect()
                del self.pending_verifications[user_id]
            
            await self.send_welcome_message(event)
        except Exception as e:
            logger.error(f"Error in cancel_operation: {e}")
    
    async def handle_message(self, event):
        """Enhanced message handler with user control inputs"""
        try:
            user_id = event.sender_id
            
            if not self.is_authorized(user_id):
                return
            
            if user_id not in self.user_sessions:
                return
            
            session = self.user_sessions[user_id]
            action = session.get('action')
            
            if action == "add_user" and session.get('step') == "credentials":
                await self.process_user_credentials(event, session)
            elif action == "add_user" and session.get('step') == "otp":
                await self.process_otp_verification(event, session)
            elif action == "add_secondary_admin":
                await self.process_add_secondary_admin(event)
            elif action == "update_admin_limit":
                await self.process_update_admin_limit(event)
            elif action == "delete_urls":
                await self.process_delete_urls(event, session)
            elif action == "add_urls":
                await self.process_add_urls(event, session)
            elif action == "set_delay":
                await self.process_set_delay(event, session)
            elif action == "set_custom_expiry":
                await self.process_set_custom_expiry(event, session)
        except Exception as e:
            logger.error(f"Error in handle_message: {e}")
    
    async def process_user_credentials(self, event, session):
        """Process user credentials input"""
        try:
            text = event.raw_text.strip()
            
            # Parse credentials - support both formats
            try:
                if '|' in text:
                    # Old format: api_id | api_hash | mobile_number
                    parts = [p.strip() for p in text.split('|')]
                    if len(parts) != 3:
                        raise ValueError("Invalid pipe-separated format")
                    api_id, api_hash, phone = parts
                else:
                    # New format: separate lines
                    lines = [line.strip() for line in text.split('\n') if line.strip()]
                    if len(lines) != 3:
                        raise ValueError("Please provide exactly 3 lines: api_id, api_hash, and phone number")
                    api_id, api_hash, phone = lines
                
                # Clean up values
                api_id = api_id.strip()
                api_hash = api_hash.strip()
                phone = phone.strip()
                
                # Validate
                if not api_id.isdigit():
                    raise ValueError("Invalid API ID - must be numeric")
                if len(api_hash) != 32:
                    raise ValueError("Invalid API hash - must be 32 characters long")
                if not re.match(r'^\+\d{10,15}$', phone):
                    raise ValueError("Invalid phone number - must start with + and contain 10-15 digits")
                
                # Create Telegram client and send code
                client = TelegramClient(f"temp_{api_id}", int(api_id), api_hash)
                await client.connect()
                
                result = await client.send_code_request(phone)
                
                # Store verification data
                self.pending_verifications[event.sender_id] = {
                    'client': client,
                    'api_id': api_id,
                    'api_hash': api_hash,
                    'phone': phone,
                    'phone_code_hash': result.phone_code_hash
                }
                
                session['step'] = 'otp'
                
                await event.respond(
                    "📱 **OTP Verification**\n\n"
                    f"OTP has been sent to {phone}\n"
                    "Please enter the OTP code:",
                    buttons=[
                        [Button.inline("❌ Cancel", b"cancel")],
                        [Button.inline("🔙 Back", b"add_user")]
                    ]
                )
                
            except Exception as e:
                await event.respond(
                    "❌ **Invalid Input**\n\n"
                    "Please enter credentials in one of these formats:\n\n"
                    "**Format 1 (Separate lines):**\n"
                    "```\n25910392\n494845d6ac932abff6e830d28e5a3037\n+919098769260```\n\n"
                    "**Format 2 (Pipe-separated):**\n"
                    "```25910392 | 494845d6ac932abff6e830d28e5a3037 | +919098769260```\n\n"
                    f"Error: {str(e)}",
                    buttons=[[Button.inline("🔙 Back", b"add_user")]]
                )
        except Exception as e:
            logger.error(f"Error in process_user_credentials: {e}")
    
    async def process_otp_verification(self, event, session):
        """Process OTP verification"""
        try:
            otp = event.raw_text.strip()
            
            if event.sender_id not in self.pending_verifications:
                await event.respond("❌ No pending verification found.")
                return
            
            verification_data = self.pending_verifications[event.sender_id]
            client = verification_data['client']
            
            try:
                # Verify OTP
                await client.sign_in(
                    verification_data['phone'],
                    otp,
                    phone_code_hash=verification_data['phone_code_hash']
                )
                
                # Check if user already exists
                api_id = verification_data['api_id']
                phone = verification_data['phone']
                user_exists = api_id in self.credentials_data
                
                # Create sessions directory if it doesn't exist
                sessions_dir = "sessions"
                os.makedirs(sessions_dir, exist_ok=True)
                
                # Clean phone number for filename (remove + and any special characters)
                clean_phone = phone.replace('+', '').replace('-', '').replace(' ', '')
                new_session_path = os.path.join(sessions_dir, f"{clean_phone}.session")
                temp_session_path = f"temp_{api_id}.session"
                
                # Disconnect client before moving session file
                await client.disconnect()
                
                # Move session file to organized location
                if os.path.exists(temp_session_path):
                    try:
                        # If target session already exists, remove it first
                        if os.path.exists(new_session_path):
                            os.remove(new_session_path)
                        
                        # Move the temporary session to the organized location
                        shutil.move(temp_session_path, new_session_path)
                        logger.info(f"Session moved from {temp_session_path} to {new_session_path}")
                    except Exception as e:
                        logger.error(f"Error moving session file: {e}")
                        # Continue anyway, the session will still work from temp location
                
                if user_exists:
                    # Update existing user data
                    self.credentials_data[api_id].update({
                        "last_updated": datetime.now().isoformat(),
                        "phone": phone,  # Update phone in case it changed
                        "session_file": new_session_path
                    })
                    
                    # Clean up
                    del self.pending_verifications[event.sender_id]
                    del self.user_sessions[event.sender_id]
                    
                    await event.respond(
                        "✅ **User Updated Successfully**\n\n"
                        f"Phone: {phone}\n"
                        f"API ID: {api_id}\n"
                        f"Session: {new_session_path}\n\n"
                        "⚠️ User credentials were already in database and have been updated.",
                        buttons=[[Button.inline("🔙 Back to Users", b"manage_users")]]
                    )
                else:
                    # Save new user credentials
                    user_data = {
                        "api_id": api_id,
                        "api_hash": verification_data['api_hash'],
                        "phone": phone,
                        "last_updated": datetime.now().isoformat(),
                        "delay": "1m",
                        "forward_mode": "1",
                        "mode_set": True,
                        "start": False,
                        "auto_start_forwarding": True,
                        "expiry_date": (datetime.now() + timedelta(days=30)).strftime("%Y-%m-%d-%H:%M:%S")
                    }
                    
                    self.credentials_data[api_id] = user_data
                    self.save_credentials_data()
                    
                    # Initialize empty groups list for user
                    if api_id not in self.groups_data:
                        self.groups_data[api_id] = []
                        self.save_groups_data()
                    
                    # Clean up
                    del self.pending_verifications[event.sender_id]
                    del self.user_sessions[event.sender_id]
                    
                    await event.respond(
                        "✅ **User Added Successfully**\n\n"
                        f"Phone: {phone}\n"
                        f"API ID: {api_id}\n"
                        f"Session: {new_session_path}",
                        buttons=[[Button.inline("🔙 Back to Users", b"manage_users")]]
                    )
                    
            except PhoneCodeInvalidError:
                await event.respond(
                    "❌ **Invalid OTP**\n\n"
                    "Please enter the correct OTP:",
                    buttons=[
                        [Button.inline("❌ Cancel", b"cancel"),
                        Button.inline("🔙 Back", b"add_user")]
                    ]
                )
                
            except SessionPasswordNeededError:
                await event.respond(
                    "❌ **2FA Enabled**\n\n"
                    "This account has 2-factor authentication enabled.\n"
                    "Please disable 2FA and try again.",
                    buttons=[[Button.inline("🔙 Back", b"add_user")]]
                )
                
            except Exception as e:
                # Clean up on error
                try:
                    await client.disconnect()
                except:
                    pass
                
                if event.sender_id in self.pending_verifications:
                    del self.pending_verifications[event.sender_id]
                
                await event.respond(
                    f"❌ **Verification Failed**\n\n"
                    f"Error: {str(e)}",
                    buttons=[[Button.inline("🔙 Back", b"add_user")]]
                )
        except Exception as e:
            logger.error(f"Error in process_otp_verification: {e}")
    
    async def process_add_secondary_admin(self, event):
        """Process adding secondary admin"""
        try:
            admin_id = int(event.raw_text.strip())
            
            # Check if already exists
            if admin_id in self.admin_data['secondary_admins']:
                await event.respond(
                    "❌ **Admin Already Exists**\n\n"
                    "This user is already a secondary admin.",
                    buttons=[[Button.inline("🔙 Back", b"back_settings")]]
                )
                return
            
            # Check if it's the primary admin
            if admin_id == self.admin_data['primary_admin']:
                await event.respond(
                    "❌ **Cannot Add Primary Admin**\n\n"
                    "Primary admin cannot be added as secondary admin.",
                    buttons=[[Button.inline("🔙 Back", b"back_settings")]]
                )
                return
            
            # Check limit
            if len(self.admin_data['secondary_admins']) >= self.admin_data['admin_limit']:
                await event.respond(
                    "❌ **Admin Limit Reached**\n\n"
                    "Maximum secondary admin limit reached.",
                    buttons=[[Button.inline("🔙 Back", b"back_settings")]]
                )
                return
            
            # Add admin
            self.admin_data['secondary_admins'].append(admin_id)
            self.save_admin_data()
            
            del self.user_sessions[event.sender_id]
            
            await event.respond(
                "✅ **Secondary Admin Added**\n\n"
                f"Admin ID {admin_id} has been added successfully.",
                buttons=[[Button.inline("🔙 Back to Settings", b"back_settings")]]
            )
            
        except ValueError:
            await event.respond(
                "❌ **Invalid Admin ID**\n\n"
                "Please enter a valid numeric admin ID:",
                buttons=[[Button.inline("❌ Cancel", b"cancel")]]
            )
        except Exception as e:
            logger.error(f"Error in process_add_secondary_admin: {e}")
            await event.respond(
                f"❌ **Error**\n\n"
                f"Failed to add admin: {str(e)}",
                buttons=[[Button.inline("🔙 Back", b"back_settings")]]
            )
    
    async def process_update_admin_limit(self, event):
        """Process updating admin limit"""
        try:
            new_limit = int(event.raw_text.strip())
            
            if new_limit < 0:
                raise ValueError("Limit cannot be negative")
            
            current_admins = len(self.admin_data['secondary_admins'])
            if new_limit < current_admins:
                await event.respond(
                    f"❌ **Invalid Limit**\n\n"
                    f"Cannot set limit to {new_limit}.\n"
                    f"You currently have {current_admins} secondary admins.\n"
                    f"Please remove some admins first or set a higher limit.",
                    buttons=[[Button.inline("🔙 Back", b"back_settings")]]
                )
                return
            
            old_limit = self.admin_data['admin_limit']
            self.admin_data['admin_limit'] = new_limit
            self.save_admin_data()
            
            del self.user_sessions[event.sender_id]
            
            await event.respond(
                "✅ **Admin Limit Updated**\n\n"
                f"Limit changed from {old_limit} to {new_limit}.",
                buttons=[[Button.inline("🔙 Back to Settings", b"back_settings")]]
            )
            
        except ValueError:
            await event.respond(
                "❌ **Invalid Input**\n\n"
                "Please enter a valid positive number:",
                buttons=[[Button.inline("❌ Cancel", b"cancel")]]
            )
        except Exception as e:
            logger.error(f"Error in process_update_admin_limit: {e}")
            await event.respond(
                f"❌ **Error**\n\n"
                f"Failed to update limit: {str(e)}",
                buttons=[[Button.inline("🔙 Back", b"back_settings")]]
            )
    
    async def process_add_urls(self, event, session):
        """Process adding URLs for a user"""
        try:
            user_id = session.get('user_id')
            urls_text = event.raw_text.strip()
            
            if not user_id or user_id not in self.credentials_data:
                await event.respond("❌ User not found.")
                return
            
            # Parse URLs (one per line or comma separated)
            urls = []
            for line in urls_text.replace(',', '\n').split('\n'):
                url = line.strip()
                if url and url.startswith('https://t.me/'):
                    urls.append({"url": url, "active": True})
            
            if not urls:
                await event.respond(
                    "❌ **No Valid URLs Found**\n\n"
                    "Please enter valid Telegram URLs starting with https://t.me/",
                    buttons=[[Button.inline("❌ Cancel", b"cancel")]]
                )
                return
            
            # Add URLs to user's group data
            if user_id not in self.groups_data:
                self.groups_data[user_id] = []
            
            self.groups_data[user_id].extend(urls)
            self.save_groups_data()
            
            del self.user_sessions[event.sender_id]
            
            phone = self.credentials_data[user_id].get('phone', 'Unknown')
            await event.respond(
                f"✅ **URLs Added Successfully**\n\n"
                f"Added {len(urls)} URLs for user {phone}.",
                buttons=[[Button.inline("🔙 Back", f"user_{user_id}".encode())]]
            )
        except Exception as e:
            logger.error(f"Error in process_add_urls: {e}")
    
    async def process_delete_urls(self, event, session):
        """Process deleting URLs for a user"""
        try:
            user_id = session.get('user_id')
            indices_text = event.raw_text.strip()
            
            if not user_id or user_id not in self.groups_data:
                await event.respond("❌ User or URLs not found.")
                return
            
            try:
                # Parse indices (e.g., "1,2,3,4,79,97")
                indices = []
                for idx_str in indices_text.split(','):
                    idx = int(idx_str.strip()) - 1  # Convert to 0-based index
                    if idx >= 0:
                        indices.append(idx)
                
                if not indices:
                    raise ValueError("No valid indices")
                
                # Remove URLs in reverse order to maintain indices
                user_urls = self.groups_data[user_id]
                removed_count = 0
                
                for idx in sorted(indices, reverse=True):
                    if 0 <= idx < len(user_urls):
                        user_urls.pop(idx)
                        removed_count += 1
                
                self.save_groups_data()
                del self.user_sessions[event.sender_id]
                
                phone = self.credentials_data[user_id].get('phone', 'Unknown')
                await event.respond(
                    f"✅ **URLs Deleted Successfully**\n\n"
                    f"Removed {removed_count} URLs for user {phone}.",
                    buttons=[[Button.inline("🔙 Back", f"user_{user_id}".encode())]]
                )
                
            except ValueError:
                await event.respond(
                    "❌ **Invalid Input**\n\n"
                    "Please enter valid URL indices (e.g., 1,2,3,4):",
                    buttons=[[Button.inline("❌ Cancel", b"cancel")]]
                )
        except Exception as e:
            logger.error(f"Error in process_delete_urls: {e}")
            await event.respond(
                f"❌ **Error**\n\n"
                f"Failed to delete URLs: {str(e)}",
                buttons=[[Button.inline("❌ Cancel", b"cancel")]]
            )
    
    async def process_set_delay(self, event, session):
        """Process delay setting input"""
        try:
            target_user_id = session.get('target_user_id')
            delay_text = event.raw_text.strip().lower()
            
            if target_user_id not in self.credentials_data:
                await event.respond("❌ User not found.")
                return
            
            # Validate delay format
            delay_pattern = r'^(\d+[hms])+$|^(\d+h)?(\d+m)?(\d+s)?$'
            if not re.match(delay_pattern, delay_text) or delay_text == "":
                await event.respond(
                    "❌ **Invalid Format**\n\n"
                    "Please use format like: 30s, 2m, 1h, 5m30s",
                    buttons=[[Button.inline("🔙 Back", f"user_{target_user_id}".encode())]]
                )
                return
            
            # Save delay
            self.credentials_data[target_user_id]['delay'] = delay_text
            self.credentials_data[target_user_id]['last_updated'] = datetime.now().isoformat()
            self.save_credentials_data()
            
            del self.user_sessions[event.sender_id]
            
            phone = self.credentials_data[target_user_id].get('phone', 'Unknown')
            await event.respond(
                f"✅ **Delay Updated**\n\n"
                f"Delay for {phone} set to: {delay_text}",
                buttons=[[Button.inline("🔙 Back to User", f"user_{target_user_id}".encode())]]
            )
            
        except Exception as e:
            logger.error(f"Error in process_set_delay: {e}")
    
    async def process_set_custom_expiry(self, event, session):
        """Process custom expiry date input"""
        try:
            target_user_id = session.get('target_user_id')
            expiry_text = event.raw_text.strip()
            
            if target_user_id not in self.credentials_data:
                await event.respond("❌ User not found.")
                return
            
            # Validate date format
            try:
                datetime.strptime(expiry_text, "%Y-%m-%d-%H:%M:%S")
            except ValueError:
                await event.respond(
                    "❌ **Invalid Date Format**\n\n"
                    "Please use format: YYYY-MM-DD-HH:MM:SS\n"
                    "Example: 2025-12-31-23:59:59",
                    buttons=[[Button.inline("🔙 Back", f"set_expiry_{target_user_id}".encode())]]
                )
                return
            
            # Save expiry date
            self.credentials_data[target_user_id]['expiry_date'] = expiry_text
            self.credentials_data[target_user_id]['last_updated'] = datetime.now().isoformat()
            self.save_credentials_data()
            
            del self.user_sessions[event.sender_id]
            
            phone = self.credentials_data[target_user_id].get('phone', 'Unknown')
            await event.respond(
                f"✅ **Expiry Date Set**\n\n"
                f"Expiry for {phone} set to: {expiry_text}",
                buttons=[[Button.inline("🔙 Back to User", f"user_{target_user_id}".encode())]]
            )
            
        except Exception as e:
            logger.error(f"Error in process_set_custom_expiry: {e}")


# Main execution
async def main():
    """Main function to run the bot"""
    try:
        # Create database directory
        os.makedirs('database', exist_ok=True)
        
        # Initialize and start bot
        bot = TelegramBot()
        
        print("🤖 Starting Telegram Bot...")
        print("📁 Database folder: database/")
        print("📁 Sessions folder: sessions/")
        print("🔑 Bot token configured")
        print("👑 Primary admin:", bot.admin_data['primary_admin'])
        print("👥 Secondary admins:", len(bot.admin_data['secondary_admins']))
        print("📊 Users in database:", len(bot.credentials_data))
        
        await bot.start_bot()
        
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)
    finally:
        logger.info("Bot shutdown complete")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Bot stopped by user")
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        sys.exit(1)