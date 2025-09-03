#!/usr/bin/env python3
"""
domy_groups.py - Add dummy/test groups to the database
This script adds sample Telegram groups for testing purposes
"""

import os
import sys
import logging
from colorama import Fore, Style, init

# Initialize colorama for Windows compatibility
init(autoreset=True)

# Import the ConfigManager (assuming it's in the same directory or adjust path as needed)
try:
    from config_manager import ConfigManager  # Adjust import path as needed
except ImportError:
    print(Fore.RED + "Error: Could not import ConfigManager. Make sure config_manager.py is in the same directory.")
    sys.exit(1)

class DomyGroupsManager:
    """Manages dummy/test groups for testing purposes"""
    
    def __init__(self):
        # Setup basic logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('logs/domy_groups.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
        # Initialize ConfigManager
        self.config_manager = ConfigManager(self.logger)
        self.config_manager.check_and_create_files()
        
        # Predefined dummy groups for testing
        self.dummy_groups = [
            # Public test channels/groups (these are real but safe for testing)
            "https://t.me/telegram",
            "https://t.me/BotNews",
            "https://t.me/TelegramTips",
            
            # Example format groups (fictional but properly formatted)
            "https://t.me/test_group_1",
            "https://t.me/test_group_2",
            "https://t.me/demo_channel",
            "https://t.me/sample_group",
            "https://t.me/example_chat",
            
            # Different URL formats for testing
            "https://t.me/joinchat/AAAAAAAAAAAAAAAAAAAAAA",  # Example invite link format
            "https://telegram.me/test_public_group",         # Alternative domain
            
            # Test groups with various naming conventions
            "https://t.me/TestGroup123",
            "https://t.me/test_channel_demo",
            "https://t.me/SampleCommunity",
            "https://t.me/DevTestGroup",
            "https://t.me/QualityAssurance"
        ]
    
    def display_menu(self):
        """Display the main menu"""
        print("\n" + "="*50)
        print(Fore.CYAN + Style.BRIGHT + "🤖 DOMY GROUPS MANAGER")
        print("="*50)
        print(Fore.WHITE + "1. " + Fore.GREEN + "Add all dummy groups")
        print(Fore.WHITE + "2. " + Fore.YELLOW + "Add specific dummy groups")
        print(Fore.WHITE + "3. " + Fore.BLUE + "View current groups")
        print(Fore.WHITE + "4. " + Fore.MAGENTA + "Add custom group")
        print(Fore.WHITE + "5. " + Fore.RED + "Clear all groups")
        print(Fore.WHITE + "6. " + Fore.CYAN + "Remove specific group")
        print(Fore.WHITE + "0. " + Fore.WHITE + "Exit")
        print("="*50)
    
    def add_all_dummy_groups(self):
        """Add all predefined dummy groups"""
        print(Fore.CYAN + "\n📥 Adding all dummy groups...")
        
        current_groups = self.config_manager.load_group_urls()
        new_groups = []
        duplicate_count = 0
        
        for group_url in self.dummy_groups:
            if group_url not in current_groups:
                new_groups.append(group_url)
            else:
                duplicate_count += 1
        
        if new_groups:
            # Add new groups to existing ones
            all_groups = current_groups + new_groups
            self.config_manager.save_group_urls(all_groups)
            
            print(Fore.GREEN + f"✅ Added {len(new_groups)} new dummy groups!")
            if duplicate_count > 0:
                print(Fore.YELLOW + f"⚠️  Skipped {duplicate_count} duplicate groups")
        else:
            print(Fore.YELLOW + "⚠️  All dummy groups already exist in the database")
        
        self.logger.info(f"Added {len(new_groups)} dummy groups, skipped {duplicate_count} duplicates")
    
    def add_specific_dummy_groups(self):
        """Allow user to select specific dummy groups to add"""
        print(Fore.CYAN + "\n📋 Available dummy groups:")
        
        for i, group_url in enumerate(self.dummy_groups, 1):
            print(f"{Fore.WHITE}{i:2d}. {Fore.BLUE}{group_url}")
        
        print(f"\n{Fore.YELLOW}Enter group numbers to add (comma-separated) or 'all' for all groups:")
        print(f"{Fore.YELLOW}Example: 1,3,5 or all")
        
        try:
            user_input = input(f"{Fore.WHITE}Your choice: ").strip()
            
            if user_input.lower() == 'all':
                self.add_all_dummy_groups()
                return
            
            # Parse selected numbers
            selected_numbers = [int(x.strip()) for x in user_input.split(',')]
            selected_groups = []
            
            for num in selected_numbers:
                if 1 <= num <= len(self.dummy_groups):
                    selected_groups.append(self.dummy_groups[num - 1])
                else:
                    print(Fore.RED + f"⚠️  Invalid number: {num}")
            
            if selected_groups:
                current_groups = self.config_manager.load_group_urls()
                new_groups = [group for group in selected_groups if group not in current_groups]
                
                if new_groups:
                    all_groups = current_groups + new_groups
                    self.config_manager.save_group_urls(all_groups)
                    print(Fore.GREEN + f"✅ Added {len(new_groups)} selected groups!")
                else:
                    print(Fore.YELLOW + "⚠️  All selected groups already exist")
            
        except ValueError:
            print(Fore.RED + "❌ Invalid input format. Please use numbers separated by commas.")
        except Exception as e:
            print(Fore.RED + f"❌ Error: {e}")
    
    def view_current_groups(self):
        """Display all current groups in the database"""
        print(Fore.CYAN + "\n📋 Current groups in database:")
        
        groups = self.config_manager.load_group_urls()
        
        if groups:
            for i, group_url in enumerate(groups, 1):
                # Highlight dummy groups
                if group_url in self.dummy_groups:
                    print(f"{Fore.GREEN}{i:2d}. {group_url} {Fore.YELLOW}(dummy)")
                else:
                    print(f"{Fore.WHITE}{i:2d}. {Fore.BLUE}{group_url}")
            
            print(f"\n{Fore.CYAN}📊 Total groups: {Fore.WHITE}{len(groups)}")
            dummy_count = sum(1 for group in groups if group in self.dummy_groups)
            print(f"{Fore.CYAN}🤖 Dummy groups: {Fore.YELLOW}{dummy_count}")
            print(f"{Fore.CYAN}👥 Custom groups: {Fore.GREEN}{len(groups) - dummy_count}")
        else:
            print(Fore.YELLOW + "📭 No groups found in database")
    
    def add_custom_group(self):
        """Add a custom group URL"""
        print(Fore.CYAN + "\n➕ Add custom group")
        
        group_url = input(f"{Fore.WHITE}Enter group URL: ").strip()
        
        if not group_url:
            print(Fore.RED + "❌ Empty URL provided")
            return
        
        # Basic URL validation
        if not (group_url.startswith('https://t.me/') or group_url.startswith('https://telegram.me/')):
            print(Fore.YELLOW + "⚠️  URL doesn't look like a Telegram group link")
            confirm = input(f"{Fore.WHITE}Add anyway? (y/N): ").strip().lower()
            if confirm != 'y':
                print(Fore.YELLOW + "❌ Cancelled")
                return
        
        if self.config_manager.add_group_url(group_url):
            print(Fore.GREEN + "✅ Custom group added successfully!")
        else:
            print(Fore.RED + "❌ Failed to add custom group (might already exist)")
    
    def clear_all_groups(self):
        """Clear all groups from database"""
        print(Fore.RED + "\n🗑️  WARNING: This will delete ALL groups from the database!")
        
        confirm = input(f"{Fore.WHITE}Are you sure? Type 'yes' to confirm: ").strip()
        
        if confirm.lower() == 'yes':
            try:
                self.config_manager.save_group_urls([])
                print(Fore.GREEN + "✅ All groups cleared from database!")
                self.logger.info("All groups cleared from database")
            except Exception as e:
                print(Fore.RED + f"❌ Error clearing groups: {e}")
        else:
            print(Fore.YELLOW + "❌ Operation cancelled")
    
    def remove_specific_group(self):
        """Remove a specific group from database"""
        print(Fore.CYAN + "\n🗑️  Remove specific group")
        
        groups = self.config_manager.load_group_urls()
        
        if not groups:
            print(Fore.YELLOW + "📭 No groups found in database")
            return
        
        print(Fore.CYAN + "\nSelect group to remove:")
        for i, group_url in enumerate(groups, 1):
            if group_url in self.dummy_groups:
                print(f"{Fore.WHITE}{i:2d}. {Fore.BLUE}{group_url} {Fore.YELLOW}(dummy)")
            else:
                print(f"{Fore.WHITE}{i:2d}. {Fore.BLUE}{group_url}")
        
        try:
            choice = int(input(f"{Fore.WHITE}Enter group number to remove: ").strip())
            
            if 1 <= choice <= len(groups):
                group_to_remove = groups[choice - 1]
                
                if self.config_manager.remove_group_url(group_to_remove):
                    print(Fore.GREEN + f"✅ Removed: {group_to_remove}")
                else:
                    print(Fore.RED + "❌ Failed to remove group")
            else:
                print(Fore.RED + "❌ Invalid choice")
                
        except ValueError:
            print(Fore.RED + "❌ Please enter a valid number")
        except Exception as e:
            print(Fore.RED + f"❌ Error: {e}")
    
    def run(self):
        """Main program loop"""
        print(Fore.GREEN + Style.BRIGHT + "Welcome to Domy Groups Manager! 🤖")
        
        while True:
            try:
                self.display_menu()
                choice = input(f"\n{Fore.WHITE}Enter your choice: ").strip()
                
                if choice == '0':
                    print(Fore.CYAN + "\n👋 Goodbye!")
                    break
                elif choice == '1':
                    self.add_all_dummy_groups()
                elif choice == '2':
                    self.add_specific_dummy_groups()
                elif choice == '3':
                    self.view_current_groups()
                elif choice == '4':
                    self.add_custom_group()
                elif choice == '5':
                    self.clear_all_groups()
                elif choice == '6':
                    self.remove_specific_group()
                else:
                    print(Fore.RED + "❌ Invalid choice. Please try again.")
                
                # Wait for user before continuing
                input(f"\n{Fore.GRAY}Press Enter to continue...")
                
            except KeyboardInterrupt:
                print(Fore.CYAN + "\n\n👋 Interrupted by user. Goodbye!")
                break
            except Exception as e:
                print(Fore.RED + f"\n❌ Unexpected error: {e}")
                self.logger.error(f"Unexpected error in main loop: {e}")

def main():
    """Entry point of the script"""
    try:
        manager = DomyGroupsManager()
        manager.run()
    except Exception as e:
        print(Fore.RED + f"Fatal error: {e}")
        logging.error(f"Fatal error in main: {e}")

if __name__ == "__main__":
    main()