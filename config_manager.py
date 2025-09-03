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