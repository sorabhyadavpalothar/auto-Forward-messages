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