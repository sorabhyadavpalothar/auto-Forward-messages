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