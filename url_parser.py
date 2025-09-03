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