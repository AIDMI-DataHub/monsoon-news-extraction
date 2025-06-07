"""
Smart Google News Handler - Complete Implementation
Save this as: smart_google_news_handler.py

Advanced rate limiting and anti-detection system for Google News API
"""

import time
import random
import hashlib
import logging
import threading
from datetime import datetime, timedelta
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Optional, Dict, List, Any
import requests
from urllib.parse import quote, urlencode
import json
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class RateLimitState:
    """Track rate limiting state for intelligent backoff"""
    consecutive_failures: int = 0
    last_failure_time: Optional[datetime] = None
    last_success_time: Optional[datetime] = None
    total_requests: int = 0
    successful_requests: int = 0
    current_delay: float = 1.0
    blocked_until: Optional[datetime] = None
    
class SmartGoogleNewsHandler:
    """
    Advanced Google News handler with sophisticated rate limiting,
    anti-detection measures, and intelligent retry mechanisms.
    """
    
    def __init__(self, base_delay=2.0, max_delay=120.0, jitter_range=0.5):
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.jitter_range = jitter_range
        
        # Rate limiting state per language/region combination
        self.rate_states: Dict[str, RateLimitState] = defaultdict(RateLimitState)
        
        # Request timing tracking for pattern analysis
        self.request_history = deque(maxlen=100)
        
        # User agent rotation
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:121.0) Gecko/20100101 Firefox/121.0',
        ]
        
        # Session management for connection reuse
        self.sessions: Dict[str, requests.Session] = {}
        
        # Circuit breaker pattern
        self.circuit_breaker_state = {
            'failures': 0,
            'last_failure': None,
            'state': 'closed',  # closed, open, half-open
            'failure_threshold': 5,
            'recovery_timeout': 300  # 5 minutes
        }
        
        # Lock for thread safety
        self._lock = threading.Lock()
        
        # Smart query optimization
        self.query_success_rates: Dict[str, float] = {}
        self.banned_query_patterns = set()
        
        logger.info("🧠 Smart Google News Handler initialized")
    
    def get_session(self, lang_code: str) -> requests.Session:
        """Get or create a session for a specific language/region"""
        session_key = f"{lang_code}"
        
        if session_key not in self.sessions:
            session = requests.Session()
            
            # Configure session with smart headers
            session.headers.update({
                'User-Agent': random.choice(self.user_agents),
                'Accept': 'application/rss+xml, application/xml, text/xml',
                'Accept-Language': f'{lang_code},en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'Cache-Control': 'no-cache',
                'Pragma': 'no-cache',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'none',
                'Upgrade-Insecure-Requests': '1'
            })
            
            # Configure timeouts and retries
            try:
                adapter = requests.adapters.HTTPAdapter(
                    max_retries=3,
                    pool_connections=10,
                    pool_maxsize=20
                )
                session.mount('https://', adapter)
                session.mount('http://', adapter)
            except Exception as e:
                logger.warning(f"Could not configure session adapter: {e}")
            
            self.sessions[session_key] = session
            logger.debug(f"📱 Created new session for {lang_code}")
        
        return self.sessions[session_key]
    
    def calculate_smart_delay(self, state_key: str, error_type: str = None) -> float:
        """Calculate intelligent delay based on current state and error patterns"""
        with self._lock:
            state = self.rate_states[state_key]
            
            # Base delay calculation
            if state.consecutive_failures == 0:
                delay = self.base_delay
            else:
                # Exponential backoff with different multipliers for different errors
                multiplier = 2.0
                if error_type:
                    if 'ssl' in error_type.lower() or 'eof' in error_type.lower():
                        multiplier = 3.0  # SSL errors need longer waits
                    elif 'timeout' in error_type.lower():
                        multiplier = 2.5
                    elif 'connection' in error_type.lower():
                        multiplier = 4.0  # Connection issues need much longer
                    elif 'retries' in error_type.lower():
                        multiplier = 5.0  # Rate limiting - longest wait
                
                delay = min(
                    self.base_delay * (multiplier ** state.consecutive_failures),
                    self.max_delay
                )
            
            # Add intelligent jitter based on time patterns
            jitter = random.uniform(-self.jitter_range, self.jitter_range)
            
            # Time-based adjustments
            current_hour = datetime.now().hour
            if 9 <= current_hour <= 17:  # Business hours - be more careful
                delay *= 1.5
            elif 20 <= current_hour <= 23:  # Evening peak - very careful
                delay *= 2.0
            
            # Add jitter
            delay = max(0.5, delay + (delay * jitter))
            
            # Pattern-based adjustments
            if len(self.request_history) >= 5:
                recent_failures = sum(1 for req in list(self.request_history)[-5:] if not req['success'])
                if recent_failures >= 3:
                    delay *= 2.0  # Recent pattern of failures
            
            state.current_delay = delay
            return delay
    
    def should_skip_request(self, state_key: str) -> tuple:
        """Determine if we should skip this request based on circuit breaker logic"""
        with self._lock:
            # Check circuit breaker
            if self.circuit_breaker_state['state'] == 'open':
                if self.circuit_breaker_state['last_failure']:
                    time_since_failure = (datetime.now() - self.circuit_breaker_state['last_failure']).total_seconds()
                    if time_since_failure < self.circuit_breaker_state['recovery_timeout']:
                        return True, f"Circuit breaker open for {self.circuit_breaker_state['recovery_timeout'] - time_since_failure:.0f}s"
                    else:
                        # Try to recover
                        self.circuit_breaker_state['state'] = 'half-open'
                        logger.info("🔄 Circuit breaker moving to half-open state")
            
            # Check if we're in a blocked state
            state = self.rate_states[state_key]
            if state.blocked_until and datetime.now() < state.blocked_until:
                remaining = (state.blocked_until - datetime.now()).total_seconds()
                return True, f"Rate limited for {remaining:.0f}s"
            
            return False, ""
    
    def optimize_query(self, query: str, lang_code: str) -> str:
        """Optimize query to avoid patterns that typically get rate limited"""
        # Remove query patterns that have low success rates
        query_hash = hashlib.md5(f"{query}_{lang_code}".encode()).hexdigest()[:8]
        
        if query_hash in self.banned_query_patterns:
            logger.warning(f"🚫 Skipping banned query pattern: {query[:50]}...")
            return None
        
        # Simplify overly complex queries that often fail
        if query.count('OR') > 6:  # Too many OR conditions
            # Split into simpler parts
            parts = query.split(' OR ')
            if len(parts) > 6:
                # Keep only the most successful terms
                simplified_query = ' OR '.join(parts[:4])  # Keep first 4 terms
                logger.info(f"🔧 Simplified complex query: {simplified_query}")
                return simplified_query
        
        # Escape special characters that might cause issues
        if any(char in query for char in ['(', ')', '"'] if query.count(char) > 4):
            # Too many special characters - simplify
            simplified = query.replace('(', '').replace(')', '').replace('"', '')
            parts = [part.strip() for part in simplified.split('OR') if part.strip()]
            if parts:
                simplified_query = ' OR '.join(parts[:3])  # Take first 3 clean parts
                logger.info(f"🔧 Cleaned special characters: {simplified_query}")
                return simplified_query
        
        return query
    
    def record_request_result(self, state_key: str, success: bool, error_type: str = None, response_time: float = 0):
        """Record the result of a request for learning and optimization"""
        with self._lock:
            state = self.rate_states[state_key]
            state.total_requests += 1
            
            # Record in history for pattern analysis
            self.request_history.append({
                'timestamp': datetime.now(),
                'state_key': state_key,
                'success': success,
                'error_type': error_type,
                'response_time': response_time
            })
            
            if success:
                state.successful_requests += 1
                state.consecutive_failures = 0
                state.last_success_time = datetime.now()
                state.current_delay = max(self.base_delay, state.current_delay * 0.8)  # Reduce delay on success
                state.blocked_until = None  # Clear any blocking
                
                # Circuit breaker recovery
                if self.circuit_breaker_state['state'] == 'half-open':
                    self.circuit_breaker_state['state'] = 'closed'
                    self.circuit_breaker_state['failures'] = 0
                    logger.info("✅ Circuit breaker closed - recovered!")
                    
            else:
                state.consecutive_failures += 1
                state.last_failure_time = datetime.now()
                
                # Circuit breaker logic
                self.circuit_breaker_state['failures'] += 1
                self.circuit_breaker_state['last_failure'] = datetime.now()
                
                if self.circuit_breaker_state['failures'] >= self.circuit_breaker_state['failure_threshold']:
                    self.circuit_breaker_state['state'] = 'open'
                    logger.warning(f"⚠️ Circuit breaker opened after {self.circuit_breaker_state['failures']} failures")
                
                # Set blocking period for severe errors
                if error_type and any(severe in error_type.lower() for severe in ['retries', 'ssl', 'connection']):
                    block_duration = min(300, 30 * state.consecutive_failures)  # Up to 5 minutes
                    state.blocked_until = datetime.now() + timedelta(seconds=block_duration)
                    logger.warning(f"🚫 Blocking requests for {block_duration}s due to {error_type}")
    
    def smart_search(self, gn_instance, query: str, when_parameter: str, lang_code: str, region: str, max_retries: int = 4) -> Optional[Dict]:
        """
        Perform intelligent search with advanced error handling and optimization
        """
        state_key = f"{lang_code}_{region}"
        
        # Check if we should skip this request
        should_skip, skip_reason = self.should_skip_request(state_key)
        if should_skip:
            logger.warning(f"⏭️ Skipping request: {skip_reason}")
            return None
        
        # Optimize the query
        optimized_query = self.optimize_query(query, lang_code)
        if not optimized_query:
            return None
        
        start_time = time.time()
        last_error = None
        
        for attempt in range(max_retries):
            try:
                # Calculate delay before request
                if attempt > 0:
                    error_type_str = str(last_error) if last_error else ""
                    delay = self.calculate_smart_delay(state_key, error_type_str)
                    logger.info(f"⏳ Attempt {attempt + 1}/{max_retries} - waiting {delay:.1f}s for {lang_code}...")
                    time.sleep(delay)
                
                # Rotate user agent periodically
                if random.random() < 0.3:  # 30% chance to rotate
                    session = self.get_session(lang_code)
                    session.headers['User-Agent'] = random.choice(self.user_agents)
                
                # Add request timing info for better pattern recognition
                current_time = datetime.now()
                logger.debug(f"🔍 Searching: {optimized_query[:50]}... | {lang_code} | Attempt {attempt + 1}")
                
                # Perform the actual search
                results = gn_instance.search(query=optimized_query, when=when_parameter)
                
                # Calculate response time
                response_time = time.time() - start_time
                
                # Record success
                self.record_request_result(state_key, True, response_time=response_time)
                
                logger.info(f"✅ Search successful in {response_time:.2f}s: {len(results.get('entries', []))} results")
                return results
                
            except Exception as e:
                last_error = e
                error_str = str(e).lower()
                error_type = self.classify_error(error_str)
                
                response_time = time.time() - start_time
                self.record_request_result(state_key, False, error_type, response_time)
                
                logger.warning(f"❌ Attempt {attempt + 1} failed ({error_type}): {str(e)[:100]}...")
                
                # Don't retry certain fatal errors
                if self.is_fatal_error(error_str):
                    logger.error(f"💀 Fatal error detected, not retrying: {error_type}")
                    break
                
                # Special handling for different error types
                if error_type == 'rate_limit':
                    # Exponentially increase delay for rate limits
                    if state_key in self.rate_states:
                        self.rate_states[state_key].current_delay *= 3.0
                elif error_type == 'ssl_error':
                    # SSL errors often require longer waits
                    time.sleep(random.uniform(10, 20))
                elif error_type == 'connection_error':
                    # Connection errors might be temporary
                    time.sleep(random.uniform(5, 15))
                
                # Last attempt handling
                if attempt == max_retries - 1:
                    logger.error(f"💥 All {max_retries} attempts failed for query: {optimized_query[:50]}...")
                    # Mark query pattern as potentially problematic
                    query_hash = hashlib.md5(f"{optimized_query}_{lang_code}".encode()).hexdigest()[:8]
                    if state_key in self.rate_states and self.rate_states[state_key].consecutive_failures >= 3:
                        self.banned_query_patterns.add(query_hash)
                        logger.warning(f"🚫 Marking query pattern as banned: {query_hash}")
        
        return None
    
    def classify_error(self, error_str: str) -> str:
        """Classify error type for appropriate handling"""
        error_str = error_str.lower()
        
        if any(keyword in error_str for keyword in ['max retries', 'retries exceeded']):
            return 'rate_limit'
        elif any(keyword in error_str for keyword in ['ssl', 'eof', 'certificate']):
            return 'ssl_error'
        elif any(keyword in error_str for keyword in ['connection', 'remotedisconnected', 'connectionerror']):
            return 'connection_error'
        elif any(keyword in error_str for keyword in ['timeout', 'timed out']):
            return 'timeout_error'
        elif any(keyword in error_str for keyword in ['forbidden', '403', 'blocked']):
            return 'access_denied'
        elif any(keyword in error_str for keyword in ['not found', '404']):
            return 'not_found'
        else:
            return 'unknown_error'
    
    def is_fatal_error(self, error_str: str) -> bool:
        """Determine if an error is fatal and shouldn't be retried"""
        fatal_indicators = [
            'forbidden', '403', 'access denied', 'authentication',
            'invalid api key', 'quota exceeded permanently'
        ]
        return any(indicator in error_str for indicator in fatal_indicators)
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get comprehensive statistics about request patterns"""
        with self._lock:
            total_requests = sum(state.total_requests for state in self.rate_states.values())
            total_successful = sum(state.successful_requests for state in self.rate_states.values())
            
            stats = {
                'total_requests': total_requests,
                'successful_requests': total_successful,
                'success_rate': (total_successful / total_requests * 100) if total_requests > 0 else 0,
                'circuit_breaker_state': self.circuit_breaker_state['state'],
                'banned_patterns': len(self.banned_query_patterns),
                'active_sessions': len(self.sessions),
                'per_region_stats': {}
            }
            
            for state_key, state in self.rate_states.items():
                if state.total_requests > 0:
                    stats['per_region_stats'][state_key] = {
                        'requests': state.total_requests,
                        'success_rate': (state.successful_requests / state.total_requests * 100),
                        'consecutive_failures': state.consecutive_failures,
                        'current_delay': state.current_delay,
                        'blocked': state.blocked_until is not None and datetime.now() < state.blocked_until
                    }
            
            return stats
    
    def reset_state(self, state_key: str = None):
        """Reset rate limiting state for debugging/recovery"""
        with self._lock:
            if state_key:
                if state_key in self.rate_states:
                    self.rate_states[state_key] = RateLimitState()
                    logger.info(f"🔄 Reset state for {state_key}")
            else:
                self.rate_states.clear()
                self.circuit_breaker_state = {
                    'failures': 0,
                    'last_failure': None,
                    'state': 'closed',
                    'failure_threshold': 5,
                    'recovery_timeout': 300
                }
                self.banned_query_patterns.clear()
                logger.info("🔄 Reset all states")
    
    def adaptive_delay(self):
        """Calculate adaptive delay based on global system state"""
        # Base delay between different types of requests
        base_delay = 1.0
        
        # Increase delay based on recent failure patterns
        if len(self.request_history) >= 10:
            recent_failures = sum(1 for req in list(self.request_history)[-10:] if not req['success'])
            failure_rate = recent_failures / 10
            
            if failure_rate > 0.5:  # More than 50% failures
                base_delay *= 3.0
            elif failure_rate > 0.3:  # More than 30% failures
                base_delay *= 2.0
        
        # Time-based adjustments
        current_hour = datetime.now().hour
        if 8 <= current_hour <= 10 or 17 <= current_hour <= 19:  # Peak hours
            base_delay *= 1.5
        
        return base_delay
    
    def cleanup_sessions(self):
        """Clean up old sessions to prevent resource leaks"""
        for session in self.sessions.values():
            try:
                session.close()
            except Exception as e:
                logger.debug(f"Error closing session: {e}")
        self.sessions.clear()
        logger.info("🧹 Cleaned up all sessions")

# Global instance for smart handling
smart_handler = SmartGoogleNewsHandler()