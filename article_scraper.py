import time
import math
import logging
import newspaper
import os
import threading
import traceback
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ProcessPoolExecutor, TimeoutError
from contextlib import contextmanager
import signal
import hashlib

# Import webdriver manager for automatic ChromeDriver management
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import TimeoutException, WebDriverException
from webdriver_manager.chrome import ChromeDriverManager

# Try to import additional libraries, but continue if not available
try:
    import trafilatura
    TRAFILATURA_AVAILABLE = True
except ImportError:
    TRAFILATURA_AVAILABLE = False
    logging.warning("trafilatura not available. Install with: pip install trafilatura")

try:
    from playwright.sync_api import sync_playwright
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False
    logging.warning("playwright not available. Install with: pip install playwright && playwright install")

try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False
    logging.warning("psutil module not found. Install with: pip install psutil")

# Configure logging to show timestamps, process ID, and log level
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [PID %(process)d] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# Global watchdog timer thread to force-kill if all else fails
class DriverWatchdog(threading.Thread):
    def __init__(self, driver_pid, timeout=60):
        super(DriverWatchdog, self).__init__(daemon=True)
        self.driver_pid = driver_pid
        self.timeout = timeout
        self.stopped = threading.Event()
        
    def run(self):
        start_time = time.time()
        while not self.stopped.wait(1):  # Check every second
            if time.time() - start_time > self.timeout:
                logging.warning(f"Watchdog timeout reached for Chrome PID {self.driver_pid}. Force killing.")
                self.kill_driver()
                break
    
    def kill_driver(self):
        if not PSUTIL_AVAILABLE:
            return
            
        try:
            kill_process_tree(self.driver_pid)
        except Exception as e:
            logging.error(f"Watchdog failed to kill process: {e}")
    
    def stop(self):
        self.stopped.set()

# Class for handling timeouts with a context manager
class TimeoutHandler:
    def __init__(self, seconds, error_message='Process timed out'):
        self.seconds = seconds
        self.error_message = error_message
        
    def handle_timeout(self, signum, frame):
        raise TimeoutError(self.error_message)
        
    def __enter__(self):
        signal.signal(signal.SIGALRM, self.handle_timeout)
        signal.alarm(self.seconds)
        
    def __exit__(self, type, value, traceback):
        signal.alarm(0)

def get_driver():
    """
    Initialize Chrome driver with improved error handling and automatic driver management
    """
    chrome_options = Options()
    chrome_options.add_argument("start-maximized")
    chrome_options.add_argument("disable-infobars")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--ignore-certificate-errors")
    chrome_options.add_argument("--ignore-ssl-errors")
    chrome_options.add_argument("--headless=new")  # Use new headless mode
    chrome_options.add_argument("--disable-gpu")  # Disable GPU for headless
    
    # Use eager page load strategy but don't block images - crucial for proper rendering
    chrome_options.page_load_strategy = 'eager'
    
    # Set preferences for better performance 
    # Keep images enabled but disable other resource-intensive features
    prefs = {
        "profile.default_content_setting_values.notifications": 2,  # Block notifications
        "browser.enable_spellchecking": False,  # Disable spellcheck
        "download_restrictions": 3  # Disable downloads
    }
    chrome_options.add_experimental_option('prefs', prefs)
    
    # Set language preferences to handle multilingual content better
    chrome_options.add_argument("--lang=en-US,en;q=0.9,hi;q=0.8,ta;q=0.7,te;q=0.6,bn;q=0.5,mr;q=0.4")
    
    # Add user agent to avoid bot detection
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36")
    
    # Multiple ChromeDriver strategies with graceful fallbacks
    driver_strategies = [
        ("WebDriverManager Auto", lambda: ChromeDriverManager().install()),
        ("WebDriverManager with cache clear", lambda: ChromeDriverManager().install()),
        ("System ChromeDriver", lambda: "/usr/local/bin/chromedriver"),
        ("Homebrew ChromeDriver", lambda: "/opt/homebrew/bin/chromedriver"),
        ("Manual ChromeDriver", lambda: "chromedriver")  # Assumes it's in PATH
    ]
    
    for strategy_name, driver_path_func in driver_strategies:
        try:
            logging.info(f"Attempting {strategy_name}...")
            
            if strategy_name == "WebDriverManager with cache clear":
                # Clear cache and try again for WebDriverManager
                import shutil
                cache_dir = os.path.expanduser("~/.wdm")
                if os.path.exists(cache_dir):
                    logging.info("Clearing WebDriverManager cache...")
                    shutil.rmtree(cache_dir)
            
            driver_path = driver_path_func()
            
            # Validate that the driver path exists and is executable
            if not os.path.exists(driver_path):
                logging.warning(f"{strategy_name}: Driver not found at {driver_path}")
                continue
                
            # Check if it's actually the chromedriver executable (not a text file)
            if os.path.isfile(driver_path):
                # Try to make it executable
                try:
                    os.chmod(driver_path, 0o755)
                except Exception as chmod_error:
                    logging.warning(f"Could not make {driver_path} executable: {chmod_error}")
                
                # Check file size - chromedriver should be several MB, not a few KB
                file_size = os.path.getsize(driver_path)
                if file_size < 1000000:  # Less than 1MB is suspicious
                    logging.warning(f"{strategy_name}: Driver file seems too small ({file_size} bytes), might be corrupted")
                    continue
            
            service = Service(driver_path)
            driver = webdriver.Chrome(service=service, options=chrome_options)
            
            # Set longer script timeout for complex pages
            driver.set_script_timeout(15)
            
            # Try to get the Chrome process ID
            pid = None
            if PSUTIL_AVAILABLE:
                pid = get_chrome_pid(driver)
                
            logging.info(f"Chrome driver initiated successfully using {strategy_name} in process {os.getpid()}, Chrome PID: {pid}")
            return driver, pid
            
        except Exception as e:
            logging.warning(f"{strategy_name} failed: {str(e)}")
            if "Exec format error" in str(e) or "Permission denied" in str(e):
                logging.warning(f"Driver executable issue with {strategy_name}. Trying next strategy...")
            continue
    
    # If all strategies failed, raise the last exception
    raise Exception("All ChromeDriver initialization strategies failed. Please install ChromeDriver manually or check your Chrome installation.")

def get_chrome_pid(driver):
    """Try to get the Chrome process ID from the driver"""
    if not PSUTIL_AVAILABLE:
        return None
        
    try:
        # Extract Chrome PID from the service process (this may not work in all environments)
        chrome_pid = None
        
        # Look for Chrome processes that could be children of this process
        for proc in psutil.process_iter(['pid', 'name']):
            if proc.info['name'] and 'chrome' in proc.info['name'].lower():
                try:
                    if proc.parent() and proc.parent().pid == os.getpid():
                        chrome_pid = proc.pid
                        break
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass
        
        return chrome_pid
    except Exception as e:
        logging.warning(f"Could not determine Chrome PID: {e}")
        return None

def kill_process_tree(pid):
    """Kill a process and all its children."""
    if not pid or not PSUTIL_AVAILABLE:
        return
        
    try:
        parent = psutil.Process(pid)
        children = []
        try:
            children = parent.children(recursive=True)
        except Exception:
            pass
            
        for child in children:
            try:
                child.kill()
                logging.info(f"Killed child process {child.pid}")
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
                
        try:
            parent.kill()
            logging.info(f"Killed parent process {pid}")
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
            
    except (psutil.NoSuchProcess, psutil.AccessDenied) as e:
        logging.warning(f"Could not kill process {pid}: {e}")
    except Exception as e:
        logging.error(f"Error killing process tree {pid}: {e}", exc_info=True)

def safely_quit_driver(driver, chrome_pid=None, watchdog=None):
    """Safely quit the driver, handling any exceptions."""
    if watchdog:
        watchdog.stop()
        
    if driver:
        try:
            # Try a fast, clean quit first
            driver.quit()
            logging.info(f"Chrome driver successfully quit in process {os.getpid()}")
        except Exception as e:
            logging.error(f"Error quitting Chrome driver in process {os.getpid()}: {str(e)}")
            # Force kill Chrome processes if normal quit fails
            if chrome_pid and PSUTIL_AVAILABLE:
                try:
                    kill_process_tree(chrome_pid)
                except Exception as e:
                    logging.error(f"Error force killing Chrome processes: {str(e)}")
            
            # Additional cleanup for any orphaned Chrome processes
            if PSUTIL_AVAILABLE:
                try:
                    for proc in psutil.process_iter(['pid', 'name']):
                        if proc.info['name'] and 'chrome' in proc.info['name'].lower():
                            if proc.parent() and proc.parent().pid == os.getpid():
                                logging.info(f"Force killing orphaned Chrome process {proc.pid}")
                                kill_process_tree(proc.pid)
                except Exception as e:
                    logging.error(f"Error in orphaned Chrome cleanup: {str(e)}")

def fallback_extract_with_requests(url):
    """
    Try to extract article content using requests and BeautifulSoup as fallback
    with multiple extraction strategies
    """
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
            'Accept-Language': 'en-US,en;q=0.9,hi;q=0.8,ta;q=0.7'
        }
        
        # Try with a session for cookies and redirects
        session = requests.Session()
        response = session.get(url, headers=headers, timeout=20)
        
        if response.status_code != 200:
            logging.warning(f"Failed to access {url}: Status code {response.status_code}")
            return None, None, None, None
            
        # Strategy 1: Try newspaper3k first
        try:
            article = newspaper.Article(url)
            article.download(input_html=response.text)
            article.parse()
            
            title = article.title
            text = article.text
            
            if title and text and len(text) > 200:
                language = detect_language(text)
                return url, title, text, language
        except Exception as e:
            logging.warning(f"newspaper3k extraction failed for {url}: {e}")
        
        # Strategy 2: Try trafilatura if available
        if TRAFILATURA_AVAILABLE:
            try:
                extracted_text = trafilatura.extract(response.text)
                if extracted_text and len(extracted_text) > 200:
                    soup = BeautifulSoup(response.text, 'html.parser')
                    title_tag = soup.find('title')
                    title = title_tag.text.strip() if title_tag else None
                    language = detect_language(extracted_text)
                    return url, title, extracted_text, language
            except Exception as e:
                logging.warning(f"trafilatura extraction failed for {url}: {e}")
        
        # Strategy 3: Try BeautifulSoup with multiple approaches
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Try to get title
        title = None
        title_tag = soup.find('h1') or soup.find('title')
        if title_tag:
            title = title_tag.text.strip()
        
        # Try to find article content
        article_text = ""
        
        # Approach 1: Find content by common article containers
        candidates = []
        for selector in ['article', '.article', '.content', '.story', 'main', '#content', '.post-content', '.news-content']:
            elements = soup.select(selector)
            candidates.extend(elements)
        
        # Approach 2: Look for the div with most paragraphs
        if not candidates:
            paragraph_counts = {}
            for div in soup.find_all('div'):
                paragraphs = div.find_all('p')
                if len(paragraphs) >= 3:  # Only consider divs with at least 3 paragraphs
                    paragraph_counts[div] = len(paragraphs)
            
            if paragraph_counts:
                sorted_divs = sorted(paragraph_counts.items(), key=lambda x: x[1], reverse=True)
                candidates.extend([div for div, count in sorted_divs[:3]])  # Add top 3 divs
        
        # Extract text from candidates
        if candidates:
            # Find candidate with most text content
            best_candidate = max(candidates, key=lambda x: len(x.get_text()))
            
            # Get paragraphs from best candidate
            paragraphs = best_candidate.find_all('p')
            if paragraphs:
                article_text = '\n'.join([p.get_text().strip() for p in paragraphs if len(p.get_text().strip()) > 20])
            else:
                # If no paragraphs, use whole content
                article_text = best_candidate.get_text(separator='\n', strip=True)
        
        # Approach 3: If above approaches failed, try getting all paragraphs
        if not article_text or len(article_text) < 200:
            paragraphs = soup.find_all('p')
            article_text = '\n'.join([p.get_text().strip() for p in paragraphs if len(p.get_text().strip()) > 30])
        
        # Return result if we have meaningful content
        if title and article_text and len(article_text) > 200:
            language = detect_language(article_text)
            return url, title, article_text, language
            
        # If we still have too little content, log failure
        logging.warning(f"Failed to extract meaningful content from {url}")
        return None, None, None, None
        
    except Exception as e:
        logging.error(f"Error in fallback extraction for {url}: {str(e)}")
        return None, None, None, None

def extract_with_playwright(url):
    """Extract article content using Playwright"""
    if not PLAYWRIGHT_AVAILABLE:
        return None, None, None, None
        
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            try:
                page = browser.new_page(
                    user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'
                )
                page.goto(url, timeout=30000, wait_until='domcontentloaded')
                
                # Wait a bit for content to be rendered
                page.wait_for_timeout(2000)
                
                # Get the final URL
                final_url = page.url
                
                # Get the title
                title = page.title()
                
                # Extract text using different strategies
                # Strategy 1: Get article content
                article_content = None
                for selector in ['article', '.article', '.content', '.story', 'main', '#content', '.post-content']:
                    try:
                        content = page.query_selector(selector)
                        if content:
                            article_content = content.inner_text()
                            break
                    except:
                        continue
                
                # Strategy 2: Get all paragraphs if article not found
                if not article_content or len(article_content) < 200:
                    paragraphs = page.query_selector_all('p')
                    paragraph_texts = []
                    for p in paragraphs:
                        try:
                            text = p.inner_text().strip()
                            if len(text) > 30:  # Only include substantial paragraphs
                                paragraph_texts.append(text)
                        except:
                            continue
                    
                    if paragraph_texts:
                        article_content = '\n'.join(paragraph_texts)
                
                # Clean up and return
                browser.close()
                
                if article_content and len(article_content) > 200:
                    language = detect_language(article_content)
                    return final_url, title, article_content, language
                return None, None, None, None
                
            except Exception as e:
                logging.error(f"Playwright extraction failed for {url}: {e}")
                if browser:
                    browser.close()
                return None, None, None, None
    except Exception as e:
        logging.error(f"Playwright initialization failed: {e}")
        return None, None, None, None

def detect_language(text):
    """Simple language detection based on character frequency"""
    if not text or len(text) < 50:
        return "en"  # Default to English for short or empty text
        
    # Count characters in different scripts
    devanagari = sum(1 for c in text if '\u0900' <= c <= '\u097F')  # Hindi, Sanskrit, etc.
    bengali = sum(1 for c in text if '\u0980' <= c <= '\u09FF')
    tamil = sum(1 for c in text if '\u0B80' <= c <= '\u0BFF')
    telugu = sum(1 for c in text if '\u0C00' <= c <= '\u0C7F')
    kannada = sum(1 for c in text if '\u0C80' <= c <= '\u0CFF')
    malayalam = sum(1 for c in text if '\u0D00' <= c <= '\u0D7F')
    gujarati = sum(1 for c in text if '\u0A80' <= c <= '\u0AFF')
    punjabi = sum(1 for c in text if '\u0A00' <= c <= '\u0A7F')
    
    # Count total text length and non-Latin characters
    total_len = len(text)
    non_latin = sum(1 for c in text if not (c.isascii() and c.isalpha()) and not c.isspace() and not c.isdigit() and c not in ".,;:!?'\"()[]{}")
    
    # Determine dominant script
    scripts = {
        "hi": devanagari,
        "bn": bengali,
        "ta": tamil,
        "te": telugu,
        "kn": kannada,
        "ml": malayalam,
        "gu": gujarati,
        "pa": punjabi
    }
    
    # If the text has significant non-Latin characters, identify the script
    if non_latin > total_len * 0.15:  # If more than 15% non-Latin
        dominant_script = max(scripts.items(), key=lambda x: x[1])
        if dominant_script[1] > total_len * 0.1:  # If the script represents over 10% of text
            return dominant_script[0]
    
    return "en"  # Default to English

def process_url(url):
    """
    Process a single URL using multiple extraction strategies.
    Returns a tuple (final_url, article_title, article_text, language)
    """
    pid = os.getpid()
    logging.info(f"Process {pid} started for URL: {url}")
    
    # Strategy 1: Try Playwright extraction
    if PLAYWRIGHT_AVAILABLE:
        try:
            logging.info(f"Attempting Playwright extraction for {url}")
            result = extract_with_playwright(url)
            if result and result[2] and len(result[2]) > 200:
                logging.info(f"Playwright extraction successful for {url}")
                return result
        except Exception as e:
            logging.warning(f"Playwright extraction failed for {url}: {e}")
    
    # Strategy 2: Try Selenium (with proper WebDriver management)
    driver = None
    chrome_pid = None
    watchdog = None
    
    try:
        logging.info(f"Attempting Selenium extraction for {url}")
        
        # Use a context manager for overall timeout
        with TimeoutHandler(60, f"Overall process for URL {url} timed out"):
            # Initialize driver with improved options
            driver, chrome_pid = get_driver()
            
            # Start watchdog timer in a separate thread
            if chrome_pid:
                watchdog = DriverWatchdog(chrome_pid, timeout=55)
                watchdog.start()
                
            # Set page load timeout
            driver.set_page_load_timeout(30)
            
            try:
                logging.info(f"Process {pid} requesting URL: {url}")
                driver.get(url)
            except TimeoutException as te:
                logging.warning(f"Timeout while loading page for {url}: {te}")
                try:
                    driver.execute_script("window.stop();")
                except Exception:
                    pass
            except Exception as e:
                logging.error(f"Error loading page for {url}: {str(e)}")
                safely_quit_driver(driver, chrome_pid, watchdog)
                driver = None
            
            if driver:
                # Wait for page to load as much as possible
                wait_for_ready_state(driver, 10)
                
                # Additional wait for dynamic content
                time.sleep(5)
                
                try:
                    # Get what we need from the browser
                    final_url = driver.current_url
                    html = driver.page_source
                    
                    # Close the driver immediately after getting the data
                    safely_quit_driver(driver, chrome_pid, watchdog)
                    driver = None
                    chrome_pid = None
                    watchdog = None
                    
                    # Process the article with newspaper library first
                    article = newspaper.Article(final_url)
                    article.download(input_html=html)
                    article.parse()
                    
                    title = article.title
                    text = article.text
                    
                    # If content is good, return it
                    if title and text and len(text) > 200:
                        language = detect_language(text)
                        logging.info(f"Selenium + newspaper extraction successful for {url}")
                        return final_url, title, text, language
                    
                    # If content is too short, try alternative extraction with BeautifulSoup
                    soup = BeautifulSoup(html, 'html.parser')
                    
                    # Try to find main content using common patterns
                    main_content = None
                    for selector in [
                        'article', '.article', '.content', '.story', 
                        'main', '#content', '.post-content', '.news-content'
                    ]:
                        elements = soup.select(selector)
                        if elements:
                            main_content = max(elements, key=lambda x: len(x.get_text()))
                            break
                    
                    if main_content:
                        # Get paragraphs from main content
                        paragraphs = main_content.find_all('p')
                        if paragraphs:
                            text = '\n'.join([p.get_text().strip() for p in paragraphs if len(p.get_text().strip()) > 20])
                        else:
                            # If no paragraphs, use whole content
                            text = main_content.get_text(separator='\n', strip=True)
                    
                    # If we have good content now, return it
                    if title and text and len(text) > 200:
                        language = detect_language(text)
                        logging.info(f"Selenium + BeautifulSoup extraction successful for {url}")
                        return final_url, title, text, language
                        
                except Exception as e:
                    logging.error(f"Error extracting page data for {url} in process {pid}: {str(e)}")
    
    except Exception as e:
        logging.error(f"Unexpected error in Selenium extraction for {url}: {str(e)}")
    finally:
        # Ensure driver is quit and watchdog is stopped
        if driver or chrome_pid or watchdog:
            safely_quit_driver(driver, chrome_pid, watchdog)
    
    # Strategy 3: Try direct fallback with requests/BeautifulSoup
    try:
        logging.info(f"Attempting fallback extraction for {url}")
        result = fallback_extract_with_requests(url)
        if result and result[2] and len(result[2]) > 200:
            logging.info(f"Fallback extraction successful for {url}")
            return result
    except Exception as e:
        logging.error(f"Fallback extraction failed for {url}: {e}")
    
    logging.warning(f"All extraction strategies failed for {url}")
    return None, None, None, None

def wait_for_ready_state(driver, timeout):
    """Wait for the page to reach a reasonable ready state with improved logic."""
    start_time = time.time()
    last_body_size = 0
    stable_count = 0
    
    while time.time() - start_time < timeout:
        try:
            ready_state = driver.execute_script("return document.readyState")
            
            # Check if body content has stabilized
            body_size = driver.execute_script("return document.body.innerHTML.length")
            
            if ready_state == "complete":
                logging.info(f"Page reached 'complete' state.")
                return True
                
            if ready_state == "interactive":
                # For interactive pages, check if content has stabilized
                if abs(body_size - last_body_size) < 100:  # Content size changes less than 100 chars
                    stable_count += 1
                    if stable_count >= 3:  # Content stable for 3 consecutive checks
                        logging.info(f"Page content stabilized in 'interactive' state.")
                        return True
                else:
                    stable_count = 0
                    
            last_body_size = body_size
            time.sleep(0.5)
            
        except Exception as e:
            logging.warning(f"Error checking ready state: {str(e)}")
            break
    
    logging.warning(f"Page did not reach complete state after {timeout} sec. Stopping load.")
    try:
        driver.execute_script("window.stop();")
    except Exception as e:
        logging.error(f"Error calling window.stop(): {str(e)}")
    
    return False

class ArticleScraper:
    def __init__(self, parallelism=2, process_timeout=60):
        """
        Initialize the ArticleScraper.
        
        Args:
            parallelism (int): Maximum number of parallel processes to use
            process_timeout (int): Maximum seconds to wait for a single process
        """
        self.parallelism = min(parallelism, 3)  # Reduced max parallelism for stability
        self.process_timeout = process_timeout
        # REMOVED: self.processed_urls - this was causing the global duplicate issue
    
    def getArticles(self, urls):
        if not urls:
            logging.info("No URLs provided. Exiting getArticles().")
            return []
            
        # Only filter duplicates within THIS specific batch, not globally
        unique_urls = []
        seen_in_batch = set()
        
        for url in urls:
            # Normalize URL for better duplicate detection within this batch
            url_normalized = url.strip().lower()
            if url_normalized not in seen_in_batch:
                seen_in_batch.add(url_normalized)
                unique_urls.append(url)
                
        if len(unique_urls) < len(urls):
            logging.info(f"Filtered out {len(urls) - len(unique_urls)} duplicate URLs within this batch")
            
        if not unique_urls:
            logging.info("No new URLs to process after filtering.")
            return []
            
        results = []
        total = len(unique_urls)
        logging.info(f"Processing {total} URLs using {self.parallelism} processes.")
        
        # Process URLs in smaller batches to prevent resource exhaustion
        batch_size = min(5, total)  # Smaller batch size for more stability
        
        for i in range(0, total, batch_size):
            batch_urls = unique_urls[i:i+batch_size]
            batch_results = self._process_batch(batch_urls, i, total)
            results.extend(batch_results)
            
            # More significant pause between batches
            if i + batch_size < total:
                logging.info(f"Completed batch {i//batch_size + 1}. Pausing to release resources.")
                time.sleep(5)
                
                # Force garbage collection
                try:
                    import gc
                    gc.collect()
                except ImportError:
                    pass
            
        logging.info(f"Completed processing all articles. Total successful: "
                    f"{len([r for r in results if r and r[0] is not None])}/{total}")
        return results

    def _process_batch(self, batch_urls, start_index, total):
        batch_results = []
        
        # Use sequential processing for stability if parallelism is 1
        if self.parallelism == 1:
            for i, url in enumerate(batch_urls):
                try:
                    result = process_url(url)
                    batch_results.append(result)
                    logging.info(f"Progress: {start_index + i + 1}/{total} URLs processed.")
                except Exception as e:
                    logging.error(f"Error for URL {url}: {str(e)}")
                    batch_results.append((None, None, None, None))
            return batch_results
            
        # Use ProcessPoolExecutor for parallel processing
        with ProcessPoolExecutor(max_workers=self.parallelism) as executor:
            future_to_url = {executor.submit(process_url, url): url for url in batch_urls}
            
            processed = start_index
            for future in future_to_url:
                url = future_to_url[future]
                try:
                    result = future.result(timeout=self.process_timeout)
                    batch_results.append(result)
                except TimeoutError:
                    logging.error(f"Timeout for URL: {url}")
                    batch_results.append((None, None, None, None))
                except Exception as e:
                    logging.error(f"Error for URL {url}: {str(e)}")
                    batch_results.append((None, None, None, None))
                
                processed += 1
                logging.info(f"Progress: {processed}/{total} URLs processed.")
                
        return batch_results

    def quit(self):
        # No persistent driver exists in this process-based approach
        logging.info("ArticleScraper.quit() called.")
        
        # Force garbage collection
        try:
            import gc
            gc.collect()
        except ImportError:
            pass

if __name__ == '__main__':
    # For testing purposes, run with a sample URL
    test_urls = [
        "https://www.hindustantimes.com/india-news/delhi-heatwave-to-continue-till-wednesday-possible-relief-from-thursday-101744080008859.html",
        "https://indianexpress.com/article/cities/delhi/delhi-news-live-updates-weather-heatwave-bjp-aap-9931292/"
    ]
    
    scraper = ArticleScraper(parallelism=1, process_timeout=60)
    articles = scraper.getArticles(test_urls)
    
    for i, article in enumerate(articles):
        if article and article[0]:
            print(f"\nArticle {i+1}:")
            print(f"URL: {article[0]}")
            print(f"Title: {article[1]}")
            print(f"Language: {article[3]}")
            print(f"Text preview: {article[2][:200]}...")
        else:
            print(f"\nArticle {i+1}: Extraction failed")