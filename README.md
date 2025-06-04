# 🌧️ Monsoon News Extraction Pipeline

Automated pipeline for extracting and monitoring monsoon-related news across Indian states and union territories for climate impact analysis.

## 🚀 Features

- **Daily Automated Extraction**: Runs automatically at 10:30 AM IST via GitHub Actions
- **Multi-State Coverage**: Extracts news from all Indian states and union territories
- **Smart Deduplication**: Intelligent duplicate detection using multiple strategies
- **Quality Assessment**: Categorizes articles by extraction quality (high/medium/low)
- **Multiple Extraction Methods**: Playwright, Selenium, and fallback strategies
- **Language Detection**: Supports multiple Indian languages

## 📁 Project Structure

```
monsoon-news-extraction/
├── main.py                 # Main pipeline orchestrator
├── monsoon.py             # News collection script
├── extract_articles.py    # Article content extraction
├── article_scraper.py     # Web scraping utilities
├── utils.py              # Utility functions
├── requirements.txt      # Python dependencies
├── .github/
│   └── workflows/
│       └── daily-monsoon-extraction.yml  # GitHub Actions workflow
├── data/                 # Raw CSV data (states/union-territories)
├── JSON Output/          # Main extracted articles (high+medium quality)
└── JSON Output Spare/    # Complete dataset with statistics
```

## 🛠️ Installation

### Local Setup

1. **Clone the repository:**
```bash
git clone https://github.com/your-username/monsoon-news-extraction.git
cd monsoon-news-extraction
```

2. **Create virtual environment:**
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. **Install dependencies:**
```bash
pip install -r requirements.txt
```

4. **Install browser dependencies:**
```bash
# For Playwright
playwright install chromium

# For macOS (ChromeDriver)
brew install chromedriver
```

## 🚀 Usage

### Local Execution

**Run complete pipeline:**
```bash
python main.py
```

**Run with options:**
```bash
# Specific date
python main.py --date 2025-06-01

# Look back 3 days
python main.py --days-back 3

# Process only specific state
python main.py --state kerala

# Skip folder creation
python main.py --skip-folders

# Skip article extraction (CSV only)
python main.py --skip-extraction
```

### GitHub Actions (Automated)

The pipeline runs automatically daily at **10:30 AM IST** via GitHub Actions.

**Manual trigger:**
1. Go to your repository → Actions tab
2. Select "Daily Monsoon News Extraction"
3. Click "Run workflow"
4. Optionally specify date, days back, or specific state

## 📊 Output

### Main Output
- **`JSON Output/YYYY-MM-DD/articles_combined.json`**: High and medium quality articles
- **Articles include**: title, URL, full text, language, region, quality score

### Detailed Output  
- **`JSON Output Spare/YYYY-MM-DD/`**: Complete dataset with statistics
  - `articles_all.json`: All extracted articles
  - `articles_high_quality.json`: High quality only
  - `articles_medium_quality.json`: Medium quality only  
  - `articles_low_quality.json`: Low quality only
  - `extraction_stats.json`: Detailed statistics

### Raw Data
- **`data/states/[state]/Monsoon/YYYY/MM/DD/results.csv`**: Raw news links
- **`data/union-territories/[ut]/Monsoon/YYYY/MM/DD/results.csv`**: Raw UT links

## 🔧 Configuration

### GitHub Actions Setup

1. **Enable Actions**: Repository Settings → Actions → Allow all actions

2. **Set up secrets** (if needed for notifications):
   - `NOTIFICATION_WEBHOOK`: For Slack/Discord notifications
   - `EMAIL_PASSWORD`: For email alerts

3. **Adjust schedule**: Edit `.github/workflows/daily-monsoon-extraction.yml`
   ```yaml
   schedule:
     - cron: '0 5 * * *'  # 5:00 AM UTC = 10:30 AM IST
   ```

### Browser Configuration

The pipeline automatically handles browser setup with fallback strategies:
1. **GitHub Actions**: Uses Ubuntu with Chromium
2. **Local macOS**: WebDriverManager + Homebrew fallbacks  
3. **Local Windows/Linux**: WebDriverManager auto-download

## 📈 Monitoring

### GitHub Actions Monitoring
- **Workflow status**: Repository → Actions tab
- **Artifacts**: Download extraction results for 30 days
- **Auto-issues**: Creates GitHub issue on pipeline failure
- **Logs**: Detailed execution logs with timestamps

### Local Monitoring
- **Console output**: Real-time progress with emojis
- **Log files**: Detailed error logs (if configured)
- **Statistics**: Extraction quality and language breakdown

## 🛠️ Troubleshooting

### Common Issues

**ChromeDriver errors on macOS:**
```bash
# Remove quarantine
sudo xattr -d com.apple.quarantine /path/to/chromedriver
# Or install via Homebrew
brew install chromedriver
```

**GitHub Actions failures:**
- Check Actions tab for detailed logs
- Issues are auto-created for failures
- Artifacts contain full extraction data

**Low extraction rates:**
- Check internet connectivity
- Verify news websites are accessible
- Review extraction quality in statistics

## 🤝 Contributing

1. Fork the repository
2. Create feature branch: `git checkout -b feature/amazing-feature`
3. Commit changes: `git commit -m 'Add amazing feature'`
4. Push to branch: `git push origin feature/amazing-feature`
5. Open Pull Request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- **News Sources**: Various Indian news websites and RSS feeds
- **Libraries**: Selenium, Playwright, BeautifulSoup, Newspaper3k
- **Infrastructure**: GitHub Actions for automated execution

---

**🌧️ Stay updated with monsoon news across India! 🇮🇳**