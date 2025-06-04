def get_language_for_region(region_name: str) -> str:
    """
    Returns the primary local language code for a given Indian state/UT.
    We'll also use 'en' for states or UTs that do not have a well-supported code in Google News.
    """
    language_mapping = {
        "andhra-pradesh": "te",
        "arunachal-pradesh": "en",
        "assam": "as",
        "bihar": "hi",
        "chhattisgarh": "hi",
        "goa": "en",
        "gujarat": "gu",
        "haryana": "hi",
        "himachal-pradesh": "hi",
        "jharkhand": "hi",
        "karnataka": "kn",
        "kerala": "ml",
        "madhya-pradesh": "hi",
        "maharashtra": "mr",
        "manipur": "mni",
        "meghalaya": "en",
        "mizoram": "lus",
        "nagaland": "en",
        "odisha": "or",
        "punjab": "pa",
        "rajasthan": "hi",
        "sikkim": "ne",
        "tamil-nadu": "ta",
        "telangana": "te",
        "tripura": "bn",
        "uttar-pradesh": "hi",
        "uttarakhand": "hi",
        "west-bengal": "bn",
        "andaman-and-nicobar-islands": "en",
        "chandigarh": "hi",
        "dadra-and-nagar-haveli-and-daman-and-diu": "gu",
        "lakshadweep": "ml",
        "delhi": "hi",
        "puducherry": "ta",
        "jammu-and-kashmir": "ur",
        "ladakh": "hi",
    }
    return language_mapping.get(region_name, "en")

def get_all_languages_for_region(region_name: str) -> list:
    """
    Returns a list of all relevant language codes for a given Indian state/UT.
    Many regions have multiple languages that should be queried for better coverage.
    """
    multilingual_mapping = {
        # States
        "andhra-pradesh": ["te", "en"],
        "arunachal-pradesh": ["en", "hi"],
        "assam": ["as", "en", "bn", "hi"],
        "bihar": ["hi", "en", "ur"],
        "chhattisgarh": ["hi", "en"],
        "goa": ["en", "mr", "kn"],
        "gujarat": ["gu", "en", "hi"],
        "haryana": ["hi", "en", "pa"],
        "himachal-pradesh": ["hi", "en"],
        "jharkhand": ["hi", "en"],
        "karnataka": ["kn", "en", "te", "ta"],
        "kerala": ["ml", "en", "ta"],
        "madhya-pradesh": ["hi", "en"],
        "maharashtra": ["mr", "en", "hi"],
        "manipur": ["mni", "en", "hi"],
        "meghalaya": ["en", "hi"],
        "mizoram": ["lus", "en", "hi"],
        "nagaland": ["en", "hi"],
        "odisha": ["or", "en", "hi"],
        "punjab": ["pa", "en", "hi"],
        "rajasthan": ["hi", "en"],
        "sikkim": ["ne", "en", "hi"],
        "tamil-nadu": ["ta", "en"],
        "telangana": ["te", "en", "ur"],
        "tripura": ["bn", "en", "hi"],
        "uttar-pradesh": ["hi", "en", "ur"],
        "uttarakhand": ["hi", "en"],
        "west-bengal": ["bn", "en", "hi"],
        
        # Union Territories
        "andaman-and-nicobar-islands": ["en", "hi", "bn", "ta"],
        "chandigarh": ["hi", "en", "pa"],
        "dadra-and-nagar-haveli-and-daman-and-diu": ["gu", "en", "hi", "mr"],
        "lakshadweep": ["en", "ml"],
        "delhi": ["hi", "en", "ur", "pa"],
        "puducherry": ["ta", "en", "ml", "te"],
        "jammu-and-kashmir": ["en", "hi", "ur"],
        "ladakh": ["en", "hi", "ur"],
    }
    
    return multilingual_mapping.get(region_name, ["en"])

def get_language_name(language_code: str) -> str:
    """
    Maps language codes to human-readable language names.
    """
    language_names = {
        "en": "English",
        "hi": "Hindi",
        "ta": "Tamil",
        "te": "Telugu",
        "ml": "Malayalam",
        "kn": "Kannada",
        "bn": "Bengali",
        "gu": "Gujarati",
        "mr": "Marathi",
        "or": "Odia",
        "pa": "Punjabi",
        "as": "Assamese",
        "ur": "Urdu",
        "ne": "Nepali",
        "mni": "Meitei (Manipuri)",
        "lus": "Mizo (Lushai)"
    }
    return language_names.get(language_code, language_code)

def get_climate_impact_terms(language_code: str) -> list:
    """
    High-priority monsoon-impact keywords in Indian languages + English.
    This is the main function to use for monsoon-related content extraction.
    """
    terms = {
        "en": [
            "monsoon", "heavy rain", "cloudburst", "flood",
            "flash flood", "waterlogging", "landslide",
            "cyclone", "power outage", "pipeline burst",
            "sewage overflow", "snakebite", "cholera",
            "dengue", "malaria", "economic loss", "deaths",
            "rainfall", "storm", "weather alert", "weather warning",
            "IMD", "meteorological", "precipitation", "downpour",
            "rescue operations", "relief camps", "evacuation",
            "water level", "dam overflow", "bridge collapse"
        ],
        "hi": [
            "मॉनसून", "भारी बारिश", "बादल फटना", "बाढ़",
            "फ्लैश फ्लड", "जलभराव", "भूस्खलन",
            "चक्रवात", "बिजली कटौती", "पाइपलाइन फटना",
            "सीवर ओवरफ्लो", "सर्पदंश", "हैजा",
            "डेंगू", "मलेरिया", "आर्थिक नुकसान", "मृत्यु",
            "वर्षा", "तूफान", "मौसम चेतावनी", "बचाव अभियान",
            "राहत शिविर", "निकासी", "जल स्तर", "बांध टूटना"
        ],
        "ta": [
            "பருவமழை", "கனமழை", "மேக வெடிப்பு", "வெள்ளம்",
            "ஆக்சடி வெள்ளம்", "நீர்ப்படிவு", "மண் சரிவு",
            "சுழல்காற்று", "மின்சாரம் துண்டிப்பு", "குழாய் வெடிப்பு",
            "கழிவு நீர் முழக்கம்", "பாம்பு கடி", "காலிரா",
            "டெங்கு", "மலேரியா", "பொருளாதார இழப்பு", "உயிரிழப்பு",
            "மழைப்பொழிவு", "புயல்", "வானிலை எச்சரிக்கை",
            "மீட்பு நடவடிக்கை", "நிவாரண முகாம்", "வெளியேற்றம்"
        ],
        "te": [
            "మోన్సూన్", "భారీ వర్షం", "మేఘ విరేచనం", "వరదలు",
            "ఫ్లాష్ వరద", "నీటి నిల్వ", "భూస్ఖలనం",
            "చక్రవాతం", "విద్యుత్ కోత", "పైప్ పేలుడు",
            "కాలువ పొర్లడం", "పాము కాటు", "కాలరా",
            "డెంగ్యూ", "మలేరియా", "ఆర్థిక నష్టం", "మరణాలు",
            "వర్షపాతం", "తుఫాను", "వాతావరణ హెచ్చరిక",
            "రక్షణ చర్యలు", "ఉపశమన శిబిరాలు", "తరలింపు"
        ],
        "ml": [
            "മൺസൂൺ", "കനത്ത മഴ", "മേഘവിറക്കം", "വെള്ളപ്പൊക്കം",
            "ഹർത്താൽ വെള്ളപ്പൊക്കം", "ജലനിലവാരം", "ഭൂസ്ഖലനം",
            "ചുഴലിക്കാറ്റ്", "വൈദ്യുതി തകരാർ", "പൈപ്പ് പൊട്ടൽ",
            "മാലിന്യ വെള്ളപ്പൊക്കം", "പാമ്പുകടിയേറ്റൽ", "കോളറ",
            "ഡെങ്കി", "മലേറിയ", "സാമ്പത്തിക നഷ്ടം", "മരണങ്ങൾ",
            "മഴയുടെ അളവ്", "കൊടുങ്കാറ്റ്", "കാലാവസ്ഥാ മുന്നറിയിപ്പ്",
            "രക്ഷാ പ്രവർത്തനങ്ങൾ", "ദുരിതാശ്വാസ ക്യാമ്പുകൾ", "ഒഴിപ്പിക്കൽ"
        ],
        "bn": [
            "বর্ষা", "ভারী বৃষ্টি", "মেঘভাঙা বৃষ্টি", "বন্যা",
            "হঠাৎ বন্যা", "জলাবদ্ধতা", "ভূমিধস",
            "ঘূর্ণিঝড়", "বিদ্যুৎ বিভ্রাট", "পাইপলাইন ফাটল",
            "পয়ঃপ্রণালী উপচে পড়া", "সাপের কামড়", "কলেরা",
            "ডেঙ্গু", "ম্যালেরিয়া", "আর্থিক ক্ষতি", "মৃত্যু",
            "বৃষ্টিপাত", "ঝড়", "আবহাওয়া সতর্কতা",
            "উদ্ধার কার্যক্রম", "ত্রাণ শিবির", "সরিয়ে নেওয়া"
        ],
        "gu": [
            "ચોમાસું", "ભારે વરસાદ", "બાદલ ફાટવું", "પૂર",
            "ફ્લૅશ પૂર", "જળભરાવ", "ભૂસ્ખલન",
            "ચક્રવાત", "વિજળી કપાત", "પાઇપ લિકેજ",
            "ગટર ઉપરછાપો", "સાપડંખ", "હૈજા",
            "ડેંગ્યુ", "મલેરિયા", "આર્થિક નુકસાન", "મૃત્યુ",
            "વરસાદ", "તોફાન", "હવામાન ચેતવણી",
            "બચાવ કામગીરી", "રાહત કેમ્પ", "સ્થળાંતર"
        ],
        "mr": [
            "पावसाळा", "मुसळधार पाऊस", "ढगफुटी", "पूर",
            "अचानक पूर", "पाणी साचणे", "भूस्खलन",
            "चक्रीवादळ", "वीज कपात", "पाईप फुटणे",
            "मैला पाणी ओसंडणे", "साप चावणे", "हैजा",
            "डेंगी", "मलेरिया", "आर्थिक नुकसान", "मृत्यू",
            "पर्जन्यवृष्टी", "वादळ", "हवामान सावधगिरी",
            "बचाव कार्य", "मदत छावणी", "स्थलांतर"
        ],
        "kn": [
            "ಮಳೆಯ ಕಾಲ", "ಭಾರೀ ಮಳೆ", "ಮೇಘ ಸ್ಫೋಟ", "ಪ್ರವಾಹ",
            "ಆಕಸ್ಮಿಕ ಪ್ರವಾಹ", "ನೀರಿನ ತಡೆ", "ಭೂಕುಸಿತ",
            "ಚಂಡಮಾರುತ", "ವಿದ್ಯುತ್ ವ್ಯತ್ಯಯ", "ಪೈಪ್ ಸ್ಫೋಟ",
            "ಒಳಚರಂಡಿ ಕಲುಷ", "ಹಾವು ಕಚ್ಚು", "ಕಾಲರಾ",
            "ಡೆಂಗ್ಯು", "ಮಲೆರಿಯಾ", "ಆರ್ಥಿಕ ನಷ್ಟ", "ಮೃತ್ಯು",
            "ಮಳೆಯ ಪ್ರಮಾಣ", "ಚಂಡಮಾರುತ", "ಹವಾಮಾನ ಎಚ್ಚರಿಕೆ",
            "ರಕ್ಷಣಾ ಕಾರ್ಯಾಚರಣೆ", "ಪರಿಹಾರ ಶಿಬಿರ", "ಸ್ಥಳಾಂತರಿಸುವಿಕೆ"
        ],
        "pa": [
            "ਮਾਨਸੂਨ", "ਭਾਰੀ ਮੀਂਹ", "ਬੱਦਲ ਫਟਣਾ", "ਹੜ੍ਹ",
            "ਅਚਾਨਕ ਹੜ੍ਹ", "ਪਾਣੀ ਭਰਨਾ", "ਮਿੱਟੀ ਖਿਸਕਣਾ",
            "ਚੱਕਰਵਾਤ", "ਬਿਜਲੀ ਬੰਦ", "ਪਾਈਪ ਲੀਕ",
            "ਸੀਵਰੇਜ ਓਵਰਫਲੋ", "ਸਾਂਪ ਡੱਸਣਾ", "ਕਾਲਰਾ",
            "ਡੇਂਗੂ", "ਮਲੇਰੀਆ", "ਆਰਥਿਕ ਨੁਕਸਾਨ", "ਮੌਤਾਂ",
            "ਬਰਸਾਤ", "ਤੂਫਾਨ", "ਮੌਸਮ ਚੇਤਾਵਨੀ",
            "ਬਚਾਅ ਕਾਰਵਾਈ", "ਰਾਹਤ ਕੈਂਪ", "ਬੇਘਰ ਕਰਨਾ"
        ],
        "or": [
            "ବର୍ଷା", "ଭାରି ବର୍ଷା", "ମେଘ ଫୁଟ", "ବାଢ଼",
            "ଅପର୍ଯ୍ୟାପ୍ତ ବାଢ଼", "ଜଳ ଭରା", "ଭୂସ୍ଖଳନ",
            "ଚକ୍ରବାତ", "ବିଦ୍ୟୁତ ବିଚ୍ଛିନ୍ନ", "ପାଇପ ଫାଟିବା",
            "ନାଳା ଉଫାନ", "ସାପ ଡସିବା", "ହଏଜା",
            "ଡେଙ୍ଗୁ", "ମାଲେରିଆ", "ଆର୍ଥିକ କ୍ଷତି", "ମୃତ୍ୟୁ",
            "ବର୍ଷା ପରିମାଣ", "ଝଡ଼", "ପାଗ ଚେତାବନୀ",
            "ଉଦ୍ଧାର କାର୍ଯ୍ୟ", "ରାହତ କ୍ୟାମ୍ପ", "ସ୍ଥାନାନ୍ତର"
        ],
        "as": [
            "বৰষা", "ভাৰী বৰষুণ", "বদলী বিষ্ফোৰণ", "বানপানী",
            "হঠনীয়া বান", "পানী জমা", "ভূস্‌খলন",
            "ঘূৰ্ণী ঝড়", "বিদ্যুৎ বিচ্ছিন্ন", "পাইপ ফুট",
            "চেফাৰ ওপঙা", "সাপৰ কামোৰ", "হৈজা",
            "ডেঙ্গু", "মেলেৰিয়া", "আৰ্থিক ক্ষতি", "মৃত্যু",
            "বৰষুণৰ পৰিমাণ", "ধুমুহা", "বতৰৰ সতৰ্কবাণী",
            "উদ্ধাৰ কাৰ্য", "সাহায্য শিবিৰ", "স্থানান্তৰ"
        ],
        "ne": [
            "बर्षा", "भारी वर्षा", "बादल फुट्ने", "बाढी",
            "आकस्मिक बाढी", "पानी जम्ने", "पहिरो",
            "चक्रवात", "बिजुली कटौती", "पाइप फुट्ने",
            "ढल ओभरफ्लो", "सर्प टोकाइ", "हैजा",
            "डेंगु", "मलेरिया", "आर्थिक हानि", "मृत्यु",
            "वर्षाको मात्रा", "आँधी", "मौसम चेतावनी",
            "उद्धार कार्य", "राहत शिविर", "स्थानान्तरण"
        ],
        "mni": [
            "ꯅꯣꯡꯄꯣꯛ ꯆꯥꯔꯥ", "ꯑꯀꯅꯕ ꯅꯣꯡꯄꯣꯛ", "ꯅꯣꯡꯄꯣꯛ ꯁꯥꯡꯅ", "ꯏꯁꯤꯡ ꯆꯦꯟꯊꯣꯀꯄ",
            "ꯊꯨꯅ ꯑꯣꯏꯕ ꯏꯁꯤꯡ", "ꯏꯁꯤꯡ ꯊꯃꯁꯤꯡ", "ꯂꯩꯃꯥꯌ ꯈꯦꯟꯊꯣꯀꯄ",
            "ꯅꯨꯡꯁꯤꯠ", "ꯏꯀꯥꯏ ꯈꯥꯏꯗꯣꯀꯄ", "ꯄꯥꯏꯞ ꯀꯦꯟꯊꯣꯀꯄ",
            "ꯁꯤꯋꯔ ꯑꯣꯚꯔꯐ꯭ꯂꯣ", "ꯂꯤꯜ ꯀꯠꯄ", "ꯀꯣꯂꯦꯔꯥ",
            "ꯗꯦꯡꯒꯨ", "ꯃꯦꯂꯦꯔꯤꯌꯥ", "ꯁꯦꯜ ꯊꯥꯗꯣꯀꯄ", "ꯁꯤꯕ"
        ],
        "lus": [
            "ruahtui", "ruahtui nasa", "chhim thlawk", "tui kaih",
            "rang taka tui kaih", "tui ding", "tlang tla",
            "thli huai", "current thi", "tui kawng bo",
            "sewage kaih chhuak", "rul nei", "cholera",
            "dengue", "malaria", "sum laklohna", "thih"
        ]
    }
    return terms.get(language_code, terms["en"])

# Backward compatibility aliases
get_language_terms_monsoon = get_climate_impact_terms