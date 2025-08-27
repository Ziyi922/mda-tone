import os
import re
import glob
import html
import pandas as pd
from bs4 import BeautifulSoup
from collections import Counter
from sec_edgar_downloader import Downloader
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from tqdm import tqdm
import nltk
print("the current cd：", os.getcwd())
# 📦 Download required NLTK data (only needs to run once)
nltk.download('punkt')
nltk.download('stopwords')
# nltk.download('punkt_tab')
# 📁 Local paths

file_path = r"\\nstu-nas01.uwe.ac.uk\\users4$\\q33-liu\Windows\Desktop\AFM\\company.csv"
positive_path = r"\\nstu-nas01.uwe.ac.uk\\users4$\\q33-liu\Windows\Desktop\AFM\\lm_positive.txt"
negative_path = r"\\nstu-nas01.uwe.ac.uk\\users4$\\q33-liu\Windows\Desktop\AFM\\lm_negative.txt"
save_dir = r"\\nstu-nas01.uwe.ac.uk\\users4$\\q33-liu\Windows\Desktop\AFM\\mda_output"

os.makedirs(save_dir, exist_ok=True)
# clean sec_edgar_fillings
import shutil 

if os.path.exists("C:\\Users\\q33-liu\\sec-edgar-filings"):
    shutil.rmtree("C:\\Users\\q33-liu\\sec-edgar-filings")

# 🔍 Extract Item 7 MD&A
def extract_mda(html_text):
    import html
    from bs4 import BeautifulSoup
    import re

    html_text = html.unescape(html_text)
    soup = BeautifulSoup(html_text, "lxml")
    tags = soup.find_all()

    start_idx, end_idx = None, None

    # 🔍 find Item 7
    for i, tag in enumerate(tags):
        text = tag.get_text(strip=True).lower()
        style = str(tag.get("style", "")).lower()
        if re.match(r'^item[\s\xa0\-–~]*7(?![a-z0-9])', text) and (
            "bold" in style or "700" in style or tag.name in {"b", "strong"}
        ):
            start_idx = i
            break

    if start_idx is None:
        print("❌ can not find the start point: Item 7")
        return ""

    # 🔍 find the end point from the start point：7A > 8 > 8A
    for j in range(start_idx + 1, len(tags)):
        text = tags[j].get_text(strip=True).lower()

        if re.match(r'^item[\s\xa0\-–~]*7a(?![a-z0-9])', text):
            end_idx = j
            break
        elif re.match(r'^item[\s\xa0\-–~]*8(?![a-z0-9])', text) and end_idx is None:
            end_idx = j
        elif re.match(r'^item[\s\xa0\-–~]*8a(?![a-z0-9])', text) and end_idx is None:
            end_idx = j

    # ✂️ intercept the range of paragraphs
    selected = tags[start_idx:end_idx] if end_idx else tags[start_idx:]
    html_block = ''.join(str(tag) for tag in selected)

    # 🧼 clean HTML
    snippet = BeautifulSoup(html_block, "lxml")
    for t in snippet(["script", "style", "noscript", "table", "td", "tr"]):
        t.decompose()

    text = snippet.get_text(" ", strip=True)
    return re.sub(r'\s+', ' ', text)


# 🧹 Clean text for analysis
def clean_text(text):
    text = re.sub(r'[^A-Za-z ]', ' ', text).lower()
    tokens = word_tokenize(text)
    stop = set(stopwords.words("english"))
    html_noise = {"font", "style", "color", "size", "valign", "bottom", "arial", "family"}
    return [t for t in tokens if t not in stop and t not in html_noise and len(t) > 2]


# 📖 Read word lists
with open(positive_path, "r") as f:
    pos_words = set(w.strip().lower() for w in f if w.strip())

with open(negative_path, "r") as f:
    neg_words = set(w.strip().lower() for w in f if w.strip())

# 📖 Load target firm-year list
df = pd.read_csv(file_path)

# ✅ EDGAR Downloader
# dl = Downloader(r"\\nstu-nas01.uwe.ac.uk\users4$\q33-liu\Windows\Desktop\AFM\sec_data", 
#                 "Ziyi Qian 18015655828@163.com")
dl = Downloader("sec_data", "Ziyi Qian 18015655828@163.com") 
  # Use your own email
# 🔁 Process all firms
results = []
error_log = []

for i, row in tqdm(df.iterrows(), total=len(df), desc="extract MD&A"):
    ticker = row["ticker"]
    year = int(row["fyear"])
    gvkey = row["gvkey"]


    try:
        submission_year = year+1
        dl.get("10-K", ticker, after=f"{submission_year}-01-01", before=f"{submission_year+1}-01-01")

        base_path = os.path.join("sec-edgar-filings", ticker.upper(), "10-K")
        filing_paths = glob.glob(os.path.join(base_path, "*", "full-submission.txt"))
        
        # ✅ match 2 number of year -25- not -2025-
        year_suffix = str(submission_year)[-2:]
        matched_path = None
        for path in filing_paths:
            folder = os.path.basename(os.path.dirname(path))
            if f"-{year_suffix}-" in folder:
                matched_path = path
                break

        if not matched_path:
            print(f"⚠️ {ticker}-{year} has no {submission_year} document")
            continue

        with open(matched_path, "r", encoding="utf-8", errors="ignore") as f:
            full_text = f.read()

        mda_text = extract_mda(full_text)
        if not mda_text.strip():
            print(f"⚠️ {ticker}-{year}  MD&A is null")
            continue

        # print(f"\n{ticker}-{year} extract MD&A first 1000字：\n{mda_text[:]}")
        tokens = clean_text(mda_text)

        counter = Counter(tokens)
        # print(f"\n{counter.most_common(50)}")
        pos_count = sum(counter[w] for w in counter if w in pos_words)
        neg_count = sum(counter[w] for w in counter if w in neg_words)
        total = len(tokens)
        tone = (pos_count - neg_count) / total if total > 0 else 0

        results.append({
            "gvkey": gvkey,
            "ticker": ticker,
            "fyear": year,
            "mda_tone": tone,
            "pos_count": pos_count,
            "neg_count": neg_count,
            "token_total": total
        })

        if len(results) % 10 == 0:
            pd.DataFrame(results).to_csv(f"{save_dir}/partial_mda_tone_{i}.csv", index=False)

    except Exception as e:
        print(f"❌ skip {ticker}-{year}: {e}")
        error_log.append({"ticker": ticker, "year": year, "error": str(e)})

# 💾 Save final results
pd.DataFrame(results).to_csv(f"{save_dir}/final_mda_tone.csv", index=False)
pd.DataFrame(error_log).to_csv(f"{save_dir}/mda_tone_errors.csv", index=False)
print("✅ Done")
