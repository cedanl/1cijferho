![Braille fonts](https://see.fontimg.com/api/rf5/DOeDd/MGE4NTM1Njg3NjZhNDZhZTgwNTE0MjE5YzUxMzA0OTgudHRm/VEVYVCBBTkFMWVNJUw/braille-cc0.png?r=dw&h=81&w=1250&fg=00B17E&bg=000000&s=65)

<div align="center">
  <h1>1CijferHO Tool</h1>

  <p>🚀 Transform DUO data into research insights</p>

  <p>
    <a href="#"><img src="https://custom-icon-badges.demolab.com/badge/Windows-0078D6?logo=windows11&logoColor=white" alt="Windows"></a>
    <a href="#"><img src="https://img.shields.io/badge/macOS-000000?logo=apple&logoColor=F0F0F0" alt="macOS"></a>
    <a href="#"><img src="https://img.shields.io/badge/Linux-FCC624?logo=linux&logoColor=black" alt="Linux"></a>
    <img src="https://badgen.net/github/last-commit/cedanl/1cijferho" alt="GitHub Last Commit">
    <img src="https://badgen.net/github/contributors/cedanl/1cijferho" alt="Contributors">
    <img src="https://img.shields.io/github/license/cedanl/1cijferho" alt="GitHub License">
  </p>

  <p>🎬 Demo Video (Coming Soon!)</p>
</div>

## 📋 Overview
> [!NOTE]
> No Python or technical knowledge required! This tool is designed for everyone, regardless of programming experience.

**Breaking down the barriers between raw educational data and meaningful research for every HO and WO institution in the Netherlands.**

### 🚨 The Challenge
DUO provides educational data in complex formats that create massive barriers for researchers:
- **Fixed-width ASCII files** - Giant strings of data with no clear field separation
- **Unstructured metadata** - Field positions buried in poorly formatted .txt files  
- **Manual processing nightmare** - Hours of tedious work to make sense of a single dataset
- **Error-prone workflows** - Easy to misalign fields or lose data integrity

### ✨ Our Solution
A **blazingly fast, automated pipeline** that transforms DUO's complex data into clean, research-ready formats:

- ⚡ **Lightning Fast Processing** - Handle massive files in minutes using multiprocessing
- 🎯 **Zero Manual Work** - Fully automated from upload to final output
- ✅ **Bulletproof Validation** - Multiple validation steps ensure data integrity
- 🔒 **Privacy-First** - Automatic anonymization of sensitive columns (BSN, etc.)
- 📊 **Research-Ready Output** - Clean CSV and compressed Parquet files
- 🏢 **Institution-Friendly** - Designed for every HO and WO in the Netherlands

### 🎉 The Impact
**⏱️ Time Savings:** From 4-8 hours of manual work to 5-10 minutes of automated processing  
**🎯 Accuracy:** Error-free processing with automated validation  
**🤝 Collaboration:** Building towards standardized datasets for the entire Dutch higher education research community

## ✨ Features
- [x] **Smart Metadata Extraction** - Automatically finds and extracts field position tables from messy DUO .txt files
- [x] **Intelligent File Matching** - Automatically matches main data files with their corresponding decode/metadata files
- [x] **Turbo Conversion** - Converts fixed-width files to CSV format using multiprocessing for blazing speed
- [x] **Quality Assurance** - Validates conversion accuracy with comprehensive error checking
- [x] **Data Optimization** - Compresses files to efficient Parquet format (60-80% size reduction)
- [x] **Privacy Protection** - Automatically anonymizes sensitive columns using cryptographic hashing
- [x] **User-friendly Interface** - Streamlit-based UI requiring no coding knowledge
- [x] **`uv` Powered Setup** - One-click installation that handles Python and all dependencies automatically

<br>

## 🔧 First Time Setup
> [!WARNING]
> Do not skip these steps if this is your first time using this application. It will not work without them.

> [!TIP]
> Save the repository in a Projects/CEDA folder on your main drive for quick access.


### 1. Get the Repository

#### Option A: Clone with Git (or [Github Desktop](https://github.com/apps/desktop))
```bash
git clone https://github.com/cedanl/1cijferho.git
cd 1cijferho
```

#### Option B: Download ZIP
[![Download Repository](https://img.shields.io/badge/Download-Repository-green)](https://github.com/cedanl/1cijferho/archive/refs/heads/main.zip)

After downloading extract the ZIP file and navigate into the folder.

### 2. Install [![uv Badge](https://img.shields.io/badge/uv-DE5FE9?logo=uv&logoColor=fff&style=flat)](https://docs.astral.sh/uv/)

#### MacOS & Linux (Terminal)
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

#### Windows (Powershell or [Windows Terminal](https://apps.microsoft.com/detail/9n0dx20hk701?hl=nl-NL&gl=NL))
```powershell
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```
Close and reopen your terminal after installation.

#### Verify installation

```bash
uv self update
```

See the [installation documentation](https://docs.astral.sh/uv/getting-started/installation/) for
details and alternative installation methods.

<br>

## 🚀 Running the Application

Ready to see the magic happen? Your 1CijferHO Tool is just one command away! ✨

### First, get to the right spot:

Open a terminal in your `1cijferho` folder - it's super easy!
- **Windows**: `Shift + Right-click` in folder → `Open in Windows Terminal` 
- **Mac**: `Right-click` folder → `New Terminal at Folder`
- **VS Code**: Just click `Terminal` → `New Terminal`

Or simply navigate there:
```bash
cd path/to/1cijferho
```

### Then, launch with a single command:

```bash
uv run streamlit run src/main.py
```

That's it! The app will automatically spring to life in your browser. If you've completed all the steps in the First Time Setup correctly, this is the **only command** you'll need going forward. 🎉

> **Pro Tip**: Create a shortcut: `.bat` file (Windows) or `.sh` script (macOS/Linux)
> **Pro Tip**: Check out our [architecture.md](architecture.md) for technical details!

Happy analyzing! ✨📊📝


<br>

## 🛠️ Built With
[![uv Badge](https://img.shields.io/badge/uv-DE5FE9?logo=uv&logoColor=fff&style=flat)](https://docs.astral.sh/uv/)
[![Streamlit Badge](https://img.shields.io/badge/Streamlit-FF4B4B?logo=streamlit&logoColor=fff&style=flat)](https://streamlit.io/)
[![Python Badge](https://img.shields.io/badge/Python-3776AB?logo=python&logoColor=fff&style=flat)](https://www.python.org/)

## 🤲 Support
If you find this project helpful, please consider:
- ⭐ Starring the repo
- 🐛 Reporting bugs
- 💡 Suggesting features
- 💻 Contributing code

If you encounter any issues or need further assistance, please feel free to [open an issue](https://github.com/cedanl/1cijferho/issues) or contact a.sewnandan@hhs.nl | t.iwan@vu.nl

## 🙏 Acknowledgements
<strong>Special thanks to:</strong>
- [Ash Sewnandan](https://github.com/asewnandan) & [Tomer Iwan](https://github.com/Tomeriko96) for setting the foundation with a clean, user-friendly interface and robust architecture.
- [CEDA & Npuls](https://community-data-ai.npuls.nl/groups/view/44d20066-53a8-48c2-b4e9-be348e05d273/project-center-for-educational-data-analytics-ceda) for making this project possible by providing valuable resources and support.


## 🫂 Contributors
Thank you to all the [people](https://github.com/cedanl/1cijferho/graphs/contributors) who have already contributed to 1cijferho.


[![](https://github.com/asewnandan.png?size=50)](https://github.com/asewnandan)
[![](https://github.com/tin900.png?size=50)](https://github.com/tin900)
[![](https://github.com/Tomeriko96.png?size=50)](https://github.com/Tomeriko96)

## 🚦 License
![GitHub License](https://img.shields.io/github/license/cedanl/1cijferho)
