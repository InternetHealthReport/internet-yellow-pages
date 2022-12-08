# Internet Yellow Pages


## Loading a dump and playing with it


## How to create a new dumps
Clone this repository.
```
git clone https://github.com/InternetHealthReport/internet-yellow-pages.git
cd internet-yellow-pages
```

Create python environment and install python libraries:
```
python3 -m venv .
source bin/activate
pip install -r requirements.txt
```

Configuration file, rename example file and add API keys:
```
cp config.conf.example config.conf
# Edit as needed
```

Create and populate a new database:
```
python3 create_db.py
```
### Tips and Tricks


## Candidate data sources
- RIS peers
- Atlas
- Regulators: start with ARCEP's open data
- openIPmap
- AS Hegemony
- dns tags
- CERT/ NOG per countries
- mobile prefixes (Japan)
- 

