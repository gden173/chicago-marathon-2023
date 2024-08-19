
# Chicago Marathon 2023 

<!--toc:start-->
- [Marathon Cheaters](#marathon-cheaters)
    - [Description](#description)
        - [Data](#data)
        - [Location](#location)
        - [Scraping Script](#scraping-script)
    - [Attribution](#attribution)
<!--toc:end-->

## Description 

The initial dataset that has been collected was scraped from the [2023 Chicago
Marathon](https://results.chicagomarathon.com/2023) official results in which
the late [Kelvin Kiptum](https://en.wikipedia.org/wiki/Kelvin_Kiptum) ran the
current world record marathon time of `02:00:30` and 
the 2024 Paris Olympic Marathon Championt [Siffan Hassan](https://en.wikipedia.org/wiki/Sifan_Hassan) ran the
Second fastest recorded time for women.

## Data 

### Location

All data is located in the `data/` directory. The data is stored in a Parquet files, 
with the aggregated parquet file being `data/chicago-marathon-results-2023.parquet`.

### Format 

The data is in the format 
 - 'Name (CTZ)' 
 - 'Age Group'
 - 'Bib Number' 
 - 'City' 
 - `State'
 - 'Gender' 
 - 'Short'
 - 'Split'
 - 'Time Of Day'
 - 'Time'
 - 'Diff'
 - 'min/km'
 - 'km/h'
 - 'min/mile'
 - 'miles/h'

### Scraping Script 

The script that scrapes the data is located in `[scrape.py](scrape.py)`. The
script uses the `requests` and `beautifulsoup4` libraries to scrape the data,
it relies on the [`chicago_marathon_records.csv`](chicago_marathon_records.csv)
file to get the list of urls to scrape.


## Attribution

- [Chicago Marathon Official Page](https://results.chicagomarathon.com/2023)
