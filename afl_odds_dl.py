from bs4 import BeautifulSoup
from datetime import datetime
import pandas as pd
import pytz
import re
import sqlite3
import urllib.request

DB = "afl_odds.db"
YEAR = datetime.today().year


def main():

    try:
        # create db if doesnt exist
        conn = sqlite3.connect(DB)
        c = conn.cursor()

        #  create table if doesnt exist
        sql = '''
            CREATE TABLE IF NOT EXISTS tbl_AFL_Odds (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_time DATETIME,
                home_team STRING,
                home_odds REAL,
                away_team STRING,
                away_odds REAL,
                source STRING,
                updated DATETIME NOT NULL
            )
        '''
        c.execute(sql)

        # insert data from scraping the odds
        sql = '''
            INSERT INTO tbl_AFL_Odds (updated, event_time, home_team, away_team, home_odds, away_odds, source)
            VALUES (datetime('now','localtime'), :event_time, :home_team, :away_team, :home_odds, :away_odds, :source)
        '''
        c.executemany(sql, get_odds())

        # commit and close
        conn.commit()
        c.close()

        # print table
        db_print(DB, "select * from tbl_AFL_Odds ORDER BY updated desc, event_time asc limit 20")

    except Exception as e:
        print(e)

    finally:
        if conn:
            conn.close()


def get_odds():

    try:
        # URL for scraping AFL odds
        url = "https://www.sportsbet.com.au/betting/australian-rules/afl"

        # get that beautiful soup
        page = urllib.request.urlopen(url)
        soup = BeautifulSoup(page, 'html.parser')

        # with open("soup1.html", "w", encoding="UTF-8") as f:
        #     f.write(soup.prettify())

        # get the relevant part of the soup
        regex = re.compile("\d+-competition-event-card")
        soup = soup.find_all("div", attrs={"data-automation-id": regex})

        # for s in soup:
        #     with open("soup2.html", "a", encoding="UTF-8") as f:
        #         f.write(s.prettify())

        # extract out time from soup
        event_times = []
        for s in soup:
            t = s.find("span", attrs={"data-automation-id": "competition-event-card-time"})
            event_times.append(t.getText())

        # extract out odds from soup for each market
        mkt_odds = []
        for s in soup:
            t = s.find_all("span", attrs={"data-automation-id": "price-text"})
            mkt_odds.append(cleaner(t))

        # extract out market labels from soup
        mkt_labels = []
        for s in soup:
            t = s.find_all(
                "div", attrs={"data-automation-id": "market-coupon-label"})
            mkt_labels.append(cleaner(t))

        # extract out participants from soup
        participants = []
        regex = re.compile("(participant-(one|two))")
        for s in soup:
            t = s.find_all("div", attrs={"data-automation-id": regex})
            participants.append(cleaner(t))

        # all lists should be same size (number of matches)
        if (len(mkt_odds) != len(mkt_labels)
            or len(mkt_odds) != len(participants)
                or len(mkt_labels) != len(participants)):
            return None

        matches = []

        # loop through each match
        for i in range(len(mkt_odds)):
            # get attributes of match
            teams = participants[i]
            mkts = mkt_labels[i]
            odds = mkt_odds[i]
            dt = datetime.strptime(f"{event_times[i]} {YEAR}", "%A, %d %b %H:%M %Y") 

            # loop through all the betting markets for the match
            # eg. head to head, line, etc
            for j, mkt in enumerate(mkts):
                # only interested in head to head market
                if mkt.lower() == "head to head":
                    odds_home = odds[j * 2]
                    odds_away = odds[j * 2 + 1]
                    break

            # add dict to list of matches
            matches.append({
                "event_time": dt,
                "home_team": fix_team_name(teams[0]),
                "away_team": fix_team_name(teams[1]),
                "home_odds": odds_home,
                "away_odds": odds_away,
                "updated": str(datetime.now().astimezone(pytz.utc)),
                "source": re.search('https?://([A-Za-z_0-9.-]+).*', url).group(1)
            })
        return matches

    except Exception as e:
        print(f"Error occured\n{e}")
        return None


def cleaner(list):
    """" Cleans the lists extracted from beautiful soup """

    content = []
    for li in list:
        content.append(li.getText().replace("\n", " ").replace("\t""", " "))
    # print(content)
    return content


def fix_team_name(teamname):
    """ Standardise team names """
    if teamname == "Greater Western Sydney":
        teamname = "GWS"

    return teamname


def db_print(db, sql):

    # Read sqlite query results into a pandas DataFrame
    conn = sqlite3.connect(db)
    df = pd.read_sql_query(sql, conn)

    # print df
    print(f"\n{df.to_string(index=False)}\n")

    conn.close()


if __name__ == '__main__':
    main()
