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
        create_db_objects(c)
        
        # get odds
        odds = get_odds()

        # insert odds data into sql
        for match in odds:
            sql = '''
                INSERT OR IGNORE INTO match (event_time, round, home_team, away_team)
                VALUES (:event_time, :round, :home_team, :away_team);
            '''
            c.execute(sql, match)
            # id = c.lastrowid
            sql = '''
                INSERT INTO odds (match_id, home_odds, away_odds, market, source, updated)
                SELECT id, :home_odds, :away_odds, :market, :source, datetime('now','localtime')
                FROM match
                WHERE round=:round AND home_team=:home_team AND away_team=:away_team
            '''
            c.execute(sql, match)

        # commit and close
        conn.commit()
        c.close()

        # print table
        # db_print(DB, "select * from afl_odds ORDER BY updated desc, event_time asc limit 20")

    except Exception as e:
        print(e)

    finally:
        if conn:
            conn.close()


def create_db_objects(cursor):
    
        sql = '''
            CREATE TABLE IF NOT EXISTS match (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_time DATETIME,
                round INTEGER,
                home_team STRING,
                away_team STRING,
                UNIQUE(event_time,home_team,away_team)
            )
        '''
        cursor.execute(sql)
        sql = '''
            CREATE TABLE IF NOT EXISTS odds (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                match_id INTEGER,
                market STRING,
                home_odds REAL,
                away_odds REAL,
                source STRING,
                updated DATETIME NOT NULL
            );
        '''
        cursor.execute(sql)
        sql = '''
            CREATE VIEW IF NOT EXISTS afl_odds AS 
            SELECT round, event_time, home_team, home_odds, away_team, away_odds, updated
            FROM match m 
            INNER JOIN odds o ON m.id = o.match_id
        '''
        cursor.execute(sql)


def get_odds():

    try:
        # URL for scraping AFL odds
        url = "https://www.sportsbet.com.au/betting/australian-rules/afl"

        # get that beautiful soup
        page = urllib.request.urlopen(url)
        soup = BeautifulSoup(page, 'html.parser')

        # with open("soup1.html", "w", encoding="UTF-8") as f:
        #     f.write(soup.prettify())
        
        #  get the round number
        round = soup.find("li", attrs={"data-automation-id": "competition-round-selector-1"}).getText()

        # get the relevant part of the soup
        regex = re.compile("\d+-competition-event-card")
        matches = soup.find_all("div", attrs={"data-automation-id": regex})

        # for match in matches:
        #     with open("soup2.html", "a", encoding="UTF-8") as f:
        #         f.write(match.prettify())

        # extract out time from soup
        event_times = []
        for match in matches:
            et = match.find("span", attrs={"data-automation-id": "competition-event-card-time"})
            event_times.append(et.getText())

        # extract out odds from soup for each market
        mkt_odds = []
        for match in matches:
            o = match.find_all("span", attrs={"data-automation-id": "price-text"})
            mkt_odds.append(cleaner(o))

        # extract out market labels from soup
        mkt_labels = []
        for match in matches:
            mkt = match.find_all(
                "div", attrs={"data-automation-id": "market-coupon-label"})
            mkt_labels.append(cleaner(mkt))

        # extract out participants from soup
        participants = []
        regex = re.compile("(participant-(one|two))")
        for match in matches:
            team = match.find_all("div", attrs={"data-automation-id": regex})
            participants.append(cleaner(team))

        # all lists should be same size (number of matches)
        if (len(mkt_odds) != len(mkt_labels)
            or len(mkt_odds) != len(participants)
                or len(mkt_labels) != len(participants)):
            return None

        #  reset list of matches
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
                    market = mkt
                    break

            # add dict to list of matches
            matches.append({
                "event_time": dt,
                "round": round,
                "home_team": fix_team_name(teams[0]),
                "away_team": fix_team_name(teams[1]),
                "market": market,
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
