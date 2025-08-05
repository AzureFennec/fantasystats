import logging
import os
import time
from datetime import date
import pandas as pd
import requests
from bs4 import BeautifulSoup

os.makedirs('logs', exist_ok=True)

logging.basicConfig(
    filename=f'logs/log_{date.today()}.txt',
    level=logging.INFO,
    format='%(levelname)s - %(message)s',
    filemode='w'
)

pd.set_option('display.max_colwidth', None)

class Game:
    def __init__(self,team,html,cat):
        self.team=team
        self.html=html
        self.cat=cat
        self.df=None

categories={
        'passing':{
            'dataframe':[],
            'table':3,
            'drop_columns':['Age','Pos','QBrec','Cmp%','TD%','Succ%','Lng','AY/A','Y/G','Rate','Sk%','NY/A','ANY/A','4QC','Awards'],
            'type':'offense',
            'pivot':{
                'start':3,
                'end':15
            },
            'html':{
                'id':'passing'
            }
        },
        'rushing':{
            'dataframe':[],
            'table':4,
            'columns':{
                'start':6,
                'end':14
            },
            'drop_columns':['1D','Succ%','Lng'],
            'type':'offense',
            'pivot':{
                'start':1,
                'end':6
            },
            'html':{
                'id':'rushing_and_receiving'}
        },
        'receiving':{
            'dataframe':[],
            'table':4,
            'columns':{
                'start':15,
                'end':26
            },
            'drop_columns':['1D','Succ%','Lng'],
            'type':'offense',
            'pivot':{
                'start':1,
                'end':9
            },
            'html':{
                'id':'rushing_and_receiving'
            }
        },
        'field-goals':{
            'dataframe':[],
            'table':6,
            'categories':{
                '01-19':{
                    'Attempted':2,
                    'Made':3
                },
                '20-29':{
                    'Attempted':4,
                    'Made':5
                },
                '30-39':{
                    'Attempted':6,
                    'Made':7
                },
                '40-49':{
                    'Attempted':8,
                    'Made':9
                },
                '50+':{
                    'Attempted':10,
                    'Made':11
                },
                'Total':{
                    'Attempted':12,
                    'Made':13
                },
                'Long':{
                    'Long':14
                }
            },
            'drop_columns':['FG%','Age','Pos','G','GS','XPA','XPM','XP%','KO','KOYds','TB','TB%','KOAvg','Awards'],
            'type':'special_teams',
            'html':{
                'id':'kicking'
            }
        },
        'defense':{
            'dataframe':[],
            'table':8,
            'drop_columns':['Awards','Age'],
            'type':'defense',
            'html':{
                'id':'defense'
            }
        }}

teams={
    'Buffalo Bills':{       #AFC East
        'url':'buf'
    },
    'Miami Dolphins':{
        'url':'mia'
    },
    'NE Patriots':{
        'url':'nwe'
    },
    'New York Jets':{
        'url':'nyj'
    },
    'Baltimore Ravens':{    #AFC North
        'url':'rav'
    },
    'Cleveland Browns':{
        'url':'cle'
    },
    'Pittsburgh Steelers':{
        'url':'pit'
    },
    'Cicninnati Bengals':{
        'url':'cin'
    },
    'KC Chiefs':{           #AFC West
        'url':'kan'
    },
    'LA Chargers':{
        'url':'sdg'
    },
    'Denver Broncos':{
        'url':'den'
    },
    'Las Vegas Raiders':{
        'url':'rai'
    },
    'Houston Texans':{      #AFC South
        'url':'htx'
    },
    'Indiannapolis Colts':{
        'url':'clt'
    },
    'Tennessee Titans':{
        'url':'oti'
    },
    'Jacksonville Jaguars':{
        'url':'jax'
    },
    'New York Giants':{        #NFC East
        'url':'nyg'
    },
    'Dallas Cowboys':{
        'url':'dal'
    },
    'Washington Commanders':{
        'url':'was'
    },
    'Philadelphia Eagles':{ 
        'url':'phi'
    },
    'Chicago Bears':{           #NFC North
        'url':'chi'
    },
    'Greenbay Packers':{
        'url':'gnb'
    },
    'Detroit Lions':{
        'url':'det'
    },
    'Minesotta Vikings':{
        'url':'min'
    },
    'LA Rams':{                 #NFC West
        'url':'ram'
    },
    'SF 49ers':{
        'url':'sfo'
    },
    'Seattle Seahawks':{
        'url':'sea'
    },
    'Arizona Cardinals':{
        'url':'crd'
    },
    'Tampa Bay Buccaneers':{    #NFC South
        'url':'tam'
    },
    'Atlanta Falcons':{
        'url':'atl'
    },
    'New Orleans Saints':{
        'url':'nor'
    },
    'Carolina Panthers':{
        'url':'car'
    }
}

errors={}

def load_page(url):
    response = requests.get(url)
    response.raise_for_status()
    time.sleep(6) # rate limiter. maintains compliance with PFR's scraping guideline(no more than 10 queries per minute)
    return response.text

def build_table(game):
    cat=game.cat
    html=game.html
    team=game.team

    logging.info(f'Building table for {cat} for {team}\n')

    soup=BeautifulSoup(html,"html.parser")

    id=categories[cat]['html']['id']

    table=soup.find("table",id=id,attrs={'data-soc-sum-phase-type':'reg'})

    if not table:
        logging.info(f'Unable to locate table for {cat}. Checking comments.\n')
        # Fallback: search inside HTML comments
        from bs4 import Comment
        comments=soup.find_all(string=lambda text: isinstance(text, Comment))
        for comment in comments:
            if 'table' in comment:
                comment_soup=BeautifulSoup(comment, 'html.parser')
                table=comment_soup.find("table", id=id, attrs={'data-soc-sum-phase-type': 'reg'})
                if table:
                    break

    if table:
        logging.debug(f'Table for {team} {cat} successfuly loaded by scraping logic in build_table')
    else:
        logging.debug(f'Table for {team} {cat} failed to be extracted by the scraping logic in build_table.')

    if cat=='passing':
        rows=table.find_all("tr")[1:-1]
    else:
        rows=table.find_all("tr")[2:-1]
    table_data=[]

    for row in rows:
        row_data=[cell.text.strip() for cell in row.find_all(["td", "th"])]
        table_data.append(row_data)

    thead=table.find("thead")
    if thead:
        rows=thead.find_all("tr")
        if rows:
            headers=[th.text.strip() for th in rows[-1].find_all("th")]

    try:
        df=pd.DataFrame(table_data,columns=headers)
        logging.debug(f'Dataframe for {team} {cat} successfuly created at conclusion of build_table. Dataframe is:\n\n{df}\n')
    except (ValueError,KeyError):
        raise ValueError (f'Dataframe for {team} {cat} failed to build at conclusion of build_table')
    game.df=df

def dfclean(game):
    df=pd.DataFrame(game.df)
    category=game.cat
    team=game.team

    logging.info(f'Cleaning {category} for {team}\n')
    logging.debug(f'Dataframe at start of dfclean is:\n{df}\n')

    df=df.rename(columns={'Player':'Names'})
    if category in ['receiving','rushing']: # PFR stores rushing/receiving stats together- this will take just the names and the relevant stast for either rushing or receiving
        names=pd.Series(df['Names'],name='Names')
        catdf=df.iloc[:, categories[category]['columns']['start']:categories[category]['columns']['end']]
        df=pd.concat([names,catdf],axis =1)
    elif category=='passing':
        df=df.iloc[:, [i for i in range(df.shape[1]) if i!=25]]
        df=df.drop(columns='Rk')
    elif category=='defense':
        droprows=[10,11,22,23,34,35,46,47]
        droprows=df.index.intersection(droprows)
        try:
            df=df.drop(droprows).reset_index(drop=True)
            logging.debug(f'Defense table successfuly dropped rows')
        except Exception as e:
            logging.debug(f'Defense table failed to drop rows due to the following issue: {e}\n')
        dfconvert(game)

        try:
            df=df.rename(columns={df.columns[6]: 'Yds(int)',df.columns[13]: 'Yds(fmb)'})
            logging.debug(f'Defense table successfully renamed.\n')
        except Exception as e:
            logging.debug(f'Defense table failed to rename due to the following error: {e}\n')
        df=df[(df.Comb!=0) | (df.Solo!=0)]

    dropcols=categories[category]['drop_columns']
    
    df=df.drop(columns=dropcols)
    
    logging.debug(f'At the end of the cleaning process, {category} df is:\n\n{df}\n')

    game.df=df

def dftransform(game):
    df=pd.DataFrame(game.df)
    category=game.cat
    team=game.team

    logging.info(f'Transforming table for {category}\n')

    if category=='field-goals':
        df1=[]
        for distance in categories[category]['categories'].keys():
            for type in categories[category]['categories'][distance].keys():
                df2=pd.DataFrame()
                df2['Names']=df['Names']
                df2['Distance']=distance
                df2['Category']=type
                col_number=categories[category]['categories'][distance][type]
                df2['Value']=df.iloc[:,col_number]
                df1.append(df2)
        df=pd.concat(df1,ignore_index=True)
        df=df.dropna()
    elif categories[category]['type']=='offense':
        df=df.sort_values(by='Yds',ascending=False)
        df = df[df.Yds!= 0]
        df=df.reset_index(drop=True)
        df['teamrank']=df.index+1
    df['Team']=team
    df=df.fillna(0)

    logging.debug(f'At the end of the transforming process, {category} df is:\n\n{df}\n')

    game.df=df

def dfconvert(game):
    df=pd.DataFrame(game.df)
    for col in df.columns:
        try:
            df[col]=pd.to_numeric(df[col],errors='raise')
        except (TypeError,ValueError):
            pass
    game.df=df

def runpipeline():
    for team in teams.keys():
        print(f'Team is:\n\n{team}')
        logging.info(f'Starting for {team}\n')
        # This block will cycle through the page for each team's 2024 stats. Adjust the year as you see fit.
        url=f'https://www.pro-football-reference.com/teams/{teams[team]["url"]}/2024.htm'
        logging.debug(f'URL is {url}')
        for attempt in range(3):
            try:
                html=load_page(url)
                if attempt>0:
                    logging.warning(f'webpage for {team} loaded, however failed {attempt} times')
                break
            except requests.exceptions.RequestException as e:
                if attempt==2:
                    logging.error(f'webpage for {team} failed 3 times and was not loaded due to the following error: {e}')
        for cat in categories.keys(): 
            game=Game(team,html,cat)
            for attempt in range(3):
                try:
                    build_table(game) # this is a critical line- the dataframe created here will be used for the rest of the code
                    break
                except AttributeError:
                    failure=f'\n{'='*50}\n\nFatal error- Unable to load the following table: {team}:{cat}. \n\nCheck if the webpage has changed its html structure.'
                    logging.critical(failure)
            try:
                dfclean(game) # will remove the team totals row + drops unwanted columns
            except AttributeError as e:
                logging.error(f'Unable to clean {cat} for {team} due to the following error: {e}\n')
                continue
            dfconvert(game)
            dftransform(game)
            try:
                categories[cat]['dataframe'].append(game.df)
            except Exception as e:
                logging.error(f'Unable to append {cat} for {team} due to the following error:{e}\n')

def ExcelExport():
    with pd.ExcelWriter('FACT_Stats.xlsx',mode='a',if_sheet_exists='replace') as writer:
        totalcats=len(categories)
        successfulcats=0
        for cat in categories.keys():
            try:
                df=pd.concat(categories[cat]['dataframe'])
                successfulcats+=1
            except Exception as e:
                logging.error(f'Unable to concatenate {cat} due to the following error: {e}')
                continue
            df.to_excel(writer,sheet_name=cat,index=False)
        if successfulcats==totalcats:
            logging.info('All categories successfuly saved.')
        logging.info(f'Code completed.')

if __name__ == '__main__':
    runpipeline()
    ExcelExport()
