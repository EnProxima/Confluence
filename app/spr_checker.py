from atlassian import Confluence
from bs4 import BeautifulSoup
import pandas as pd

SAVE_TO='mismatched_spr.xls'
WORD='label="msd_spm_spr" and parent="128659838" ' #search string. Searching only active SPR
SPR_NUM_LIMIT=500 #Limit of query resolt wo pagination
#Required fields for old fashioned SPR
spr_req_fields=['Customer',
                'Language',
                'Subscribers base, mln',
                'Service',
                'SPR Type',
                'Business responsible',
                'Target service operation start date',
                'Estimation date',
                'Business responsible',
                'MSD responsible',
                'MSD Jira task']

confluence = Confluence(
    url='https://confluence_address',
    username='LOGIN',
    password='PASSWORD')



def get_mismatched_spr(confl_resp):
    err_spr_dict = {}
    for answer in confl_resp.get('results'):
        content1 = confluence.get_page_by_id(page_id=answer['content']['id'], expand="body.view")
        soup = BeautifulSoup(content1['body']['view']['value'], 'html.parser')
        table = soup.find_all('table')[0]
        spr_tables_all = pd.read_html(str(table))
        # check for new template of SPR with * as a marker
        err_fields_new = spr_tables_all[0].loc[(spr_tables_all[0][0].str[-1:] == '*') & (spr_tables_all[0][1]).isna()][
            0]
        # check for old template of SPR
        err_fields_old = \
        spr_tables_all[0].loc[(spr_tables_all[0][0].isin(spr_req_fields)) & (spr_tables_all[0][1]).isna()][0]
        if not err_fields_old.empty or not err_fields_new.empty:
            err_spr_dict.update({answer.get('title'):
                                     [confluence.get_page_by_id
                                      (page_id=answer['content']['id'])['history']['createdBy']['username']] + list(
                                         err_fields_old)
                                     if not err_fields_old.empty else [confluence.get_page_by_id(
                                         page_id=answer['content']['id'])['history']['createdBy']['username']] + list(
                                         err_fields_new)})

    return err_spr_dict

cql = "{}".format(WORD)
answers = confluence.cql(cql,limit=SPR_NUM_LIMIT)
df_spr_mimmatched=pd.DataFrame.from_dict(get_mismatched_spr(answers), orient='index')
df_spr_mimmatched=df_spr_mimmatched.reset_index()

df_spr_mimmatched.to_excel(SAVE_TO)