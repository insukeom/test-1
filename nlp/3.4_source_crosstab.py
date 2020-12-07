import siakr_common
import siakr_tabulation
import os
import siakr_TabSynthesio
import pandas as pd
from collections import Counter

def SaveMultpleDFs(dataframe_llist, sheet_name_list, save_file_name):
    writer = pd.ExcelWriter(save_freq_file, engine='openpyxl')
    workbook = writer.book
    for d, dataframe_list in enumerate(dataframe_llist):
        print(' - save in', sheet_name_list[d], 'sheet')
        worksheet = workbook.create_sheet()
        worksheet.title = sheet_name_list[d]
        writer.sheets[sheet_name_list[d]] = worksheet
        worksheet.cell(row=1, column=1).value = sheet_name_list[d]
        r = 3
        for df in dataframe_list:
            worksheet.cell(row=r, column=1).value = df.name
            df.to_excel(writer, sheet_name=sheet_name_list[d], startrow=r)
            r = r + df.shape[0] + 3
    writer.save()
    writer.close()
    print('\n')

###### PRESET ###############################################################################################################

# FILE, PATH, SOURCE
project_path = os.path.dirname(os.path.dirname(os.getcwd()))
path = os.getcwd()
source_name="genesis_kr"
open_file = project_path+"/data/"+source_name+'_2.1.csv'
output_path = 'output_kr'
min_length = 0

banner_name = 'banner_kr'
base_name = 'base_banner_kr'

text_col_for_base_filter = 'textNorm' #브랜드별로 테이블의 베이스를 잡을때 기준이 되는 텍스트 칼럼
token_col_for_base_filter = 'key_tokens_tagless_set'  #브랜드별로 테이블의 베이스를 잡을때 기준이 되는 토큰 칼럼
text_col_for_data = 'textNorm' # 데이터를 셀때 사용하는 텍스트 칼럼
token_col_for_data = 'key_tokens_tagless_set' # 데이터를 셀때 사용하는 토큰 칼럼

##############################################################################################################################

# READ DATA
df = siakr_common.open_file(open_file, sep=';')
df = df[df['half_year']=='2020-h01']
# READ BANNER AND WHITE LIST
banner = siakr_tabulation.readBannerDF(project_path + '/lib/' + banner_name + '.csv', sep=';')
base_list = siakr_tabulation.readBannerDF(project_path + '/lib/' + base_name + '.csv', sep=';')

data_col = 'site_category'
freq_dfs = [pd.DataFrame() for x in range(len(base_list))]
perc_dfs = [pd.DataFrame() for x in range(len(base_list))]
for i, [switch, keyword_list, name] in enumerate(base_list):
    ndf = siakr_tabulation.DataScreener(df=df, switch=switch, keyword_list=keyword_list, text_col=text_col_for_base_filter, token_col=token_col_for_base_filter)
    freq_dfs[i], perc_dfs[i] = siakr_TabSynthesio.SynthesioWordBannerSingleAnswer(table_name=name, df=ndf, data_col=data_col, sentiment_col='Sentiment', target_sentiment='positive', banner=banner,
                                                                    text_col=text_col_for_data, token_col=token_col_for_data, min_len=min_length, print_sent=False)

for i, df in enumerate(freq_dfs):
    name = freq_dfs[i].name
    freq_dfs[i] = freq_dfs[i].transpose()
    freq_dfs[i].name = name

for i, df in enumerate(perc_dfs):
    name = perc_dfs[i].name
    perc_dfs[i] = perc_dfs[i].transpose()
    perc_dfs[i].name = name

save_freq_file = project_path + '/' + output_path + '/' + source_name + '_2020h1_crosstab_' + data_col + '_min_len_' + str(min_length) + '_' + banner_name + '_BaseBy_' + text_col_for_base_filter + '_' + 'DataIn_' + text_col_for_data + '.xlsx'
SaveMultpleDFs([freq_dfs, perc_dfs], ['Freq', 'Perc'], save_freq_file)