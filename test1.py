# -*- coding: utf-8 -*-

import siakr_common
import os
import pandas as pd
import siakrGoogleApi

###### PRESET ###############################################################################################################

# FILE, PATH, SOURCE
project_path = os.path.dirname(os.path.dirname(os.getcwd()))
path = os.getcwd()
source_name="dorco_thailand"
#open_file = project_path+"/data/survey/trans/"+source_name+'_trans_0.csv'
#open_file = project_path+"/data/survey/razor market study_thailand_data.xlsx"
open_file = project_path+"/data/"+source_name+'_0.1.xlsx'
save_file = project_path+"/data/"+source_name+'_sent_0.3.csv'

# # READ DATA
df = siakr_common.open_excelfile(file=open_file)
#df = siakr_common.open_file(file=open_file, sep=';')
#df = pd.read_csv(open_file, sep=';')
print(df.shape[0])
#df = df[:10]

# text = "Брошу свою и куплю себе такую. Парни вы как спину бреете?. Я тогда с ума сойду,если еще и спину брить начну,у меня на бритье будет уходить часа по 4 в день"
# text ="I'll throw mine and buy myself one. How do you guys shave your back? Then I'll go crazy, if I also start shaving my back, it will take me 4 hours to shave a day"
# doctrans = siakrGoogleApi.googleTrans(text, target_language='en')
# docSent = siakrGoogleApi.googleSent(doctrans, language='en')
# print(doctrans)
# print(docSent)

#siakrGoogleApi.googleSentIter(df, text_col='contentEn', id_col='sia_id', sentp_save_col='Sentiment_point', sent_save_col='Sentiment', save_path=project_path + '/data/sentiment/', file_name='vietnam_sent_v', target_language='en', interval=1000)
#siakrGoogleApi.googleTransIter(df, text_col='content', id_col='sia_id', save_col='contentEn', save_path=project_path + '/data/survey/trans/', file_name='dorco_vietnam_trans', target_language='en', interval=30000)
#siakrGoogleApi.googleSentTransIter(df, title_col='title', content_col='Mention Content', text_col='textNorm', id_col='sia_id', title_save_col='titleEn', content_save_col='contentEn', trans_save_col='textEn', sentp_save_col='Sentiment_point', sent_save_col='Sentiment', save_path=project_path + '/data/trans/', file_name='dorco_vietnam_trans', target_language='en', interval=1000)
#siakrGoogleApi.googleSentTransIter_survey(df, content_col='content', id_col='sia_id', trans_save_col='contentEn', sentp_save_col='Sentiment_point', sent_save_col='Sentiment', save_path=project_path + '/data/survey/trans/', file_name='dorco_thailand_trans', target_language='en', interval=1000)

siakrGoogleApi.googleSentTransIter_survey(df, content_col='content', id_col='sia_id', trans_save_col='contentEn', sentp_save_col='Sentiment_point', sent_save_col='Sentiment', save_path=project_path + '/data/survey/trans/', file_name='dorco_thailand_trans', target_language='en', interval=1000)

final_df = siakr_common.read_source_files_from_folder(path=project_path + '/data/trans/', ext='csv', sep=';')
final_df.to_csv(save_file, encoding='utf-8-sig', sep=';')
