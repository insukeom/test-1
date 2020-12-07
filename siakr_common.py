import os
import sys
import pandas as pd
import datetime as datetime
from collections import Counter
# from chatspace import ChatSpace
import decimal
import numpy as np
import re
import regex
import emoji
#import kss
import time
import math
from openpyxl import load_workbook
import torch
import ast


# INDEPENDENT FUNCTIONS ################################################################################################################################################################################

def spinning_cursor():
    while True:
        for cursor in '|/-\\':
            yield cursor


def flatten_llist(llist):
    list = [item for sublist in llist for item in sublist]
    return list


def printProgress (iteration, total, prefix = '', suffix = '', decimals = 1, barLength = 100):
	formatStr = "{0:." + str(decimals) + "f}"
	percent = formatStr.format(100 * (iteration / float(total)))
	filledLength = int(round(barLength * iteration / float(total)))
	bar = '#' * filledLength + '-' * (barLength - filledLength)
	sys.stdout.write('\r%s |%s| %s%s %s' % (prefix, bar, percent, '%', suffix)),
	if iteration == total:
		sys.stdout.write('\n')
	sys.stdout.flush()


def list_files(path, ext):
    filelist = []
    for name in os.listdir(path):
        if os.path.isfile(os.path.join(path, name)):
            if name.endswith(ext):
                filelist.append(name)
    return filelist


def append_df_to_excel(filename, dataframe_list, sheet_name_list):
	print('# siakr_common.append_df_to_excel #')
	print(' - target file:', filename)
	writer = pd.ExcelWriter(filename, engine='openpyxl', mode='a')
	writer.book = load_workbook(filename)
	for i, df in enumerate(dataframe_list):
		print(' - save in', sheet_name_list[i], 'sheet')
		df.to_excel(writer, sheet_name=sheet_name_list[i])
	writer.save()
	writer.close()
	print('\n')


def CountSaveLlistFreq(data_llist, label_list, save_freq_file, limit=0):
	"""
	# INTRODUCTION
	Count frequency of values in data_llist (list of list) and save frequencies. Returns nothing.
	When a comma separated text are found in the list of list, it convert the text into list and count

	# PARAMETER
	:param data_llist: list of list to get frequencies
	:param label_list: label for data_llist elements
	:param save_freq_file: string. Excel file name to save the frequencies
	:param limit. default=0, If 0, it returns the frequency table of all elements. Unless, it returns up to the rank specified as 'limit'. If limit=100, you will see top 100 items in order of frequency.

	:return: nothing
	"""
	print("# siakr_common.list_freq_counter #")
#	writer = pd.ExcelWriter(save_freq_file)
	writer = pd.ExcelWriter(save_freq_file, options={'strings_to_urls': False})

	for i, d in enumerate(data_llist):
		print('  - count in', label_list[i])
		list_counter = Counter(string_to_list(d))

		if limit == 0: list_freq = list_counter.most_common()
		else: list_freq = list_counter.most_common(limit)

		freq_df = pd.DataFrame()
		freq_df[label_list[i]] = list_freq
		freq_item = []
		freq_count = []

		for n in range(len(freq_df)):
			freq_item.append(freq_df[label_list[i]][n][0])
			freq_count.append(freq_df[label_list[i]][n][1])

		freq_df = freq_df.drop(axis=1, columns=label_list[i])
		freq_df['item'] = freq_item
		freq_df['count'] = freq_count
		freq_df = freq_df.reset_index(drop=True)
		freq_df.to_excel(writer, label_list[i])
	print('  - saving frequency...')
	writer.save()
	print('  - competed')
	print('\n')


def stringList_to_llist(data_list, sep):
	data_llist = [re.split(sep, string.strip().replace('[', '').replace(']', '').replace("'", '').replace('"', '')) if not isinstance(string, float) else [] for string in data_list]
	data_llist = [[token.strip() for token in sentence if token.strip() != ''] for sentence in data_llist]
	return data_llist


def string_to_list(data_list, sep=', '):
	data_list = [str(x) for x in data_list if isinstance(x, float) == False]
	string_data = sep.join(data_list)
	string_data = string_data.strip().replace('[', '').replace(']', '').replace("'", '').replace('"', '').split(',')
	string_data = [x.strip() for x in string_data]
	string_data = [x for x in string_data if len(x) > 0]
	return string_data


def escapeSpecialChracters(text):
    special_character_pair = [("(", "&ParenthesesOpen&"), (")", "&ParenthesesClose&"), ("[", "&squareBracketOpen&"),
                              ("]", "&SquareBracketClose&"), ("{", "&CurlyBracketOpen&"),
                              ("}", "&CurlyBracketClose&"), ("'", "&SingleQuotationMark&"),
                              ('"', "&DoubleQuotationMark&"), ("+", "&Plus&"), ("-", "&Minus&"), ("*", "&Star&"),
                              (".", "&Dot&"), ("^", "&Circumflex&"), ("|", "&VerticalBar&")]
    for a, b in special_character_pair:
        text = text.replace(a, b)
    return text


def recoverSpecialChracters(text):
    special_character_pair = [("(", "&ParenthesesOpen&"), (")", "&ParenthesesClose&"), ("[", "&squareBracketOpen&"),
                              ("]", "&SquareBracketClose&"), ("{", "&CurlyBracketOpen&"),
                              ("}", "&CurlyBracketClose&"), ("'", "&SingleQuotationMark&"),
                              ('"', "&DoubleQuotationMark&"), ("+", "&Plus&"), ("-", "&Minus&"), ("*", "&Star&"),
                              (".", "&Dot&"), ("^", "&Circumflex&"), ("|", "&VerticalBar&")]
    for b, a in special_character_pair:
        text = text.replace(a, b)
    return text


def sList_to_list(text, sep):
    text = escapeSpecialChracters(text)
    text = re.split(sep, text.strip().replace('[', '').replace(']', '').replace("'", '').replace('"', ''))
    text = [recoverSpecialChracters(t) for t in text]
    return text


def label_sia_id(dataframe):
	df = dataframe
	df["sia_id"] = df.index + 1
	cols = df.columns.tolist()
	cols = cols[-1:] + cols[:-1]
	df = df[cols]
	return df


def concat_texts(row_by_row_list, sep):
	row_by_row_list = row_by_row_list
	my_set = set()
	res = []
	for e in row_by_row_list:
		if e not in my_set:
			res.append(e)
			my_set.add(e)
	res = [x for x in res if not isinstance(x, float)]
	concat_done = sep.join(res)
	return concat_done


########################################################################################################################################################################################################


def read_source_files_from_folder(path, ext, sep):
	"""
	# INTRODUCTION
	If single file is in the folder, it returns a dataframe of the file.
	If multiple files are found in the path, it integrates the files and return a dataframe

	# PARAMETER
	:param path: Folder where data file is in
	:param ext: extention of file (e.g., csv)
	:param sep: column separator
	:return: dataframe
	"""

	print('# siakr_common.open_or_integrate files #')
	files = list_files(path, ext)
	df = pd.DataFrame()

	if len(files) == 1:
		print( '  - opening single file...')
		df = pd.read_csv(path+'/'+files[0], sep=sep)
	elif len(files) > 1:
		df_list = [0 for i in range(len(files))]
		# READING FILES
		print('  - reading ' + str(len(df_list)) + ' source files...')
		for i, f in enumerate(files):
			print('    - ', f)
			df_list[i] = pd.read_csv(path+'/'+f, sep=sep, error_bad_lines=False)
		print('  - integrating files...')
		df = pd.concat(df_list, axis=0, join='outer', sort=False)

	elif len(files) == 0:
		print(' ! No '+ ext + ' file in ' + path + '.')

	df.index.name = 'index'

	print('  * total number of lines:', df.shape[0])
	print('  - completed')
	print('\n')
	return df

def read_source_excelfiles_from_folder(path, ext):
	"""
	# INTRODUCTION
	If single file is in the folder, it returns a dataframe of the file.
	If multiple files are found in the path, it integrates the files and return a dataframe

	# PARAMETER
	:param path: Folder where data file is in
	:param ext: extention of file (e.g., csv)
	:param sep: column separator
	:return: dataframe
	"""

	print('# siakr_common.open_or_integrate files #')
	files = list_files(path, ext)
	df = pd.DataFrame()

	if len(files) == 1:
		print( '  - opening single file...')
		df = pd.read_excel(path+'/'+files[0])
	elif len(files) > 1:
		df_list = [0 for i in range(len(files))]
		# READING FILES
		print('  - reading ' + str(len(df_list)) + ' source files...')
		for i, f in enumerate(files):
			print('    - ', f)
			df_list[i] = pd.read_excel(path+'/'+f)
		print('  - integrating files...')
		df = pd.concat(df_list, axis=0, join='outer')

	elif len(files) == 0:
		print(' ! No '+ ext + ' file in ' + path + '.')

	df.index.name = 'index'

	print('  * total number of lines:', df.shape[0])
	print('  - completed')
	print('\n')
	return df

def open_file(file, sep):
	"""
	#PARAMETER
	:param file: path + file name
	:param sep: column separator
	:return: dataframe
	"""
	print('# siakr_common.open_file #')
	print('  - reading ' + file + '...')
	df = pd.DataFrame()
	df = pd.read_csv(file, sep=sep, error_bad_lines=False, engine='python')
	try:
		df = df.drop(axis=1, columns=['index'])
	except KeyError:
		pass
	df.index.name = 'index'
	print('  * total number of lines:', df.shape[0])
	print('  - completed')
	print('\n')
	return df


def open_excelfile(file):
	"""
	#PARAMETER
	:param file: path + file name
	:param sep: column separator
	:return: dataframe
	"""
	print('# siakr_common.open_file #')
	print('  - reading ' + file + '...')
	df = pd.DataFrame()
	df = pd.read_excel(file)
	try:
		df = df.drop(axis=1, columns=['index'])
	except KeyError:
		pass
	df.index.name = 'index'
	print('  * total number of lines:', df.shape[0])
	print('  - completed')
	print('\n')
	return df

def drop_columns(dataframe, target_column_list):
	"""
	#PARAMETER
	:param dataframe: pandas dataframe. dataframe with unnecessary or irrelevant columns
	:param target_column_list: list of column names. unnecessary or irrelevant columns
	:return: dataframe
	"""
	# DECLARE VARIABLES
	df = dataframe

	print('# siakr_common.drop_columns #')
	# IF TARGET COLUMNS PASSED
	if len(target_column_list) > 0:
		print('  * number of columns before processing:', df.shape[1])
		print('  - requested to drop ', len(target_column_list), 'columns')

		for c in target_column_list:
			try:
				df = df.drop(axis=1, columns=c)
			except KeyError:
				print('  ! column'+c+' is missing.')
		print('  * number of columns after processing:', df.shape[1])

	# IF TARGET COLUMN LIST IS EMPTY
	elif len(target_column_list) == 0:
		print('  ! no columns to drop assigned.')

	print('  - completed')
	print('\n')
	return df


def drop_duplicates(dataframe, target_columns):
	"""
	# INTRODUCTION
	Drop cases with duplicated data in designated columns. It keeps the first row among duplicates.

	#PARAMETER
	:param dataframe: pandas dataframe
	:param target_columns: list of column names
	:return: pandad dataframe
	"""
	# DECLARE VARIABLES
	df = dataframe

	print('# siakr_common.drop_duplicates #')
	print('  * total number of lines:', df.shape[0])

	# IF TARGET COLUMNS ASSIGNED
	if len(target_columns) > 0:
		print('  - eliminating duplicates in ' + ", ".join(target_columns) + '...')
		df = df.drop_duplicates(subset=target_columns, keep='first', inplace=False)
		print('  * number of lines after dropping duplicates:', df.shape[0])
	# IF TARGET COLUMN LIST IS EMPTY
	elif len(target_columns) == 0:
		print('  ! no target columns stated.')
	print('  - completed')
	print('\n')
	return df


def drop_NAs(dataframe, target_columns):
	"""
	# INTRODUCTION
	Drop cases with NAs in designated columns

	#PARAMETER
	:param dataframe: pandas dataframe
	:param target_columns: list of column names
	:return: pandas dataframe
	"""
	# DECLARE VARIABLES
	df = dataframe

	print('# siakr_common.drop_nas #')
	print('  * total number of lines:', df.shape[0])

	# IF TARGET COLUMNS ASSIGNED
	if len(target_columns) > 0:
		print('  - eliminating data with nas in ' + ", ".join(target_columns) + '...')
		df = df.dropna(axis=0, subset=target_columns)
		print('  * number of lines after processing:', df.shape[0])
	# IF TARGET COLUMN LIST IS EMPTY
	elif len(target_columns) == 0:
		print('  ! no target columns stated.')
	print('- completed')
	print('\n')
	return df


def drop_data_match(dataframe, target_column, target_match):
	"""
	# INTRODUCTION
	Drop a case if it matches with 'target_match" string

	#PARAMETER
	:param dataframe: pandas dataframe
	:param target_column: list of target column names
	:param target_match: string
	:return: pandas dataframe
	"""
	# DECLARE VARIABLES
	df = dataframe

	print('# siakr_common.drop_data_match')
	print('  * total number of lines:', df.shape[0])

	# IF TARGET COLUMNS AND MATCH LIST DELIVERED
	if len(target_column) > 0 and len(target_match) > 0:
		print('  - target column:', target_column)
		print('  - target match:', target_match)
		df = df[~df[target_column].isin(target_match)]
		print('  * number of lines after processing:', df.shape[0])
	else:
		print('  ! no match list or target column stated..')
	print('  - completed')
	print('\n')
	return df


def create_text_prep_column(dataframe, text_prep_column, source_columns):
	"""
	# INTRODUCTION
	Insert a new column for text analytics. It automatically merges multiple columns, if multiple column names are given.
	If single column is passed over, it simply copy the content in the source column to the new 'text_prep' column.
	Once 'text_prep' column is inserted, it replace colons with semicolons and count the length of each text in the new 'text_prep' column in to 'length' column.
	Finally, it inserted "[text_prep_column] + '_normailzied'" column which includes only [0-9ㄱ-ㅎ가-힣a-zA-Z\!\@\#\$\%\^\&\*\(\)\_\+\-\{\}\[\]\<\>\?\.]

	#PARAMETER
	:param dataframe: pandas dataframe
	:param text_prep_column: name of new column
	:param source_columns: list of column names to be integrated into the new 'text_prep' column.
	:return:
	"""
	df = dataframe
	text_prep_column = text_prep_column
	data_columns = source_columns
	text_prep_data=[]

	print('# siakr_common.create_text_prep_column #')
	print('  * total number of lines:', df.shape[0])

	# MERGE DATA FROM MULTIPLE COLUMNS
	if len(data_columns) > 1:
		print('  - merging text in multiple columns into text_prep column...')
		data_matrix = [[0 for p in range(df.shape[0])] for q in range(len(data_columns))]
		for i, c in enumerate(data_columns):
			data_matrix[i] = df[c].tolist()

		row_by_row_list = list(zip(*data_matrix))
		text_prep_data=list(map(lambda x: concat_texts(x, sep='. '), row_by_row_list))

	# COPY DATA FROM SINGLE COLUMN
	elif len(data_columns) == 1:
		print('  - copying data to text_prep column...')
		text_prep_data = df[data_columns[0]].tolist()

	df[text_prep_column] = text_prep_data

	# COUNT LENGTH
	print('  - counting length of text_prep column...')
	df['length'] = df[text_prep_column].str.len()

	print('  - completed')
	print('\n')
	return df


def filter_by_wordlist(dataframe, target_column, word_list, switch):
	"""
	# INTRODUCTION
	It filters the data with blacklist or whitelist.
	If switch=0, it works as a blacklist. It excludes all cases containing the words in the target column
	If switch=1, it works as a whitelist. It keeps all cases containing the words in the target column only and drop everything else from the dataframe.

	#PARAMETER
	:param dataframe: pandas dataframe
	:param target_col: string. name of the colume to look up words in black or whitelist
	:param word_list: list of words
	:param switch: integer. 0 is blacklist, 1 is whitelist
	:return: pandas dataframe
	"""
	# DECLARE VARIABLE
	df = dataframe
	switch_label = ['blacklist', 'whitelist']

	print('# siakr_common.filter_by_wordlist #')
	print('  - filter by ' + switch_label[switch])
	print('  * total number of lines:', df.shape[0])
	if len(word_list) == 0:
		print('  ! no items in the wordlist.')
	elif len(word_list) > 0:
		temp_list = []
		for w in word_list:
			word_list_mask = df[target_column].str.contains(w, regex=True)
			temp_list.append(word_list_mask)

		if switch == 1: #white list
			word_list_mask = [any(tup) for tup in zip(*temp_list)]

		elif switch == 0: #black list
			word_list_mask = [not any(tup) for tup in zip(*temp_list)]

		df = df[word_list_mask]
		print('  * number of lines after processing:', df.shape[0])
	print('  - completed')
	print('\n')
	return df


def filter_by_range(dataframe, target_column, min, max):
	"""
	# INTRODUCTION
	Screen out cases if the value in target_column is out of mix to max range

	# PARAMETER
	:param dataframe: pandas dataframe
	:param target_column: string. name of column where you'd like to check the range
	:param min: int. If min=0, no minimum limit will be applied
	:param max: int. If min=0, no maximum limit will be applied
	:return: pandas dataframe
	"""
	# DECLARE VAIRBALE
	df = dataframe

	print('# siakr_com.filter_by_range #')
	print('  * total number of lines:', df.shape[0])

	# MIN
	if min > 0:
		print('  - screening out if less than min(' + str(min) + ')...')
		df = df[df[target_column] >= min]
		print('  * number of lines after processing:', df.shape[0])
	elif min == 0:
		print('  ! no minimum limit applied')
	# MAX
	if max > 0:
		print('  - screening out if more than max limit(' + str(max) + ')...')
		df = df[df[target_column] <= max]
		print('  * number of lines after processing:', df.shape[0])
	elif max == 0:
		print('  * no maximum limit applied')
	print('  - completed')
	print('\n')
	return df


def ExtractDateTime(dataframe, date_col, time_col):
	# DECLARE VARIABLE
	df = dataframe
	if time_col:
		DateTime_list = [x + ' ' + y[:8] for x, y in zip(df[date_col].tolist(), df['Time'].tolist())]
	else:
		DateTime_list = df[date_col].tolist()
	print('# siakr_common.extract_date #')
	print('  * total number of lines:', df.shape[0])
	print('  - creating year, month, day, year_month, ymd and week columns...')

	Timestamp_list = list(map(lambda x: pd.Timestamp(x), DateTime_list))
	df['datetime'] = DateTime_list
	df['year'] = [x.year for x in Timestamp_list]
	df['year_month'] = [str(x.year)+'-'+str(x.month).zfill(2) for x in Timestamp_list]
	df['ymd'] = [str(x.year)+'-'+str(x.month).zfill(2)+'-'+str(x.day).zfill(2) for x in Timestamp_list]
	df['weekNo'] = [str(x.year)+'-w'+str(x.week).zfill(2) for x in Timestamp_list]
	df['weekDate'] = list(map(lambda x: datetime.datetime.strptime(x + '-1', "%Y-W%W-%w"), [str(d.year)+'-W'+str(d.week-1) for d in Timestamp_list]))
	print('  - completed')
	print('\n')
	return df

def naverExtractDateTime(dataframe, date_col):
	# DECLARE VARIABLE
	df = dataframe
	DateTime_list = df[date_col].tolist()
	print('# siakr_common.extract_date #')
	print('  * total number of lines:', df.shape[0])
	print('  - creating year, month, day, year_month, ymd and week columns...')

	Timestamp_list = list(map(lambda x: pd.Timestamp(x), DateTime_list))
	df['datetime'] = DateTime_list
	df['year'] = [x.year for x in Timestamp_list]
	df['year_month'] = [str(x.year)+'-'+str(x.month).zfill(2) for x in Timestamp_list]
	df['ymd'] = [str(x.year)+'-'+str(x.month).zfill(2)+'-'+str(x.day).zfill(2) for x in Timestamp_list]
	df['weekNo'] = [str(x.year)+'-w'+str(x.week).zfill(2) for x in Timestamp_list]
	df['weekDate'] = list(map(lambda x: datetime.datetime.strptime(x + '-1', "%Y-W%W-%w"), [str(d.year)+'-W'+str(d.week-1) for d in Timestamp_list]))
	print('  - completed')
	print('\n')
	return df

def site_account_name(dataframe, site_name_column, account_name_column):
	df = dataframe
	print('# siakr_common.site_account_name #')
	print('  * total number of lines:', df.shape[0])
	print('  - merging site name and account name...')
	df['site_account_name'] = df[account_name_column] + '@' + df[site_name_column]
	print('\n')
	return df

def abuser_filter(dataframe, site_name_column, account_name_column, abuser_limit):
	"""
	# INTRODUCTION
	First, it integrate the site name and account name. Then it counts the frequency of account@site.
	Finally, it screens out account@site appears more then abuser_limit.

	#PARAMETER
	:param dataframe: pandas dataframe
	:param site_name_col: string. name of site name column
	:param account_name_col: string. name of account name column
	:param abuser_limit: int.
	:return: pandas datafram with 'post_num_by_account' column
	"""
	# DECLARE VARIABLES
	df = dataframe

	print('# siakr_common.abuser_filter #')
	print('  * total number of lines:', df.shape[0])
	print('  - merging site name and account name...')
	df['site_account_name'] = df[account_name_column] + '@' + df[site_name_column]

	if abuser_limit > 0:

		df['boolean']=True
		print('  - counting number of posts by account@site...')
		df['posts_num_by_account']=df.groupby('site_account_name')['boolean'].transform('sum')
		df = df.drop(axis=1, columns='boolean')
		print('  - dropping abusers...')
		df['posts_num_by_account'] = df['posts_num_by_account'].fillna(value=0)
		df=df[df['posts_num_by_account'] <= abuser_limit]
		print('  * number of lines after processing:', df.shape[0])
	elif abuser_limit == 0:
		print('  ! no abuser limit assigned.')
	print('  - completed')
	print('\n')
	return df


def save_as_text(datalist, filename):
	f = open(filename, 'w')
	for d in datalist:
		if len(d)>0:
			d=d+'\n'
	f.write(d)
	f.close()


def createFolder(directory):
	try:
		if not os.path.exists(directory):
			os.makedirs(directory)
	except OSError:
		print(' ! error: creating directory')


def dataSampler(df, columns, size, mode):
	if size < 0:
		size = int(df.shape[0] * size)

	if mode == 0:
		df = df.sample(size)
	elif mode == 1:
		df = df[:size]
	else:
		print("Error: mode = 0 (random), or 1 ('from the beginning')")
		sys.exit()

	return_df=pd.DataFrame()
	for c in columns:
		return_df[c] = df[c]

	return return_df


##########################################################################################################################################################
# STATS
##########################################################################################################################################################

def rangeFreqByInterval(data_list, min, max, interval):
	print('# siakr.rangeFreqByInterval #')
	print(' - data size:', len(data_list))
	print(' - min:', min)
	print(' - max:', max)
	print(' - interval:', interval)
	df = pd.DataFrame(data_list)
	num_of_brackets = math.ceil((max - min) / interval) + 1
	print(' - number of brackets:', num_of_brackets)
	bracket_points = [round(min + x*interval, 2) for x in range(num_of_brackets)]
	freq_df = df.apply(pd.Series.value_counts, bins=bracket_points)
	freq_df.index.name = 'range'
	freq_df = freq_df.rename(columns={freq_df.columns[0]: 'count'})
	freq_df = freq_df.sort_index()
	freq_df['AccPerc'] = 100 * freq_df['count'].cumsum() / freq_df['count'].sum()
	print(freq_df)
	print('\n')
	return freq_df


def rangeFreqByBracketNum(data_list, min, max, num_of_brackets):
	print('# siakr.rangeFreqByBracketNum #')
	print(' - data size:', len(data_list))
	print(' - min:', min)
	print(' - max:', max)
	print(' - number of brackets:', num_of_brackets)
	df = pd.DataFrame(data_list)
	interval = math.ceil(max / num_of_brackets)
	print(' - interval:', interval)
	bracket_points = [round(min + x*interval, 2) for x in range(num_of_brackets)]
	freq_df = df.apply(pd.Series.value_counts, bins=bracket_points)
	freq_df.index.name = 'range'
	freq_df = freq_df.rename(columns={freq_df.columns[0]: 'count'})
	freq_df = freq_df.sort_index()
	freq_df['AccPerc'] = 100 * freq_df['count'].cumsum() / freq_df['count'].sum()
	print(freq_df)
	print('\n')
	return freq_df


def rangeFreqByPercentile(data_list, min, max, percentile_interval):
	print('# siakr.rangeFreqByPercentile #')
	print(' - data size:', len(data_list))
	print(' - min:', min)
	print(' - max:', max)
	num_of_brackets = math.ceil(100/percentile_interval)
	print(' - number of brackets:', num_of_brackets)
	df = pd.DataFrame(data_list)
	percentile_list = [i * percentile_interval for i in range(num_of_brackets+1)]
	bracket_points = np.percentile(data_list, percentile_list, interpolation='nearest')
	bp_rev =[]
	for b in bracket_points:
		if b not in bp_rev:
			bp_rev.append(b)
	bracket_points = bp_rev
	freq_df = df.apply(pd.Series.value_counts, bins=bracket_points)
	freq_df.index.name = 'range'
	freq_df = freq_df.rename(columns={freq_df.columns[0]: 'count'})
	freq_df = freq_df.sort_index()
	freq_df['AccPerc'] = round(100 * freq_df['count'].cumsum() / freq_df['count'].sum(), 2)
	print(freq_df)
	print('\n')
	return freq_df


##########################################################################################################################################################
# EXTRACT ITEMS
##########################################################################################################################################################


def hashtag_extractor(text):
	pattern = r'#([0-9ㄱ-ㅎ가-힣a-zA-Z\\u3040-\\u309F\\u30A0-\\u30FF\\u31F0-\\u31FF\\u4E00-\\u9FFF\-\_]+)'
	hash_w = re.compile(pattern)
	hashtag = ['#' + x for x in hash_w.findall(text)]
	return hashtag


def emoji_extractor(text):
	emoji_list = []
	data = regex.findall(r'\X', text)
	for word in data:
		if any(char in emoji.UNICODE_EMOJI for char in word):
			emoji_list.append(word)
	return emoji_list


def email_extractor(text):
	# emails = ", ".join(re.findall('([0-9a-zA-Z-_.]+@[0-9a-zA-Z]+\.[a-zA-Z]{2,4})', text))
	emails = re.findall('([0-9a-zA-Z-_.]+@[0-9a-zA-Z]+\.[a-zA-Z]{2,4})', text)
	return emails


def at_users_extractor(text):
	candidates = [y for x, y in re.findall('(^|\s)(@[0-9a-zA-Z-_.]+)', text)]
	at_users = [x for x in candidates if len(''.join(re.findall('[0-9a-zA-Z]', x))) >= 1 and len(x) >= 3]
	# at_users = ", ".join([y for x, y in re.findall('(^|\s)(@[0-9a-zA-Z-_\.]+)', text)]).strip()
	return at_users


def url_extractor(text):
	# urls = ", ".join(re.findall(r'https?://[a-zA-Z0-9.\\/?=_\-&%]+|www[a-zA-Z0-9.\\/?=_\-&%]+', text))
	urls = re.findall(r'https?://[a-zA-Z0-9.\\/?=_\-&%]+|www[a-zA-Z0-9.\\/?=_\-&%]+|m[.][a-zA-Z0-9.\\/?=_\-&%]+', text)
	return urls


def phone_number_extractor(text):
	# phone_numbers = ", ".join(re.findall('\d{2,4}-\d{3,4}-\d{4}|\d{2,4}\s\d{3,4}\s\d{4}|\d{2,4}\.\d{3,4}\.\d{4}', text))
	phone_numbers = re.findall('\d{2,4}-\d{3,4}-\d{4}|\d{2,4}\s\d{3,4}\s\d{4}|\d{2,4}\.\d{3,4}\.\d{4}|\d{3,4}-\d{4}', text)
	return phone_numbers


def chosung_extractor(text):
	# chosungs = ', '.join(re.findall(r'([ㄱ-ㅎ]+)', text))
	chosungs = re.findall(r'([ㄱ-ㅎ]+)', text)
	return chosungs




# def address_detector(text):
# 	pattern = r'\w+[구,시,군,도]\s*(\w+[구,시]\s*)(\w+[면,읍]\s*)?(\w+\d*\w*[동,리,로,길]\s*)(\w*\d+-?\d*)?'
# 	address_w = re.compile(pattern)
# 	address = address_w.findall(text)
# 	return address
#
#
# def address_extractor(datalist):
# 	address = list(map(lambda x: address_detector(x), datalist))
# 	return address



##########################################################################################################################################################
# NORMALIZE
##########################################################################################################################################################


def eosMarkerWithKss(text, sep=''):
	# print('# korean sentence splitter #')
	sentence_list = kss.split_sentences(text)
	text = ' '.join([sent + '. ' + sep if not re.findall('[.!?:;)\]}]+', sent[-1]) else sent + ' ' + sep for sent in sentence_list])
	sentenceNum = len(sentence_list)
	return text, sentenceNum


def eosMarkerWithKssIter(data_list, interval, sep=''):
	print('# siakr_common.eosMarkerWithKssIter #')
	print('  - total number of lines:', len(data_list))
	t0 = time.time()
	loop_num = math.ceil(len(data_list) / interval)
	sum_text_list = []
	sum_num_list = []
	print('  - interval:', interval)
	print('  - number of loops', loop_num)
	for l in range(loop_num):
		sliced_list = data_list[l * interval:(l + 1) * interval]
		[sliced_text_list, sliced_num_list] = list(zip(*[[a, b] for (a, b) in list(map(lambda x: eosMarkerWithKss(x, sep), sliced_list))]))
		sum_text_list = sum_text_list + list(sliced_text_list)
		sum_num_list = sum_num_list + list(sliced_num_list)
		hours, rem = divmod(time.time() - t0, 3600)
		minutes, seconds = divmod(rem, 60)
		progress = (len(sum_num_list) / len(data_list)) * 100
		print('  - loop' + str(l + 1) + ', progress', "%0.2f%%" % progress, "  {:0>2}:{:0>2}:{:05.2f}".format(int(hours), int(minutes), seconds))
	print('  - completed')
	print('\n')
	return sum_text_list, sum_num_list


def eomiFix(sentence, errata):
	"""
	1) Fix eomi like '(했어)여, 염, 욤, 용', '습니당', '입니당'
	2) Fix spacing after end of sentence (정규식에서 인식한 것만 처리됨)

	:param sentence:
	:param errata: dataframe with 5 columns: type, no, error, fix, break_point
	:return: sentence with fixed eomi
	"""
	tokens = sentence.split()

	type_list = errata['type'].tolist()
	no_list = errata['no'].tolist()
	error_list = errata['error'].tolist()
	fix_list = errata['fix'].tolist()
	bp_list = errata['break_point'].tolist()  # break_point, last group of first part.

	fixPair = list(zip(type_list, no_list, error_list, fix_list, bp_list))

	fixed_sentence_list = []
	tokenTemp = []
	for token in tokens:
		for type, no, wrong, right, bp in fixPair:
			if re.findall(wrong, token):
				# print('Found#', count)
				tempX = re.findall(wrong, token)[0]
				# print('re.findall', tempX)
				# if len(tempX[-1]) > 0 and len(tempX[-2]) == 0:
					# print('has tail')
				# print('Error#', no)
				# print('BP', bp)
				bp = int(bp)
				# print('Type', type)
				# print('Raw:', token)

				# 바꿔야 할 글자와 새로운 글자가 모두 주어진 경우
				if type == 1:
					# break_point, last group of first part.
					group1end = bp  # "bp + 1"은 (여, 염, 욤, 용)이 있는 자리
					group2start = bp + 2
					# 글자를 지우는 경우(바꿀 글자가 "" 빈칸)
					if isinstance(right, float): right = ''
				# 띄어쓰기만 수정하는 경우
				elif type == 2:
					right = ''
					group1end = bp
					group2start = bp + 1

				# 끝에서 두번째 항목에 마침표, 물음표 등이 들어있는지 확인. regex 파일을 만들 때 끝에서 두번째가 문장부호가 오도록 해야 함.
				if len(tempX[-2]) > 0 and tempX[-2][0] in ['.', '?', '!', ':', ')', ']']:  # 문장부호가 있으면 bp번호 항목(요, 염, 여, 용)를 right 값으로 바꾸기
					fix = ''.join(tempX[:group1end + 1]) + right + ''.join(tempX[group2start:])
					# print('opt1')
				# 문장부호가 없는 경우, 마침표 넣어주기
				else:
					fix = ''.join(tempX[:group1end + 1]) + right + '. ' + ''.join(tempX[group2start:])
					# print('opt2')
				fix = re.sub('(.*)([.?!]+)(\S+)', '\\1\\2 \\3', fix)
				# print('Fix:', fix)
				token = fix
		tokenTemp.append(token.strip())
	fixed_sentence = ' '.join(tokenTemp)
	return fixed_sentence


def eomiFixIter(sentence_list, errata, interval=100000):
	print('# siakr_common.eomiFixIter #')
	print('  - total number of lines:', len(sentence_list))
	t0 = time.time()
	loop_num = math.ceil(len(sentence_list) / interval)
	sliced_list = []
	sum_list = []
	print('  - interval:', interval)
	print('  - number of loops', loop_num)
	for l in range(loop_num):
		sliced_list = sentence_list[l * interval:(l + 1) * interval]
		sliced_list = list(map(lambda x: eomiFix(x, errata), sliced_list))
		sum_list = sum_list + list(sliced_list)
		hours, rem = divmod(time.time() - t0, 3600)
		minutes, seconds = divmod(rem, 60)
		progress = (len(sum_list) / len(sentence_list)) * 100
		print('  - loop' + str(l + 1) + ', progress', "%0.2f%%" % progress, "  {:0>2}:{:0>2}:{:05.2f}".format(int(hours), int(minutes), seconds))
	print('  - completed')
	print('\n')
	return sum_list


def hashtagSpacing(text):
	text = text.replace('#', ' #').replace('  #', ' #').replace('# #', '#')
	text = re.sub(r'(#[0-9ㄱ-ㅎ가-힣a-zA-Z\\u3040-\\u309F\\u30A0-\\u30FF\\u31F0-\\u31FF\\u4E00-\\u9FFF\-\_]+)', '\\1 ', text)
	return text


def hashtagSpacingIter(sentence_list, interval=100000):
	print('# siakr_common.hashtagSpacingIter #')
	print('  - total number of lines:', len(sentence_list))
	t0 = time.time()
	loop_num = math.ceil(len(sentence_list) / interval)
	sliced_list = []
	sum_list = []
	print('  - interval:', interval)
	print('  - number of loops', loop_num)
	for l in range(loop_num):
		sliced_list = sentence_list[l * interval:(l + 1) * interval]
		sliced_list = list(map(lambda x: hashtagSpacing(x), sliced_list))
		sum_list = sum_list + list(sliced_list)
		hours, rem = divmod(time.time() - t0, 3600)
		minutes, seconds = divmod(rem, 60)
		progress = (len(sum_list) / len(sentence_list)) * 100
		print('  - loop' + str(l + 1) + ', progress', "%0.2f%%" % progress, "  {:0>2}:{:0>2}:{:05.2f}".format(int(hours), int(minutes), seconds))
	print('  - completed')
	print('\n')
	return sum_list


def KrTextNormalizer(text):
	text = text.replace('"', "'").replace(';', ':').replace('\n', ' ') # Mark new line (New Line Return)
	text = text.replace('"', "'").replace(';', ':')
	text = re.sub(r'([ㄱ-ㅎ]+)(\s+)([ㄱ-ㅎ]+)', '\\1\\3', text)
	text = re.sub(r'([0-9]+)(,)([0-9]+)', '\\1\\3', text)
	text = re.sub(r'([ㄱ-ㅎ]+)', ' \\1 ', text)
	text = ' '.join(text.split())
	text = ' '.join(re.findall('[0-9ㄱ-ㅎ가-힣a-zA-Z\\u3040-\\u309F\\u30A0-\\u30FF\\u31F0-\\u31FF\\u4E00-\\u9FFF\!\@#\$\%\^&\*\(\)\_\+\-\{\}\[\]<>?\.\/\\;\:\=\|\\U0001F1E0-\\U0001F1FF\\U0001F300-\\U0001F5FF\\U0001F600-\\U0001F64F\\U0001F680-\\U0001F6FF\\U0001F700-\\U0001F77F\\U0001F780-\\U0001F7FF\\U0001F800-\\U0001F8FF\\U0001F900-\\U0001F9FF\\U0001FA00-\\U0001FA6F\\U0001FA70-\\U0001FAFF\\U00002702-\\U000027B0]+', text))

	# Unicode Emoji blocks
	# "\U0001F1E0-\U0001F1FF"  # flags (iOS)
	# "\U0001F300-\U0001F5FF"  # symbols & pictographs
	# "\U0001F600-\U0001F64F"  # emoticons
	# "\U0001F680-\U0001F6FF"  # transport & map symbols
	# "\U0001F700-\U0001F77F"  # alchemical symbols
	# "\U0001F780-\U0001F7FF"  # Geometric Shapes Extended
	# "\U0001F800-\U0001F8FF"  # Supplemental Arrows-C
	# "\U0001F900-\U0001F9FF"  # Supplemental Symbols and Pictographs
	# "\U0001FA00-\U0001FA6F"  # Chess Symbols
	# "\U0001FA70-\U0001FAFF"  # Symbols and Pictographs Extended-A
	# "\U00002702-\U000027B0"  # Dingbats
	return text


def KrTextNormalizerIter(sentence_list, interval=100000):
	print('# siakr_common.KrTextNormalizerIter #')
	print('  - total number of lines:', len(sentence_list))
	t0 = time.time()
	loop_num = math.ceil(len(sentence_list) / interval)
	sliced_list = []
	sum_list = []
	print('  - interval:', interval)
	print('  - number of loops', loop_num)
	for l in range(loop_num):
		sliced_list = sentence_list[l * interval:(l + 1) * interval]
		sliced_list = list(map(lambda x: KrTextNormalizer(x), sliced_list))
		sum_list = sum_list + list(sliced_list)
		hours, rem = divmod(time.time() - t0, 3600)
		minutes, seconds = divmod(rem, 60)
		progress = (len(sum_list) / len(sentence_list)) * 100
		print('  - loop' + str(l + 1) + ', progress', "%0.2f%%" % progress, "  {:0>2}:{:0>2}:{:05.2f}".format(int(hours), int(minutes), seconds))
	print('  - completed')
	print('\n')
	return sum_list


def KrSpacerText(sentence, custom_device, custom_vocab):
	spacer = ChatSpace(device=custom_device)
	sentence = spacer.space(sentence, custom_vocab=custom_vocab, batch_size=64)
	return sentence


def KrSpacerTextIter(sentence_list, custom_device, custom_vocab, interval=100000):
	print('# siakr_common.KrSpacerTextIter #')
	print('  - total number of lines:', len(sentence_list))
	t0 = time.time()
	loop_num = math.ceil(len(sentence_list) / interval)

	spacer = ChatSpace(device=custom_device)
	sum_list = []
	print('  - interval:', interval)
	print('  - number of loops', loop_num)
	for l in range(loop_num):
		sliced_list = sentence_list[l * interval:(l + 1) * interval]
		sliced_list = spacer.space_iter(sliced_list, custom_vocab=custom_vocab)
		sum_list = sum_list + list(sliced_list)
		hours, rem = divmod(time.time() - t0, 3600)
		minutes, seconds = divmod(rem, 60)
		progress = (len(sum_list) / len(sentence_list)) * 100
		print('  - loop' + str(l + 1) + ', progress', "%0.2f%%" % progress, "  {:0>2}:{:0>2}:{:05.2f}".format(int(hours), int(minutes), seconds))
	print('  - completed')
	print('\n')
	return sum_list


def KrSpacerTextList(sentence_list, custom_device, custom_vocab):
	print('# siakr_common.KrSpacerList #')
	print('  - total number of lines:', len(sentence_list))
	spacer = ChatSpace(device=custom_device)
	spaced = []
	for spaced_text in spacer.space_iter(sentence_list, custom_vocab=custom_vocab):
		spaced.append(spaced_text)
	print('  - completed')
	print('\n')
	return spaced

def doubleSpacetoSpace(text):
	text = ' '.join(text.split())
	return text

def ReadDictInText(path):
	file = open(path, 'r')
	contents = file.read()
	dict = ast.literal_eval(contents)
	file.close()
	return dict

def SpacingFix(text, errata):
	no_list = errata['no'].tolist()
	error_list = errata['word'].tolist()
	fix_list = errata['fix'].tolist()
	for i, n in enumerate(no_list):
		text = re.sub(error_list[i], fix_list[i], text)
	text = ' '.join(text.split())
	return text

def SpacingFixIter(sentence_list, errata, interval=100000):
	print('# siakr_common.SpacingFixIter #')
	print('  - total number of lines:', len(sentence_list))
	t0 = time.time()
	loop_num = math.ceil(len(sentence_list) / interval)
	sliced_list = []
	sum_list = []
	print('  - interval:', interval)
	print('  - number of loops', loop_num)
	for l in range(loop_num):
		sliced_list = sentence_list[l * interval:(l + 1) * interval]
		sliced_list = list(map(lambda x: SpacingFix(x, errata), sliced_list))
		sum_list = sum_list + list(sliced_list)
		hours, rem = divmod(time.time() - t0, 3600)
		minutes, seconds = divmod(rem, 60)
		progress = (len(sum_list) / len(sentence_list)) * 100
		print('  - loop' + str(l + 1) + ', progress', "%0.2f%%" % progress, "  {:0>2}:{:0>2}:{:05.2f}".format(int(hours), int(minutes), seconds))
	print('  - completed')
	print('\n')
	return sum_list
