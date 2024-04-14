# -*- coding: utf-8 -*-
"""
Created on Tue Oct 31 21:44:20 2023

@author: Manu Bansal
"""


# f9 to run selected lines in interpreter

import refinitiv.data as rd
import pandas as pd
import eikon as ek
import csv

rd.open_session()
ek.set_app_key('9e6907889b3a4fc2b971665d1800f0d7020545ee20')


# defining parameters
arr_parameters_l = [
     'TR.CompanyMarketCapitalization',
     'TR.H.EV',
     'TR.F.TotRevenue',
     'TR.F.TotAssets',
     'TR.F.TotCurrLiab',
     'TR.F.OpExpnTot',
     'TR.F.InvntTot',
     'TR.F.PPENetTot',
     'TR.F.IntangAccumAmortTot',
     'TR.F.DeprTot',
     'TR.F.NetCashEndBal',
     'TR.F.LoansRcvblTot',
     'TR.F.CashSTInvst',
     'TR.F.NetCashFlowInvst',
     'TR.F.NetCashFlowOp',
     'TR.F.CostOfOpRev',
     'TR.F.RcvblTot',
     'TR.F.PremEarnedTot',
     'TR.F.DebtLTTot',
     'TR.F.STDebtNotesPble',
     'TR.F.InvstLT',
     'TR.F.STInvstTot',
     'TR.F.CashSTDeposDueBanksTot',
     'TR.F.PbleAccrExpn',
     'TR.F.NetIncAfterTax',
     'TR.F.IncBefTax',
     'TR.F.IncTax',
     'TR.F.ComShrIssuedTot',
     'TR.F.TotDebttoComEq',
     'TR.F.IncBefTaxtoEBIT',
     'TR.F.EBITMargPct',
     'TR.H.PriceToSalesPerShare',
     'TR.H.PriceToCFPerShare',
     'TR.H.PriceToBVPerShare',
     'TR.H.PriceToTangBVPerShare',
]

df, err = ek.get_data(
    instruments = ['.SPX'],
    fields = ['TR.IndexConstituentRIC']
)

list_universe_sp500 = df['Constituent RIC']
#display(df)

start_date = "2000-01-01"
end_date = "2023-01-01"



# request  data matrix (500c x 60p x 20y) from refinitiv
df_raw_xl = rd.get_history(
        universe= list(list_universe_sp500), 
        fields=arr_parameters_l,
        interval="1Y",
        start= start_date,
        end=end_date,
    )

print(df_raw_xl)

#############################

# saving raw data to csv
df_raw_xl.to_csv("D:\EBS Universität\Master Thesis\Master Thesis\python scripts\data\dftest_raw_xl.csv")
df_raw_xl = pd.read_csv("D:\EBS Universität\Master Thesis\Master Thesis\python scripts\data\dftest_raw_xl.csv", header=[0,1], index_col=0, parse_dates=True)


############################### cleaning raw data ## redundant

# dftest = df_raw_xl.resample("Y").max()
# # check NAs and non NAs
df_raw_xl.isna().sum().sum()
df_raw_xl.notna().sum().sum()


################################    aggregation + cleaning
# resampling needed for multiple entries per year 

df_raw_resampled_y = df_raw_xl.resample("Y").max()
print("number of NAs: ", df_raw_resampled_y.isna().sum().sum())
print("number not NAs: ", df_raw_resampled_y.notna().sum().sum())


#############################

# saving raw data to csv
df_raw_resampled_y.to_csv("D:\EBS Universität\Master Thesis\Master Thesis\python scripts\data\dftest_xl.csv")

df_raw_resampled_y = pd.read_csv("D:\EBS Universität\Master Thesis\Master Thesis\python scripts\data\dftest_xl.csv", header=[0,1], index_col=0)
df_raw_resampled_y.index = pd.to_datetime(df_raw_resampled_y.index)

#################################### data preporcessing complete
#################################### data transformation start

########################  df diff

df_diff = df_raw_resampled_y > df_raw_resampled_y.shift(1)
df_diff = df_diff[2:]
df_diff = df_diff.astype(str)
df_diff = df_diff .replace("True", 1)
df_diff = df_diff .replace("False", -1)
df_diff = df_diff.replace('<NA>', None)

print(df_diff)

print("number not NAs: ", df_diff.notna().sum().sum())
print("number of NAs: ", df_diff.isna().sum().sum())        ## must be 0


#############################   parameter arrays from dataframe

dict_frmt = {}

list_universe=[]
[list_universe.append(x) for x in df_diff.columns.get_level_values(0) if x not in list_universe]

list_params=[]
[list_params.append(x) for x in df_diff.columns.get_level_values(1) if x not in list_params]

list_years=[]
[list_years.append(x) for x in sorted(df_diff.index.get_level_values(0)) if x not in list_years]
   

############################# df frmt2 #######   GOD VIEW

df_frmt2=[]
dict_frmt={}
for company in list_universe:
    this_year_dict={}
    for year in sorted(list_years):
        
        this_param_dict={}
        for param in list_params:
            try:
                this_value = df_diff[company, param][year]               
            except:
                this_value = "0" ## was #
            this_param_dict[param] = this_value
        this_year_dict[year.year] = this_param_dict
    dict_frmt[company] = this_year_dict
    
df = pd.DataFrame({(i, j): dict_frmt[i][j] for i in dict_frmt.keys() for j in dict_frmt[i].keys()}).T
df.index = pd.MultiIndex.from_tuples(df.index, names=['Company', 'Year'])

## number of nas cells
print("number not NAs: ", df.notna().sum().sum())
print("number of NAs: ", df.isna().sum().sum())

# number of nas in columns
print("number not NAs: ", df.notna().sum())
print("number of NAs: ", df.isna().sum())

##############################################
#################################    cleaning 

# drop all records where Company Market Capitalisation is not available
df.dropna(subset=['Company Market Capitalization'], inplace=True)
df.count().sum()

# replace all NAs with 0
df.fillna(0, inplace=True)


# Create a boolean mask to identify rows where more than 50% of columns have 0
mask = (df == 0).sum(axis=1) > 0.5 * df.shape[1]

print(df[mask]) # should be empty df
# Use the boolean mask to filter the rows and drop them from 'df'
df = df[~mask]

df=df.astype(int)

df.to_csv("D:\EBS Universität\Master Thesis\Master Thesis\python scripts\data\df_frmt3.csv")
df= pd.read_csv("D:\EBS Universität\Master Thesis\Master Thesis\python scripts\data\df_frmt3.csv", index_col=[0,1])

################# restructuring df to final format
df_final = pd.DataFrame()
df_final['input'] = df.apply(lambda row: ', '.join(row.iloc[:-1].astype(str)), axis=1)
df_final['output'] = df.apply(lambda row: str(row.iloc[-1]), axis=1)

prompt_string = " This array contains the change in a certain company's 33 unique financial parameters observed in an undisclosed year. Taking -1 as decrease from the previous year, 1 as increase and 0 as unavailable, estimate how the company's valuation will change in the same year. Give your answer only as either -1 or 1. "



########################### slice df into training and testing sets based on years
Training_year_list = list(range(2001, 2018))
Testing_year_list = [2018, 2019]


df_instruct = df_final
df_instruct = df_final.assign(Concatenated_Column=df_final['input'] + df_final['output'])


df_instruct['Concatenated_Column'] = df_instruct.apply(lambda row: 
                                                       
                                                       "### instruction: " +
                                                       prompt_string +
                                                       "### input: [" +
                                                       row['input'] +
                                                       "] ### output: " +
                                                       row['output']                                                       
                                                , axis=1)

    

df_instruct.to_csv("D:\EBS Universität\Master Thesis\Master Thesis\python scripts\data\df_instruct.csv", index=False)
df_instruct = pd.read_csv("D:\EBS Universität\Master Thesis\Master Thesis\python scripts\data\df_instruct.csv")#, header=[0,1], index_col=0)


df_training_set = df_instruct.loc[(slice(None), Training_year_list), :]
df_testing_set  = df_instruct.loc[(slice(None), Testing_year_list), :]

###3   converting training and testing dfs to final format and save to csv
df_training_set.to_csv("D:\EBS Universität\Master Thesis\Master Thesis\python scripts\data\df_training_final.csv", index=False)
df_testing_set.to_csv("D:\EBS Universität\Master Thesis\Master Thesis\python scripts\data\df_testing_final.csv", index=False)
df_instruct.to_csv("D:\EBS Universität\Master Thesis\Master Thesis\python scripts\data\df_final.csv", index=False)

df_training_set = pd.read_csv("D:\EBS Universität\Master Thesis\Master Thesis\python scripts\data\df_training_final.csv")#, header=[0,1], index_col=0)



### stats
def count_tokens(text):
    # Split the text into words using whitespace as the delimiter
    words = text.split()
    return len(words)

# Count tokens in the entire DataFrame
total_token_count = sum(df_training_set['text'].apply(count_tokens))

# Display the total token count
print("Total Token Count:", total_token_count)



# Create a new DataFrame to store token counts
token_counts_df = pd.DataFrame()

# Iterate through columns and count tokens
for column in df_training_set.columns:
    token_counts_df[column] = df_training_set[column].apply(count_tokens)





