#EXERCICE 2 - DATA CLEANING + ETL

#IMPORT MODULES
import time
import numpy as np
import pandas as pd
import functions_ETL_pizzasPrediction


def df_to_xml(df):
    '''
    Toma un dataframe y devuelve un string de tipología xml

    '''

    xml = df.to_xml()
    return xml





def load_to_xml(file_name, xml_text):
    Register = open(str(file_name), 'w')


    Register.write(xml_text)
    Register.close()

    time.sleep(1)
    print(f'Data loaded to {file_name}')
    
    
    
    

if __name__ == '__main__':
    
    #EXTRACT
    dfs = functions_ETL_pizzasPrediction.extract()
    
    #DATA QUALITY
    print('DATA QUALITY')
    #print('Number of null values per field:\n')
    df_quality = functions_ETL_pizzasPrediction.data_quality([df for df in dfs])
    print(df_quality)
    print()

    
    #TRANSFORM
    dfs = functions_ETL_pizzasPrediction.data_cleaning(dfs[1], dfs[2], dfs[3], dfs[4])
    df_adquirements = functions_ETL_pizzasPrediction.transform(dfs[0], dfs[1], dfs[2], dfs[3])
    
    #lOAD
    dic = {'DataQuality.xml':df_quality, 'IngredientAdquirements.xml':df_adquirements}
    for key in dic.keys():
        xml_content = df_to_xml(dic[key])
        load_to_xml(key, xml_content)
