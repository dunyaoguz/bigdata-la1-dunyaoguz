import dask.bag as db
import dask.dataframe as df

dd1 = df.read_csv("./data/frenepublicinjection2016.csv", dtype={'Nom_parc': 'object',
                                                                'No_Civiq':'object'})
dd2 = df.read_csv("./data/frenepublicinjection2015.csv", dtype={'Nom_parc': 'object',
                                                                'No_Civiq':'object'})

dd1_parks = dd1[['Nom_parc']].drop_duplicates()
dd2_parks = dd2[['Nom_parc']].drop_duplicates()
inter = df.merge(dd1_parks, dd2_parks, on=['Nom_parc']) \
    .dropna() \
    .sort_values(by='Nom_parc') \
    .compute()

print(inter)