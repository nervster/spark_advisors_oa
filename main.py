import pandas as pd

DATA_DIRECTORY = './data/'

class SLCSP:
    def __init__(self):
        self.zips_df = pd.DataFrame()
        self.plans_df = pd.DataFrame()
        self.slcsp_df = pd.DataFrame()
        self.results_df = pd.DataFrame()

    def read_csvs(self):
        self.zips_df = pd.read_csv(DATA_DIRECTORY + 'zips.csv', dtype={'zipcode': str})
        self.plans_df = pd.read_csv(DATA_DIRECTORY + 'plans.csv')
        self.slcsp_df = pd.read_csv(DATA_DIRECTORY + 'slcsp.csv', dtype={'zipcode': str})
    
    def clean_up(self):
        # Filter only Silver plans and Zipcodes within slcsp_df
        self.plans_df = self.plans_df[self.plans_df['metal_level'] == 'Silver']
        self.zips_df = self.zips_df[self.zips_df['zipcode'].isin(self.slcsp_df['zipcode'])]

        # Merge and rank the plans
        self.merged_df = pd.merge(self.zips_df, self.plans_df, on=['state', 'rate_area'])
        self.merged_df['rank'] = self.merged_df.groupby(['state', 'rate_area'])['rate'].rank(ascending=True, method='dense')

        # Filter by rank and drop duplicates
        self.merged_df = self.merged_df[['state', 'rate_area', 'rate', 'rank', 'zipcode']].loc[self.merged_df['rank']== 2.0].drop_duplicates()
        
        # Keep only rows where 'zipcode' is in one rate area
        counts = self.merged_df.groupby(['zipcode']).apply(lambda x: x[['rate_area', 'state']].nunique())
        self.results_df = self.merged_df[self.merged_df['zipcode'].isin(counts[(counts['rate_area'] == 1) & (counts['state'] == 1)].index)]
        

    def merge_slcsp_with_results(self):
        self.results_df = pd.merge(self.slcsp_df['zipcode'], self.results_df, on='zipcode', how='left')
        self.results_df = self.results_df[['zipcode', 'rate']]


    def output_results(self):
        print(self.results_df.to_csv(index=False, float_format='%.2f'))


if __name__ == "__main__":
    slcsps = SLCSP()
    slcsps.read_csvs()
    slcsps.clean_up()
    slcsps.merge_slcsp_with_results()
    slcsps.output_results()