import dask, mimesis, random, click
from pathlib import Path
from typing import Optional
import pandas as pd

def generate_fake_df(
    num_cols: int = 20,
    num_rows: int = 1000,
    random_seed: int = 42,
    save_path: Optional[Path] = None
) -> pd.DataFrame:
    '''
    Generate a random data set as a pandas dataframe.

    :param num_cols: number of columns
    :type num_cols: int
    :default num_cols: 20

    :param num_rows: number of rows
    :type num_rows: int
    :default num_rows: 1000

    :param random_seed: seed for randomization
    :type random_seed: int
    :default random_seed: 42

    :param save_path: file path to save .csv
    :type save_path: str
    :default save_path: None

    :return: pandas DataFrame
    '''
    random.seed(random_seed)

    classes = [
        'Address',
        'CardType',
        'CountryCode',
        'Cryptographic',
        'Datetime',
        'Finance',
        'Food',
        'Gender',
        'Hardware',
        'Internet',
        'Payment',
        'Person',
        'Science'
    ]

    excluded_methods = [
        'Meta',
        'seed',
        'reseed',
        'locale'
    ]

    classes_and_methods = {}
    while num_cols:
        try:
            rand_class_instance = getattr(mimesis, random.choice(classes))()
            method_strings = [
                method for method in dir(rand_class_instance)
                if not method.startswith('_')
                and method not in excluded_methods
            ]
            rand_method_string = random.choice(method_strings)
            getattr(rand_class_instance, rand_method_string)() # check
            classes_and_methods[rand_class_instance] = rand_method_string
            num_cols -= 1
        except:
            pass
    
    def generate_fake_col(rand_class_instance, rand_method_string):
        rand_method = getattr(rand_class_instance, rand_method_string)
        return pd.DataFrame({
            rand_method_string: [
                rand_method() for i in range(num_rows)
            ]
        })
    
    tasks = [
        dask.delayed(generate_fake_col)(rand_class_instance, rand_method_string)
        for rand_class_instance, rand_method_string in classes_and_methods.items()
    ]

    tasks_list = dask.delayed()(tasks)
    results = tasks_list.compute()

    df = pd.concat(results, axis=1)

    if save_path:
        df.to_csv(Path(save_path), index=False)

    return df

@click.command()
@click.option('--num-cols', default=20, type=int, help='Number of columns')
@click.option('--num-rows', default=1000, type=int, help='Number of rows')
@click.option('--random-seed', default=42, type=int, help='Randomization initializtion point')
@click.option(
    "--save-path",
    type=click.Path(exists=False, file_okay=True, writable=True),
    help='Optional file path at which to save the data'
)
def cli(
    num_cols: int = 20,
    num_rows: int = 1000,
    random_seed: int = 42,
    save_path: Optional[Path] = None
):
    generate_fake_df(num_cols, num_rows, random_seed, save_path)

if __name__ == '__main__':
    cli()