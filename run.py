from pathlib import Path
import pandas as pd

def load_data(file_name: str) -> pd.DataFrame:
    """Loads data from a CSV file into a Pandas DataFrame.

    Args:
        file_name: The name of the CSV file to load.

    Returns:
        A Pandas DataFrame containing the data from the CSV file.
    """
    # Get the current file path
    current_file_path = Path(__file__).resolve()

    # Get the root path
    root_path = current_file_path.parent

    # Define the data folder path
    data_folder_path = root_path / 'data'

    # Load the data from the CSV file
    return pd.read_csv(data_folder_path / file_name)

def create_account_statement(dataframe: pd.DataFrame, platform_name: str) -> pd.DataFrame:
    """Creates an account statement DataFrame for a given platform.

    Args:
        dataframe: The source DataFrame with transaction data.
        platform_name: The name of the trading platform (e.g., 'OKX').

    Returns:
        A Pandas DataFrame formatted as an account statement.
    """
    account_statement_columns = [
        "trade_number", "date", "platform", "transaction_type", "amount",
        "price", "currency", "transfer_method", "order_invoice_number", "platform_fee"
    ]

    # Initialize an empty DataFrame with specified columns
    account_statement = pd.DataFrame(columns=account_statement_columns)

    if platform_name == 'OKX':
        # Map the columns from the OKX dataframe to the account statement DataFrame
        account_statement['date'] = dataframe['Created date']
        account_statement['transaction_type'] = dataframe['Order type']
        account_statement['amount'] = dataframe['Volume']
        account_statement['price'] = dataframe['Amount']/dataframe['Volume']
        account_statement['currency'] = dataframe['Currency']
        account_statement['transfer_method'] = dataframe['Payment method'].apply(
            lambda x: 'SEPA' if x == 'Bank Transfer' else x
        )
        account_statement['platform'] = platform_name
        account_statement['platform_fee'] = 0
        account_statement['order_invoice_number'] = dataframe['Order No']

    # Additional conditions for other platforms can be added here
    elif platform_name == 'Binance':
        # Map the columns from the Binance dataframe to the account statement DataFrame
        account_statement['date'] = dataframe['Created Time']
        account_statement['transaction_type'] = dataframe['Order Type']
        account_statement['amount'] = dataframe['Quantity']
        account_statement['price'] = dataframe['Price']
        account_statement['currency'] = dataframe['Fiat Type']
        # Set 'Transfer Method' based on 'currency'
        account_statement['transfer_method'] = dataframe['Fiat Type'].apply(
            lambda x: 'SEPA' if x == 'EUR' else 'ZEN'
        )
        account_statement['platform'] = platform_name
        account_statement['platform_fee'] = dataframe['Fee Amount']
        account_statement['order_invoice_number'] = dataframe['Order Number']
    return account_statement



def export_account_statement_to_csv(account_statement: pd.DataFrame, filename: str = 'combined_account_statement.csv') -> None:
    """Exports the account statement DataFrame to a CSV file in the root path.

    Args:
        account_statement: The DataFrame containing the account statements.
        root_path: The root path of the project where the CSV file will be saved.
        filename: The name of the CSV file. Defaults to 'combined_account_statement.csv'.
    """
    # Get the current file path
    current_file_path = Path(__file__).resolve()

    # Get the root path
    root_path = current_file_path.parent
    # Define the full path for the output file
    output_file_path = root_path / filename

    # Export the account statement DataFrame to a CSV file
    account_statement.to_csv(output_file_path, index=False)

    print(f"Account statement saved to {output_file_path}")


# Load data for each platform
okx_data = load_data('OKX_account_statement.csv')
okx_data = okx_data[okx_data['Status']=='Fulfilled']
binance_data = load_data('Binance_account_statement.csv')
binance_data = binance_data.loc[binance_data['Status'].isin(['Completed', 'Paid'])]
# binance_data = load_data('Binance_account_statement.csv')  # Uncomment and use the correct filename for Binance

# Create account statements for each platform
okx_account_statement = create_account_statement(okx_data, 'OKX')

binance_account_statement = create_account_statement(binance_data, 'Binance')

account_statement = pd.concat([okx_account_statement, binance_account_statement], axis = 0)

# Convert the 'date' column to datetime format
account_statement['date'] = pd.to_datetime(account_statement['date'])

# Sort the DataFrame by the 'date' column in ascending order
account_statement_sorted = account_statement.sort_values('date', ascending=True)

export_account_statement_to_csv(account_statement, 'account_statement.csv')

