"""
This file is for getting highest probable address of an account provided it accounts.csv and contacts.csv
"""
import re

__author__ = "Talha Saqib"

# Local imports
import configurator as conf
import collections
import operator

# Third-party imports
import pandas as pd

# Global variables
config = conf.Configurator()
config.set_warnings_off()
config.set_pandas_display()
logger = config.set_logger()


def get_frequencies_of_address_groups_df(addr_df, fields):
    try:
        # Filling null rows
        addr_df = addr_df[fields].fillna('-')

        # Uppercasing all addresses
        addr_df[fields[1]] = map(lambda x: str(x).upper(), addr_df[fields[1]])
        addr_df[fields[2]] = map(lambda x: str(x).upper(), addr_df[fields[2]])
        addr_df[fields[3]] = map(lambda x: str(x).upper(), addr_df[fields[3]])

        if (len(addr_df)) > 0:
            # 'Id' is the default count column name which is being manually renamed
            frequencies_of_address_groups_df = \
                addr_df.groupby([fields[1], fields[2], fields[3]])["Id"] \
                    .count() \
                    .reset_index(name="count") \
                    .sort_values("count", ascending=False)

            # make a dictionary here
            if not frequencies_of_address_groups_df.empty:

                # Check if top frequency row is all dashes
                df_test = frequencies_of_address_groups_df.head(1)
                if df_test[fields[1]].item() == "-" and df_test[fields[2]].item() == "-" and df_test[
                    fields[3]].item() == "-":
                    return None, None, None

                df = pd.DataFrame()
                df['keys'] = \
                    '(State:' + frequencies_of_address_groups_df[fields[2]] + ')' \
                                                                              ' - (Country:' + \
                    frequencies_of_address_groups_df[fields[3]] + ') - (City:' \
                    + frequencies_of_address_groups_df[fields[1]]+')'
                df['counts'] = frequencies_of_address_groups_df['count'].astype(int)

                df.set_index('keys', inplace=True)
                dictionary = df.to_dict()['counts']

                # sorting
                sorted_list_of_tuples = sorted(dictionary.items(), key=operator.itemgetter(1), reverse=True)
                sorted_dict = collections.OrderedDict(sorted_list_of_tuples)

                return frequencies_of_address_groups_df, dictionary, sorted_dict
            else:
                return None, None, None
        else:
            return None, None, None

    except Exception as e:
        logger.error(e)


def get_output_for_addresses(df, fields, source, max_freq, contacts_count, id, c1, c2, c3, s1, s2, s3):
    highest_probable_country = df[fields[3]].head(1).item()

    state_df = df[(df[fields[3]] == highest_probable_country) & (df[fields[2]] != '-')]
    if not state_df.empty:
        highest_probable_state = state_df[fields[2]].head(1).item()
    else:
        highest_probable_state = "-"

    city_df = df[(df[fields[3]] == highest_probable_country) & (df[fields[2]] == highest_probable_state) & (df[fields[1]] != '-')]
    if not city_df.empty:
        highest_probable_city = city_df[fields[1]].head(1).item()
    else:
        highest_probable_city = "-"

    # removing bad states
    highest_probable_country = re.sub(r"[0-9?\-.,/@#$%^&*()+={}\[\]:;<>|~]", "", highest_probable_country)
    highest_probable_state = re.sub(r"[0-9?\-.,/@#$%^&*()+={}\[\]:;<>|~]", "", highest_probable_state)
    highest_probable_city = re.sub(r"[0-9?\-.,/@#$%^&*()+={}\[\]:;<>|~]", "", highest_probable_city)


    output = {"Id": id,
              "highest_probable_city": highest_probable_city,
              "highest_probable_state": highest_probable_state,
              "highest_probable_country": highest_probable_country,
              "highest_probable_source": source,
              "highest_probable_frequency": max_freq,
              "contacts_count": contacts_count,
              "Contact(StandardState-StandardCountry) Concatenated Frequencies": c1,
              "Contact(MailingState-MailingCountry) Concatenated Frequencies": c2,
              "Account(BillingState-BillingCountry) Concatenated Frequencies": c3,
              "Contact(StandardState-StandardCountry) Sorted Frequencies": s1,
              "Contact(MailingState-MailingCountry) Sorted Frequencies": s2,
              "Account(BillingState-BillingCountry) Sorted Frequencies": s3
              }

    return output


def get_highest_probable_address_of_contacts(accounts_csv_path, contacts_csv_path):
    try:
        accounts_df = pd.read_csv(accounts_csv_path)
        # accounts_df = accounts_df[accounts_df['Owner_Name__c'] != 'AccountInventory']
        contacts_df = pd.read_csv(contacts_csv_path)

        c = 0
        for index, row in accounts_df.iterrows():
            output_df = pd.DataFrame()
            account_id = row["Id"]

            min_freq_flag = 0
            not_none_flag = 0
            got_output_flag = 0

            c += 1
            account_contacts_df = contacts_df[contacts_df["AccountId"] == account_id]
            contacts_count = len(account_contacts_df)

            # Checking Standard Addresses
            fields1 = ['Id', 'City__c', 'State__c', 'Country__c']
            contacts_standard_addr_df = account_contacts_df[fields1]
            frequencies_of_standard_address_groups_df, concat_freq_stand, sorted_freq_stand = \
                get_frequencies_of_address_groups_df(contacts_standard_addr_df, fields1)

            # Checking Mailing Addresses
            fields2 = ['Id', 'MailingCity', 'MailingState', 'MailingCountry']
            contacts_mailing_addr_df = account_contacts_df[fields2]
            frequencies_of_mailing_address_groups_df, concat_freq_mail, sorted_freq_mail = \
                get_frequencies_of_address_groups_df(contacts_mailing_addr_df, fields2)

            # Checking Billing Addresses
            fields3 = ['Id', 'BillingCity', 'BillingState', 'BillingCountry']
            # Getting cols from another dataframe
            temp_df = accounts_df[fields3]
            accounts_billing_addr_df = temp_df[temp_df['Id'] == account_id]
            # Now getting frequencies
            frequencies_of_billing_address_groups_df, concat_freq_bill, sorted_freq_bill = \
                get_frequencies_of_address_groups_df(
                    accounts_billing_addr_df,
                    fields3)

            # Checking Empty Output
            if frequencies_of_standard_address_groups_df is not None:
                not_none_flag = 1

                highest_probable_freq = frequencies_of_standard_address_groups_df["count"].head(1).item()
                if highest_probable_freq <= 1:
                    min_freq_flag = 1
                else:
                    got_output_flag = 1

            if not_none_flag == 0 or min_freq_flag:
                min_freq_flag = 0
                not_none_flag = 0

                # Checking Empty Output for mailing addresses
                if frequencies_of_mailing_address_groups_df is not None:
                    not_none_flag = 1

                    highest_probable_freq = frequencies_of_mailing_address_groups_df["count"].head(1).item()
                    if highest_probable_freq <= 1:
                        min_freq_flag = 1
                    else:
                        got_output_flag = 2

                if not_none_flag == 0 or min_freq_flag:

                    # Checking Empty Output for billing addresses
                    if frequencies_of_billing_address_groups_df is not None:
                        highest_probable_freq = frequencies_of_billing_address_groups_df["count"].head(1).item()

                        if highest_probable_freq > 0:
                            got_output_flag = 3

            if got_output_flag is not 0:

                if got_output_flag == 1:
                    fields = fields1
                    source = "ContactsStandard"
                    freq_df = frequencies_of_standard_address_groups_df

                elif got_output_flag == 2:
                    fields = fields2
                    source = "ContactsMailing"
                    freq_df = frequencies_of_mailing_address_groups_df

                elif got_output_flag == 3:
                    fields = fields3
                    source = "AccountsBilling"
                    freq_df = frequencies_of_billing_address_groups_df

                c1 = concat_freq_stand
                c2 = concat_freq_mail
                c3 = concat_freq_bill
                s1 = sorted_freq_stand
                s2 = sorted_freq_mail
                s3 = sorted_freq_bill

                output_row = get_output_for_addresses(freq_df, fields,
                                                      source, highest_probable_freq,
                                                      contacts_count, account_id, c1, c2, c3,
                                                      s1, s2, s3)
                output_df = output_df.append(output_row, ignore_index=True)
                df_row = pd.merge(accounts_df, output_df, on='Id')
                if c == 1:
                    df_row.to_csv("accounts_highest_probable_addresses_3.csv", mode='w', index=False,
                                  line_terminator='\n')
                else:
                    df_row.to_csv("accounts_highest_probable_addresses_3.csv", mode='a', index=False,
                                  header=False,
                                  line_terminator='\n')
            else:
                output_row = {"Id": account_id,
                          "highest_probable_city": '',
                          "highest_probable_state": '',
                          "highest_probable_country": '',
                          "highest_probable_source": '',
                          "highest_probable_frequency": '',
                          "contacts_count": contacts_count,
                          "Contact(StandardState-StandardCountry) Concatenated Frequencies": '',
                          "Contact(MailingState-MailingCountry) Concatenated Frequencies": '',
                          "Account(BillingState-BillingCountry) Concatenated Frequencies": '',
                          "Contact(StandardState-StandardCountry) Sorted Frequencies": '',
                          "Contact(MailingState-MailingCountry) Sorted Frequencies": '',
                          "Account(BillingState-BillingCountry) Sorted Frequencies": ''
                          }
                output_df = output_df.append(output_row, ignore_index=True)
                df_row = pd.merge(accounts_df, output_df, on='Id')
                if c == 1:
                    df_row.to_csv("accounts_highest_probable_addresses_3.csv", mode='w', index=False,
                                  line_terminator='\n')
                else:
                    df_row.to_csv("accounts_highest_probable_addresses_3.csv", mode='a', index=False,
                                  header=False,
                                  line_terminator='\n')

            logger.info(c)
            # if c == 100:
            #     break

    except Exception as e:
        logger.error(e)


def main():
    try:

        # accounts_csv_path = \
        #     "C:\Users\DELL\PycharmProjects\FunnelBeam\Highest Prob Address\csv files\mp_accounts_from_crm_addresses.csv"
        # contacts_csv_path = \
        #     "C:\Users\DELL\PycharmProjects\FunnelBeam\Highest Prob Address\csv files\Mixpanel_All_ContactsHavingAccount.csv"

        accounts_csv_path = \
            "/data/MIXPANEL/mp_accounts_from_crm_addresses.csv"
        contacts_csv_path = \
            "/data/MIXPANEL/Mixpanel_All_ContactsHavingAccount.csv"

        get_highest_probable_address_of_contacts(accounts_csv_path, contacts_csv_path)

    except Exception as e:
        logger.error(e)


if __name__ == "__main__":
    main()