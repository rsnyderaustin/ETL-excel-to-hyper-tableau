import os
from query_bundle import QueryBundle
from query_iterator import QueryIterator

# Manually enter queries here, future development can allow for user console input
def create_query_bundles():
    # Ex: age_per_year = QueryBundle(
    #         export_file_name="two_queries_test",
    #         _query_names=["all_age", "all_activity"],
    #         _query_strings=["SELECT [First Name] as first_name, Age as age FROM Yearend.sheet",
    #                  "SELECT Activity from Yearend.sheet"],
    #         matches=range(2000, 2020),
    #         sheets=['Yearend'],
    #         axis=1
    #     )

    retirement_summary = QueryBundle(
        export_file_name="retirement_summary",
        _query_names=["retirement_summary"],
        _query_strings=[
            "SELECT COUNT([Status ID]) as num_retirements, ROUND(AVG(Age), 2) as avg_age FROM DEL.sheet WHERE [Status Id] = 'RT'"],
        matches=range(2015, 2022),
        sheets=['DEL'],
        combine_columns=[True]
    )

    query_bundles = []
    local_values = locals().copy()
    for value in local_values.values():
        if isinstance(value, QueryBundle):
            query_bundles.append(value)
    return query_bundles


def main():
    query_bundles = create_query_bundles()
    query_iterator = QueryIterator(directory=os.listdir(),
                                   database_path='C:/Users/austisnyder/Downloads/PyCharmWorkingDirectory/df_tables.db',
                                   query_bundles=query_bundles)
    query_iterator.run()

    # Create a dictionary formatted year : file_name for each file in directory


main()
