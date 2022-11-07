import click
import grab_sage_entity
import load_long_term

CONTEXT_SETTINGS = dict(help_option_names=['--help'])

@click.group(context_settings=CONTEXT_SETTINGS) 
def main():
    """
    Luke McConnell's command line tool for extracting bulk entity data from Sage Intacct
    
    \b
    Created: 11/06/2022
    Updated: 11/06/2022
    """
    return


# GRAB ONE ENTITY COMMAND
@main.command()
@click.option(
    "--entity",
    prompt="Sage Intacct entity name (ex: CUSTOMER)",
    help="Pass in the name of the entity you want to save.",
    default="CUSTOMER"
)

@click.option(
    "--query",
    prompt="SQL like query for the specified entity (ex: date and location)",
    help="Pass in a query to filter down the results",
    default="WHENMODIFIED >= 06/01/2022 AND WHENMODIFIED <= 06/10/2022"
)


def one_entity(entity, query):
    """
    Grab one entity with a query and save it in storage.
    """
    grab_sage_entity.main(entity, query)

# RUN FULL ENTITY GRAB
@main.command()
@click.option(
    "--entity",
    prompt="Sage Intacct entity name (ex: CUSTOMER)",
    help="Pass in the name of the entity you want to save.",
    default="CUSTOMER"
)

@click.option(
    "--start_date",
    prompt="Date you want to start the data extract (ex: 2022-01-01)",
    help="Pass in the date that you want the scan to begin on",
    default="2022-01-01"
)

@click.option(
    "--end_date",
    prompt="Date you want to end the data extract (ex: 2022-06-01)",
    help="PAss in the date AFTER the day you want the scan to end on",
    default="2022-07-01"
)


def full_extract(entity, start_date, end_date):
    """
    Loop through months for an entity and save to storage
    """
    load_long_term.main(entity, start_date, end_date)

if __name__ == '__main__':
    main()