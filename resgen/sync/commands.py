import click

@click.group()
def sync():
    pass

@click.command()
def dataset():
    """Upload if a file with the same name doesn't already exist."""
    print("sync dataset")

sync.add_command(dataset)