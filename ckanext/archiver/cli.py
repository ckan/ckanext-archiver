import click
from ckanext.archiver import utils


def get_commands():
    return [archiver]


@click.group()
def archiver():
    pass


@archiver.command()
@click.option('-q', '--queue', default='bulk')
@click.argument('identifiers', nargs=-1)
def update(identifiers, queue):
    utils.update(identifiers, queue)


@archiver.command()
def init():
    utils.init()
    click.secho("Archiver tables are initialized", fg="green")


@archiver.command()
@click.argument('package_ref', required=False)
def view(package_ref):
    if package_ref:
        utils.view(package_ref)
    else:
        utils.view()


@archiver.command()
def clean_status():
    utils.clean_status()


@archiver.command()
def clean_cached_resources():
    utils.clean_cached_resources()


@archiver.command()
def migrate():
    utils.migrate()


@archiver.command()
def migrate_archive_dirs():
    utils.migrate_archive_dirs()


@archiver.command()
def size_report():
    utils.size_report()


@archiver.command()
def delete_files_larger_than_max_content_length():
    utils.delete_files_larger_than_max_content_length()
