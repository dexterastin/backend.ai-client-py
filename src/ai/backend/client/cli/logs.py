import sys

import click

from .pretty import print_wait, print_done, print_error
from ..session import Session


@click.command()
@click.argument('sess_id_or_alias', metavar='SESSID')
def logs():
    '''
    Shows the output logs of a running container. The session ID or its alias given
    when creating the session.
    '''
    with Session() as session:
        try:
            print_wait('Retrieving container logs...')
            kernel = session.Kernel(sess_id_or_alias)
            result = kernel.get_logs().get('result')
            logs = result.get('logs') if 'logs' in result else ''
            print(logs)
            print_done('End of logs.')
        except Exception as e:
            print_error(e)
            sys.exit(1)
