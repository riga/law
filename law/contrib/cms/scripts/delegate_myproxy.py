#!/usr/bin/env python

"""
Script to trigger a myproxy delegation, e.g. for crab submission.
"""


def delegate(renew, endpoint, username, password_file, vo, voms_proxy, crab):
    import law

    law.contrib.load("cms", "wlcg")

    # settings
    encode_username = False
    retrievers = None

    # crab mode
    if crab:
        encode_username = True
        voms_proxy = True
        retrievers = law.contrib.cms.util._default_crab_receivers

    # when not renewing, check if a previous delegation exists
    if not renew:
        info = law.wlcg.get_myproxy_info(
            endpoint=endpoint,
            username=username,
            encode_username=encode_username,
            silent=True,
        )
        if info:
            print("existing myproxy delegation found for username {}".format(info["username"]))
            return

    if voms_proxy and (renew or not law.wlcg.check_vomsproxy_validity()):
        print("\nrenewing voms-proxy")
        law.cms.renew_vomsproxy(vo=vo, password_file=password_file)

    # create a new delegation
    print("\ndelegating to {}".format(endpoint))
    law.cms.delegate_myproxy(
        endpoint=endpoint,
        username=username,
        encode_username=encode_username,
        password_file=password_file,
        vo=vo,
        retrievers=retrievers,
    )


def main():
    from argparse import ArgumentParser

    import law

    default_pf = law.config.get_expanded("job", "crab_password_file")

    parser = ArgumentParser(
        prog="law_cms_delegate_myproxy",
        description="delegates an X509 proxy to a myproxy server",
    )
    parser.add_argument(
        "--renew",
        "-r",
        action="store_true",
        help="renew the delegation even if an existing one was found",
    )
    parser.add_argument(
        "--endpoint",
        "-e",
        default="myproxy.cern.ch",
        help="the server endpoint; default: %(default)s",
    )
    parser.add_argument(
        "--password-file",
        "-p",
        help="a file containing the certificate password" + (
            "; default: {}".format(default_pf) if default_pf else ""
        ),
        default=default_pf,
    )
    parser.add_argument(
        "--username",
        "-u",
        help="a custom user name",
    )
    parser.add_argument(
        "--vo",
        "-m",
        default=law.contrib.cms.util._default_vo(),
        help="virtual organization to use; default: %(default)s",
    )
    parser.add_argument(
        "--voms-proxy",
        "-v",
        action="store_true",
        help="create a voms-proxy prior to the delegation",
    )
    parser.add_argument(
        "--crab",
        action="store_true",
        help="adds crab-specific defaults",
    )
    args = parser.parse_args()

    delegate(
        args.renew,
        args.endpoint,
        args.username,
        args.password_file,
        args.vo,
        args.voms_proxy,
        args.crab,
    )


# entry hook
if __name__ == "__main__":
    import sys

    exit_code = main()

    if isinstance(exit_code, int):
        sys.exit(exit_code)
