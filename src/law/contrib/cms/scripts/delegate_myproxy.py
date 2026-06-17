#!/usr/bin/env python

"""
Script to trigger a myproxy delegation, e.g. for crab submission.
"""

from __future__ import annotations


def delegate(
    renew: bool,
    endpoint: str,
    username: str | None,
    password_file: str | None,
    vo: str | None,
    voms_proxy: bool,
    crab: bool,
    /,
) -> None:
    import law

    law.contrib.load("cms", "wlcg")

    # settings
    encode_username = False
    retrievers: list[str] | None = None

    # crab mode
    if crab:
        encode_username = True
        voms_proxy = True
        username = None
        retrievers = law.cms.util._default_crab_receivers  # type: ignore[attr-defined]

    # when not renewing, check if a previous delegation exists
    if not renew:
        info = law.wlcg.get_myproxy_info(  # type: ignore[attr-defined]
            endpoint=endpoint,
            username=username,
            encode_username=encode_username,
            silent=True,
        )
        if info:
            print(f"existing myproxy delegation found for username {info['username']}")
            return

    if voms_proxy and (renew or not law.wlcg.check_vomsproxy_validity()):  # type: ignore[attr-defined] # noqa
        print("\nrenewing voms-proxy")
        law.cms.renew_vomsproxy(vo=vo, password_file=password_file)  # type: ignore[attr-defined]

    # create a new delegation
    print(f"\ndelegating to {endpoint}")
    law.cms.delegate_myproxy(  # type: ignore[attr-defined]
        endpoint=endpoint,
        username=username,
        encode_username=encode_username,
        password_file=password_file,
        vo=vo,
        retrievers=retrievers,
    )


def main() -> int:
    from argparse import ArgumentParser

    import law

    cfg = law.config.Config.instance()

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
        default=(pf := cfg.get_expanded("job", "crab_password_file")),
        help="a file containing the certificate password" + (f"; default: {pf}" if pf else ""),
    )
    parser.add_argument(
        "--username",
        "-u",
        help="a custom user name",
    )
    parser.add_argument(
        "--vo",
        "-m",
        default=law.cms.util._default_vo(),  # type: ignore[attr-defined]
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

    return 0


# entry hook
if __name__ == "__main__":
    import sys

    exit_code = main()

    if isinstance(exit_code, int):
        sys.exit(exit_code)
