# coding: utf-8

"""
Patch for the default sphinx python domain that adds:

- classattribute directive
"""

from docutils import nodes
from sphinx import addnodes
from sphinx.locale import _
from sphinx.domains import ObjType
from sphinx.domains.python import PyAttribute


class PyClassAttribute(PyAttribute):

    def get_signature_prefix(self, sig):
        return [
            nodes.Text("classattribute"),
            addnodes.desc_sig_space(),
        ]

    def get_index_text(self, modname, name_cls):
        name, cls = name_cls
        add_modules = self.env.config.add_module_names

        if "." in name:
            clsname, attrname = name.rsplit(".", 1)
            if modname and add_modules:
                return _(f"{attrname} ({modname}.{clsname} class attribute)")
            return _(f"{attrname} ({clsname} class attribute)")
        return _(f"{name} (in module {modname})") if modname else name


def setup(app):
    # get the py domain
    domain = app.registry.domains["py"]

    # add the new "classattribute" object type and directive
    domain.object_types["classattribute"] = ObjType(_("classattribute"), "attr", "obj")
    domain.directives["classattribute"] = PyClassAttribute

    return {"version": "law_patch"}
