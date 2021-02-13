# coding: utf-8

"""
Patch for the default sphinx python domain that adds:

- classattribute directive
"""

from sphinx.locale import _
from sphinx.domains import ObjType
from sphinx.domains.python import PyAttribute


class PyClassAttribute(PyAttribute):

    def get_signature_prefix(self, sig):
        return "classattribute"

    def get_index_text(self, modname, name_cls):
        name, cls = name_cls
        add_modules = self.env.config.add_module_names

        if "." in name:
            clsname, attrname = name.rsplit(".", 1)
            if modname and add_modules:
                return _("{} ({}.{} class attribute)").format(attrname, modname, clsname)
            else:
                return _("{} ({} class attribute)").format(attrname, clsname)
        else:
            if modname:
                return _("{} (in module {})").format(name, modname)
            else:
                return name


def setup(app):
    # get the py domain
    domain = app.registry.domains["py"]

    # add the new "classattribute" object type and directive
    domain.object_types["classattribute"] = ObjType(_("classattribute"), "attr", "obj")
    domain.directives["classattribute"] = PyClassAttribute

    return {"version": "law_patch"}
