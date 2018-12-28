# coding: utf-8

"""
Patch for the default sphinx python domain that adds:

- classattribute directive
"""


from sphinx.domains import ObjType
from sphinx.locale import l_, _

import sphinx.domains.python as pyd


def setup(app):
    # get the py domain
    domain = app.registry.domains["py"]

    # add the new "classattribute" object type and directive
    domain.object_types["classattribute"] = ObjType(l_("classattribute"), "attr", "obj")
    domain.directives["classattribute"] = pyd.PyClassmember

    # patch get_index_text
    PyClassmember__get_index_text = pyd.PyClassmember.get_index_text

    def get_index_text(self, modname, name_cls):
        text = PyClassmember__get_index_text(self, modname, name_cls)

        if text != "":
            return text

        name, cls = name_cls
        add_modules = self.env.config.add_module_names

        if self.objtype == "classattribute":
            try:
                clsname, attrname = name.rsplit(".", 1)
            except ValueError:
                if modname:
                    return _("%s (in module %s)") % (name, modname)
                else:
                    return name
            if modname and add_modules:
                return _("%s (%s.%s class attribute)") % (attrname, modname, clsname)
            else:
                return _("%s (%s class attribute)") % (attrname, clsname)
        else:
            return ""

    pyd.PyClassmember.get_index_text = get_index_text

    # patch get_signature_prefix
    PyClassmember__get_signature_prefix = pyd.PyClassmember.get_signature_prefix

    def get_signature_prefix(self, sig):
        prefix = PyClassmember__get_signature_prefix(self, sig)

        if prefix != "":
            return prefix

        if self.objtype == "classattribute":
            return "classattribute "
        else:
            return ""

    pyd.PyClassmember.get_signature_prefix = get_signature_prefix

    return {"version": "patch"}
