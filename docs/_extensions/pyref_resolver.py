"""
Resolver for python cross-references that the default python domain cannot find, mostly short references in
docstrings such as :py:meth:`get_base` that are inherited by subclasses or refer to members of base classes.

Unresolved references are looked up in the following order:

1. Importable targets, e.g. shorthands such as ``law.Task``, are mapped to the qualified name of the actual object.
2. Members of the classes in the method resolution order of the current class. Members of classes outside of law,
   e.g. ``logging.Logger``, are forwarded to intersphinx.
3. Objects whose qualified name ends with the reference target, preferring those in the current module (or the
   closest module) when the target is ambiguous.
"""

import importlib

from sphinx.ext import intersphinx  # type: ignore[import-untyped]
from sphinx.util.nodes import make_refnode  # type: ignore[import-untyped]

# role types mapped to compatible object types of the python domain
ROLE_TYPES = {
    "meth": {"method", "classmethod", "staticmethod"},
    "attr": {"attribute", "classattribute", "property", "data"},
    "class": {"class", "exception"},
    "exc": {"exception", "class"},
    "func": {"function"},
    "obj": None,
}


def _import(name):
    # import an object given by its fully qualified name
    parts = name.split(".")
    for i in range(len(parts), 0, -1):
        try:
            obj = importlib.import_module(".".join(parts[:i]))
        except Exception:
            continue
        try:
            for part in parts[i:]:
                obj = getattr(obj, part)
        except AttributeError:
            return None
        return obj
    return None


def _common_prefix(a, b):
    n = 0
    for x, y in zip(a.split("."), b.split(".")):
        if x != y:
            break
        n += 1
    return n


def _find(domain, target, types):
    # objects whose name equals or ends with the target and whose type matches
    return [
        (name, obj)
        for name, obj in domain.objects.items()
        if (name == target or name.endswith("." + target)) and (types is None or obj.objtype in types)
    ]


def _closest(cands, modname, clsname):
    # candidates whose names share the longest prefix with the current context
    if not cands:
        return []
    context = f"{modname}.{clsname}" if clsname else modname
    best = max(_common_prefix(name, context) for name, _ in cands)
    return [c for c in cands if _common_prefix(c[0], context) == best]


def _qualname(target):
    # returns the qualified name "<module>.<qualname>" of an importable target, or None
    obj = _import(target)
    if obj is None:
        return None
    module = getattr(obj, "__module__", None)
    qualname = getattr(obj, "__qualname__", None)
    if module and qualname:
        return f"{module}.{qualname}"
    # attributes without qualified names, resolve the parent instead
    parent, _, attr = target.rpartition(".")
    parent_name = _qualname(parent) if parent else None
    return f"{parent_name}.{attr}" if parent_name else None


def _intersphinx(app, env, node, contnode, target):
    # resolve a target via intersphinx
    new_node = node.deepcopy()
    new_node["reftarget"] = target
    try:
        return intersphinx.missing_reference(app, env, new_node, contnode)
    except Exception:
        return None


def resolve(app, env, node, contnode):
    if node.get("refdomain") != "py":
        return None

    reftype = node.get("reftype")
    if reftype not in ROLE_TYPES:
        return None
    types = ROLE_TYPES[reftype]

    target = node.get("reftarget", "").lstrip("~.")
    if not target:
        return None

    domain = env.get_domain("py")
    modname = node.get("py:module") or ""
    clsname = node.get("py:class") or ""

    match = None

    # 1. importable targets, matched by their qualified name within the module (documented objects might be listed
    # under shorthands such as law.slurm instead of law.contrib.slurm.workflow)
    if "." in target:
        qualname = _qualname(target)
        if qualname and qualname != target:
            obj = _import(target)
            short = getattr(obj, "__qualname__", None) or qualname.split(".")[-2] + "." + qualname.split(".")[-1]
            cands = _find(domain, short, types)
            exact = [c for c in cands if c[0] == qualname]
            if exact or len(cands) == 1:
                match = (exact or cands)[0]

    # 2. members of classes in the mro of the current class, or of the class given in "Class.member" targets
    if match is None:
        cls, member = None, None
        if "." not in target and clsname:
            cls, member = _import(f"{modname}.{clsname}" if modname else clsname), target
        elif target.count(".") == 1:
            cls_part, member = target.split(".")
            cls_cands = _closest(_find(domain, cls_part, {"class", "exception"}), modname, clsname)
            # the same class might be documented under multiple names
            cls_objs = {id(obj): obj for obj in (_import(name) for name, _ in cls_cands) if obj is not None}
            if len(cls_objs) == 1:
                cls = next(iter(cls_objs.values()))
        if isinstance(cls, type):
            for base in cls.__mro__:
                if base is object:
                    continue
                if not base.__module__.startswith("law"):
                    # external classes are documented via intersphinx, which might list inherited members only for
                    # the class that is documented, so try all external bases that provide the member
                    if hasattr(base, member):
                        ref = _intersphinx(app, env, node, contnode, f"{base.__module__}.{base.__qualname__}.{member}")
                        if ref is not None:
                            return ref
                    continue
                if member not in vars(base):
                    continue
                cands = _find(domain, f"{base.__name__}.{member}", types)
                if len(cands) == 1:
                    match = cands[0]
                    break

    # 3. unique or closest suffix match
    if match is None:
        closest = _closest(_find(domain, target, types), modname, clsname)
        if len(closest) == 1:
            match = closest[0]

    if match is None:
        return None

    name, obj = match
    return make_refnode(app.builder, node["refdoc"], obj.docname, obj.node_id, contnode, name)


def setup(app):
    app.connect("missing-reference", resolve)

    return {"version": "law_pyref_resolver", "parallel_read_safe": True}
