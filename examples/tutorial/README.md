# Hands-on law tutorial

A notebook for newcomers that introduces the most important concepts of law step by step, from a first task to workflows.

Throughout the tutorial, dice are rolled and the results are analyzed, while each stage adds a few new concepts:

1. A first task: outputs, `run()`, targets and formatters
2. Parameters: configurable tasks
3. Dependencies: `requires()`, `input()`, `req()` and the task status
4. A base task: sharing functionality and versioning outputs
5. Workflows: processing many similar tasks
6. Inspecting and cleaning up: interactive command line parameters
7. Writing outputs robustly: failures, `localize()` and decorators
8. Where to go from here

There are multiple ways to run the tutorial:

1. [![Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/riga/law/blob/master/examples/tutorial/tutorial.ipynb)
2. [![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/riga/law/master?filepath=examples/tutorial/tutorial.ipynb)
3. Locally: open [tutorial.ipynb](tutorial.ipynb) in Jupyter, e.g. via `jupyter lab tutorial.ipynb` in this directory.
   When opened inside a checkout of the law repository, the notebook uses law from the checkout.

All outputs are written into the `data` directory next to the notebook.
To start over, simply remove it.
