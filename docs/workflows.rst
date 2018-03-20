Workflows
=========

A workflow is a type of task that essentially introduces two objects: a *branch* parameter and a
*proxy* task. When a task that inherits from a workflow class is instantiated, the value of the
branch parameter decides about the task's behavior. There are two cases:

   - ``branch == -1``
      The task is considered to be **the** *workflow* itself. Calls to ``requires``, ``output`` and
      ``run`` are forwarded to the proxy task instance. The proxy task should have dedicated
      implementations of those methods in order to provide custom requirements, outputs, and the
      recipe to run (or even submit) the branch tasks.

   - ``branch != -1``
      The task is considered to be **a** *branch* task. No attribute forwarding is applied. What
      you see is what you run.

The definition of what the workflow is actually processing is done in its *branch map*. This is 
just a dictionary that maps branch numbers (i.e. the value of the branch parameter) to arbitrary 
data, depending on which distinct information is required by a branch task.

Here is an example using the local workflow:

.. code-block:: python

   import law

   class MyWorkflow(law.LocalWorkflow):

       def create_branch_map(self):
           # you need to implement this method to tell the workflow about its actual payload
           # later on, use self.branch_map (cached) to access the map

           # here, we define a workflow that will branch into 3 tasks
           # it is recommended to use contiguous branch numbers starting at 0
           return {0: "foo", 1: "bar", 2: "baz"}

       def workflow_requires(self):
           # hook for configuring additional requirements of the workflow
           # this hook is invoked from within the ``requires`` method of the proxy task, which
           # expects that you return a dictionary here
           return {"common": SomeCommonDependency.req(self, ...)}

       def requires(self):
           # requirements of the current branch task
           return SomeBranchSpecificRequirement.req(self, ...)

       def output(self):
           # ouptut targets of the current branch task
           # you might want to encode the current branch number into the output path
           return law.LocalFileTarget("some/file_{}.txt".format(self.branch))

       def run(self):
           # run method for the current branch

           # for simplicity, just write the branch information into the output file
           # self.branch_data refers to self.branch_map[self.branch]
           self.output().dump(self.branch_data, formatter="text")

If you want to run the entire workflow (locally), do:

.. code-block:: bash

   law run MyWorkflow

If you are just interested in running a particular branch task, add the branch parameter:

.. code-block:: bash

   law run MyWorkflow --branch 0

You can find a full, running workflow example `here
<https://github.com/riga/law/tree/master/examples/workflows>`_.

A task can inherit from multiple workflow classes. At execution time, the actual workflow
implementation to use can be set via the ``--workflow`` parameter. By default, the first workflow
class in the method resolution order (so the order of base classes) is used.
