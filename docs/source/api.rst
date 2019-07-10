API Reference
=============

.. py:currentmodule:: htcondor_jobs

Describing Jobs
---------------

.. autoclass:: SubmitDescription

   .. automethod:: as_submit


Submitting Jobs
---------------

.. autofunction:: submit

.. autoclass:: Transaction

   .. automethod:: submit


Querying, Acting on, and Editing Jobs
-------------------------------------

.. autoclass:: ConstraintHandle

   .. automethod:: query

   .. automethod:: remove
   .. automethod:: hold
   .. automethod:: release
   .. automethod:: pause
   .. automethod:: resume
   .. automethod:: vacate

   .. automethod:: edit

   .. autoattribute:: constraint
   .. automethod:: reduce

Cluster Handles
---------------

.. autoclass:: ClusterHandle

   .. autoattribute:: state

   .. automethod:: save
   .. automethod:: load
   .. automethod:: to_json
   .. automethod:: from_json

.. autoclass:: ClusterState

   .. automethod:: is_complete
   .. automethod:: any_running
   .. automethod:: any_in_queue
   .. automethod:: any_held

Constraints
-----------

.. autoclass:: Constraint

   .. automethod:: reduce


Combinators
+++++++++++

.. autoclass:: And

.. autoclass:: Or

.. autoclass:: Not



Comparisons
+++++++++++

.. autoclass:: Comparison

.. autoclass:: Operator

.. autoclass:: ComparisonConstraint

Shortcuts
+++++++++

.. autoclass:: InCluster
