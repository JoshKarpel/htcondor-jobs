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


.. autoclass:: ClusterHandle

   .. automethod:: from_submit_result


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
