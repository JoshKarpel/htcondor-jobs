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

   .. automethod:: save
   .. automethod:: load


Cluster Handles
---------------

.. autoclass:: ClusterHandle

   .. autoattribute:: state
   .. automethod:: to_json
   .. automethod:: from_json

.. autoclass:: ClusterState

   .. automethod:: all_complete
   .. automethod:: any_complete
   .. automethod:: any_idle
   .. automethod:: any_running
   .. automethod:: any_held

